"""HTTP contract: status codes, error envelope, headers, pagination, filters."""

from __future__ import annotations

import re
import uuid

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.db

ISO_MS_Z = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$")


async def create(client: httpx.AsyncClient, slug: str = "acme", name: str = "Acme") -> dict:  # type: ignore[type-arg]
    response = await client.post("/v1/tenants", json={"slug": slug, "name": name})
    assert response.status_code == 201, response.text
    return response.json()  # type: ignore[no-any-return]


async def activate(db: AsyncEngine, tenant_id: str) -> None:
    async with db.begin() as conn:
        await conn.execute(
            text("UPDATE tasks SET status='done' WHERE tenant_id=:t"), {"t": tenant_id}
        )
        await conn.execute(
            text("UPDATE tenants SET status='active', version=version+1 WHERE id=:t"),
            {"t": tenant_id},
        )


def assert_error(response: httpx.Response, status: int, code: str) -> dict:  # type: ignore[type-arg]
    assert response.status_code == status, response.text
    body = response.json()
    assert set(body) == {"error", "request_id"}
    assert body["error"]["code"] == code
    assert isinstance(body["error"]["message"], str) and body["error"]["message"]
    assert isinstance(body["error"]["details"], dict)
    return body  # type: ignore[no-any-return]


async def test_create_returns_201_with_location(client: httpx.AsyncClient) -> None:
    response = await client.post("/v1/tenants", json={"slug": "acme-corp", "name": "Acme Corp"})
    assert response.status_code == 201
    body = response.json()
    assert response.headers["location"] == f"/v1/tenants/{body['id']}"
    assert body["status"] == "provisioning"
    assert body["version"] == 1
    assert body["current_task"]["type"] == "deploy"
    assert ISO_MS_Z.match(body["created_at"]) and ISO_MS_Z.match(body["updated_at"])
    assert response.headers["x-request-id"]


async def test_get_tenant(client: httpx.AsyncClient) -> None:
    tenant = await create(client)
    response = await client.get(f"/v1/tenants/{tenant['id']}")
    assert response.status_code == 200
    assert response.json() == tenant


async def test_duplicate_slug_409(client: httpx.AsyncClient) -> None:
    await create(client)
    assert_error(
        await client.post("/v1/tenants", json={"slug": "acme", "name": "x"}),
        409,
        "tenant_already_exists",
    )


@pytest.mark.parametrize(
    "body",
    [
        {"slug": "Bad_Slug", "name": "x"},
        {"slug": "acme", "name": "   "},
        {"slug": "acme"},
        {"slug": "acme", "name": "x", "status": "active"},
    ],
)
async def test_create_validation_422(client: httpx.AsyncClient, body: dict) -> None:  # type: ignore[type-arg]
    error = assert_error(await client.post("/v1/tenants", json=body), 422, "validation_error")
    assert error["error"]["details"]["fields"]


async def test_malformed_json_422(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/v1/tenants", content=b"{not json", headers={"content-type": "application/json"}
    )
    assert_error(response, 422, "validation_error")


async def test_not_found_errors(client: httpx.AsyncClient) -> None:
    missing = uuid.uuid4()
    assert_error(await client.get(f"/v1/tenants/{missing}"), 404, "tenant_not_found")
    assert_error(await client.get(f"/v1/tasks/{missing}"), 404, "task_not_found")
    assert_error(await client.get(f"/v1/tenants/{missing}/tasks"), 404, "tenant_not_found")
    assert_error(await client.delete(f"/v1/tenants/{missing}"), 404, "tenant_not_found")
    assert_error(
        await client.patch(f"/v1/tenants/{missing}", json={"version": 1, "name": "x"}),
        404,
        "tenant_not_found",
    )


async def test_invalid_uuid_is_validation_error(client: httpx.AsyncClient) -> None:
    assert_error(await client.get("/v1/tenants/not-a-uuid"), 422, "validation_error")


async def test_unknown_route_and_method(client: httpx.AsyncClient) -> None:
    assert_error(await client.get("/v1/nope"), 404, "not_found")
    assert_error(await client.put("/v1/tenants"), 405, "method_not_allowed")


async def test_patch_flow_and_errors(client: httpx.AsyncClient, db: AsyncEngine) -> None:
    tenant = await create(client)
    url = f"/v1/tenants/{tenant['id']}"

    busy = assert_error(
        await client.patch(url, json={"version": 1, "name": "x"}), 409, "tenant_update_not_allowed"
    )
    assert busy["error"]["details"]["current_status"] == "provisioning"

    await activate(db, tenant["id"])  # v2
    stale = assert_error(
        await client.patch(url, json={"version": 1, "name": "x"}), 409, "tenant_version_conflict"
    )
    assert stale["error"]["details"] == {"expected_version": 1, "current_version": 2}

    response = await client.patch(url, json={"version": 2, "name": "Acme Corporation"})
    assert response.status_code == 202
    body = response.json()
    assert (body["status"], body["version"], body["name"]) == ("updating", 3, "Acme Corporation")
    assert body["current_task"]["type"] == "update"

    assert_error(await client.patch(url, json={"name": "x"}), 422, "validation_error")
    assert_error(await client.patch(url, json={"version": 3}), 422, "validation_error")


async def test_delete_flow(client: httpx.AsyncClient, db: AsyncEngine) -> None:
    tenant = await create(client)
    url = f"/v1/tenants/{tenant['id']}"
    assert_error(await client.delete(url), 409, "tenant_update_not_allowed")

    await activate(db, tenant["id"])
    response = await client.delete(url)
    assert response.status_code == 202
    assert response.json()["status"] == "destroying"
    assert (await client.get(url)).json()["status"] == "destroying"  # still readable


async def test_tenant_tasks_endpoint(client: httpx.AsyncClient, db: AsyncEngine) -> None:
    tenant = await create(client)
    await activate(db, tenant["id"])
    await client.patch(f"/v1/tenants/{tenant['id']}", json={"version": 2, "name": "B"})

    response = await client.get(f"/v1/tenants/{tenant['id']}/tasks")
    items = response.json()["items"]
    assert [(t["type"], t["status"]) for t in items] == [("deploy", "done"), ("update", "accepted")]
    assert all(t["tenant_id"] == tenant["id"] for t in items)

    only_open = await client.get(f"/v1/tenants/{tenant['id']}/tasks", params={"status": "accepted"})
    assert [t["type"] for t in only_open.json()["items"]] == ["update"]

    task_id = items[1]["id"]
    task = (await client.get(f"/v1/tasks/{task_id}")).json()
    assert task["id"] == task_id and task["error"] is None


async def test_pagination_walks_every_row_once(client: httpx.AsyncClient) -> None:
    created = {(await create(client, slug=f"tenant-{i:02d}"))["id"] for i in range(7)}
    seen: list[str] = []
    cursor: str | None = None
    pages = 0
    while True:
        params: dict[str, str | int] = {"limit": 3}
        if cursor:
            params["cursor"] = cursor
        page = (await client.get("/v1/tenants", params=params)).json()
        seen += [t["id"] for t in page["items"]]
        pages += 1
        cursor = page["next_cursor"]
        if cursor is None:
            break
    assert pages == 3
    assert len(seen) == len(set(seen)) == 7
    assert set(seen) == created


async def test_list_filters(client: httpx.AsyncClient, db: AsyncEngine) -> None:
    a = await create(client, slug="alpha")
    await create(client, slug="bravo")
    await activate(db, a["id"])

    active = (await client.get("/v1/tenants", params={"status": "active"})).json()["items"]
    assert [t["slug"] for t in active] == ["alpha"]
    by_slug = (await client.get("/v1/tenants", params={"slug": "bravo"})).json()["items"]
    assert [t["slug"] for t in by_slug] == ["bravo"]

    tasks = (await client.get("/v1/tasks", params={"tenant_id": a["id"], "type": "deploy"})).json()
    assert len(tasks["items"]) == 1


@pytest.mark.parametrize(
    "params",
    [{"status": "bogus"}, {"limit": 0}, {"limit": 201}, {"cursor": "garbage"}],
)
async def test_list_validation(client: httpx.AsyncClient, params: dict) -> None:  # type: ignore[type-arg]
    assert_error(await client.get("/v1/tenants", params=params), 422, "validation_error")


async def test_health_endpoints(client: httpx.AsyncClient) -> None:
    assert (await client.get("/healthz")).json() == {"status": "ok"}
    ready = await client.get("/readyz")
    assert ready.status_code == 200
    assert ready.json() == {"ready": True, "database": "ok", "broker": "disabled"}


async def test_openapi_is_served(client: httpx.AsyncClient) -> None:
    spec = (await client.get("/openapi.json")).json()
    assert "/v1/tenants/{tenant_id}" in spec["paths"]
