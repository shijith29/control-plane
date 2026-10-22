"""Concurrency demo against a RUNNING stack (`make race`).

Fires real concurrent HTTP requests and checks that the database lets exactly one win:

  1. N x POST with the same slug         -> 1 x 201, N-1 x 409 tenant_already_exists
  2. N x PATCH with the same version     -> 1 x 202, N-1 x 409 tenant_version_conflict
  3. PATCH racing DELETE                 -> exactly one winner
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import uuid
from collections import Counter
from typing import Any

import httpx


def summarize(responses: list[httpx.Response]) -> Counter[str]:
    out: Counter[str] = Counter()
    for r in responses:
        key = str(r.status_code)
        if r.status_code >= 400:
            key += f" {r.json()['error']['code']}"
        out[key] += 1
    return out


def report(label: str, result: Counter[str], passed: bool) -> bool:
    outcome = ", ".join(f"{k} x{v}" for k, v in sorted(result.items()))
    print(f"{label:<44} -> {outcome:<52} {'✔' if passed else '✘'}")
    return passed


async def wait_active(
    client: httpx.AsyncClient, tenant_id: str, within: float = 60
) -> dict[str, Any]:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + within
    while loop.time() < deadline:
        tenant: dict[str, Any] = (await client.get(f"/v1/tenants/{tenant_id}")).json()
        if tenant["status"] == "active":
            return tenant
        if tenant["status"] == "failed":
            sys.exit("tenant failed — is the worker running with --fail-rate=0?")
        await asyncio.sleep(0.2)
    sys.exit("tenant did not become active in time — is the worker running?")


async def main(base_url: str, n: int) -> int:
    ok = True
    limits = httpx.Limits(max_connections=n, max_keepalive_connections=n)
    async with httpx.AsyncClient(base_url=base_url, timeout=30, limits=limits) as client:
        slug = f"race-{uuid.uuid4().hex[:6]}"

        # 1. create race
        responses = await asyncio.gather(
            *[client.post("/v1/tenants", json={"slug": slug, "name": "Race"}) for _ in range(n)]
        )
        result = summarize(responses)
        expected = Counter({"201": 1, "409 tenant_already_exists": n - 1})
        ok &= report(f"[create race]  {n} x POST slug={slug}", result, result == expected)

        tenant_id = next(r.json()["id"] for r in responses if r.status_code == 201)
        tenant = await wait_active(client, tenant_id)

        # 2. patch race
        version = tenant["version"]
        responses = await asyncio.gather(
            *[
                client.patch(f"/v1/tenants/{tenant_id}", json={"version": version, "name": f"n{i}"})
                for i in range(n)
            ]
        )
        result = summarize(responses)
        expected = Counter({"202": 1, "409 tenant_version_conflict": n - 1})
        ok &= report(f"[patch race]   {n} x PATCH version={version}", result, result == expected)

        # 3. patch vs delete
        tenant = await wait_active(client, tenant_id)
        responses = await asyncio.gather(
            client.patch(
                f"/v1/tenants/{tenant_id}", json={"version": tenant["version"], "name": "x"}
            ),
            client.delete(f"/v1/tenants/{tenant_id}"),
        )
        result = summarize(responses)
        winners = sum(1 for r in responses if r.status_code == 202)
        ok &= report("[patch/delete] PATCH ∥ DELETE", result, winners == 1)

    return 0 if ok else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--base-url", default="http://localhost:8080")
    parser.add_argument("-n", type=int, default=50, help="concurrent requests per race")
    args = parser.parse_args()
    sys.exit(asyncio.run(main(args.base_url, args.n)))
