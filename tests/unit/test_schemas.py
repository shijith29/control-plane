"""Request validation rules (slug regex, name, PATCH body)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from control_plane.api.schemas import TenantCreate, TenantPatch


@pytest.mark.parametrize(
    "slug",
    ["abc", "a-c", "acme-corp", "a1b2", "z" * 28, "a" + "-" * 26 + "z", "tenant-42"],
)
def test_valid_slugs(slug: str) -> None:
    assert TenantCreate(slug=slug, name="x").slug == slug


@pytest.mark.parametrize(
    "slug",
    [
        "ab",  # too short
        "a" * 29,  # too long
        "1abc",  # must start with a letter
        "-abc",
        "abc-",  # must end alphanumeric
        "Abc",  # lowercase only
        "a_bc",  # no underscore
        "a.bc",
        "a bc",
        "",
        "ünï",
    ],
)
def test_invalid_slugs(slug: str) -> None:
    with pytest.raises(ValidationError):
        TenantCreate(slug=slug, name="x")


@pytest.mark.parametrize("name", ["", "   ", "\t\n"])
def test_name_must_not_be_blank(name: str) -> None:
    with pytest.raises(ValidationError):
        TenantCreate(slug="acme", name=name)


def test_name_is_trimmed() -> None:
    assert TenantCreate(slug="acme", name="  Acme  ").name == "Acme"


def test_unknown_fields_are_rejected() -> None:
    with pytest.raises(ValidationError):
        TenantCreate.model_validate({"slug": "acme", "name": "x", "status": "active"})


def test_patch_requires_version() -> None:
    with pytest.raises(ValidationError):
        TenantPatch.model_validate({"name": "x"})


@pytest.mark.parametrize("version", [0, -1, "2", 2.0, True])
def test_patch_version_must_be_positive_strict_int(version: object) -> None:
    with pytest.raises(ValidationError):
        TenantPatch.model_validate({"version": version, "name": "x"})


def test_patch_requires_a_mutable_field() -> None:
    with pytest.raises(ValidationError, match="at least one mutable field"):
        TenantPatch.model_validate({"version": 2})


def test_patch_slug_is_immutable() -> None:
    with pytest.raises(ValidationError):
        TenantPatch.model_validate({"version": 2, "slug": "new-slug"})


def test_valid_patch() -> None:
    patch = TenantPatch.model_validate({"version": 3, "name": " New "})
    assert (patch.version, patch.name) == (3, "New")
