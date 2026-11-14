from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Self, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.database_lock import DatabaseLock
    from ..models.database_resource import DatabaseResource
    from ..models.database_role import DatabaseRole


T = TypeVar("T", bound="UpdateDatabaseRequest")


@_attrs_define
class UpdateDatabaseRequest:
    """
    Attributes:
        resource_updates (list[DatabaseResource] | Unset): List of resources to add or update.
        resource_deletions (list[str] | Unset): List of resource ids to delete from the database.
        lock_updates (list[DatabaseLock] | Unset): List of locks to add or update
        lock_deletions (list[str] | Unset): List of lock ids to delete
        role_updates (list[DatabaseRole] | Unset): List of roles to add or update
        role_deletions (list[str] | Unset): List of role ids to delete
        language_updates (list[str] | Unset): List of languages to add or update
        language_deletions (list[str] | Unset): List of languages to delete
        original_language (str | Unset): Original language update
    """

    resource_updates: list[DatabaseResource] | Unset = UNSET
    resource_deletions: list[str] | Unset = UNSET
    lock_updates: list[DatabaseLock] | Unset = UNSET
    lock_deletions: list[str] | Unset = UNSET
    role_updates: list[DatabaseRole] | Unset = UNSET
    role_deletions: list[str] | Unset = UNSET
    language_updates: list[str] | Unset = UNSET
    language_deletions: list[str] | Unset = UNSET
    original_language: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        resource_updates: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.resource_updates, Unset):
            resource_updates = []
            for resource_updates_item_data in self.resource_updates:
                resource_updates_item = resource_updates_item_data.to_dict()
                resource_updates.append(resource_updates_item)

        resource_deletions: list[str] | Unset = UNSET
        if not isinstance(self.resource_deletions, Unset):
            resource_deletions = self.resource_deletions

        lock_updates: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.lock_updates, Unset):
            lock_updates = []
            for lock_updates_item_data in self.lock_updates:
                lock_updates_item = lock_updates_item_data.to_dict()
                lock_updates.append(lock_updates_item)

        lock_deletions: list[str] | Unset = UNSET
        if not isinstance(self.lock_deletions, Unset):
            lock_deletions = self.lock_deletions

        role_updates: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.role_updates, Unset):
            role_updates = []
            for role_updates_item_data in self.role_updates:
                role_updates_item = role_updates_item_data.to_dict()
                role_updates.append(role_updates_item)

        role_deletions: list[str] | Unset = UNSET
        if not isinstance(self.role_deletions, Unset):
            role_deletions = self.role_deletions

        language_updates: list[str] | Unset = UNSET
        if not isinstance(self.language_updates, Unset):
            language_updates = self.language_updates

        language_deletions: list[str] | Unset = UNSET
        if not isinstance(self.language_deletions, Unset):
            language_deletions = self.language_deletions

        original_language = self.original_language

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if resource_updates is not UNSET:
            field_dict["resourceUpdates"] = resource_updates
        if resource_deletions is not UNSET:
            field_dict["resourceDeletions"] = resource_deletions
        if lock_updates is not UNSET:
            field_dict["lockUpdates"] = lock_updates
        if lock_deletions is not UNSET:
            field_dict["lockDeletions"] = lock_deletions
        if role_updates is not UNSET:
            field_dict["roleUpdates"] = role_updates
        if role_deletions is not UNSET:
            field_dict["roleDeletions"] = role_deletions
        if language_updates is not UNSET:
            field_dict["languageUpdates"] = language_updates
        if language_deletions is not UNSET:
            field_dict["languageDeletions"] = language_deletions
        if original_language is not UNSET:
            field_dict["originalLanguage"] = original_language

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        from ..models.database_lock import DatabaseLock
        from ..models.database_resource import DatabaseResource
        from ..models.database_role import DatabaseRole

        d = dict(src_dict)
        _resource_updates = d.pop("resourceUpdates", UNSET)
        resource_updates: list[DatabaseResource] | Unset = UNSET
        if _resource_updates is not UNSET:
            resource_updates = []
            for resource_updates_item_data in _resource_updates:
                resource_updates_item = DatabaseResource.from_dict(
                    resource_updates_item_data
                )

                resource_updates.append(resource_updates_item)

        resource_deletions = cast(list[str], d.pop("resourceDeletions", UNSET))

        _lock_updates = d.pop("lockUpdates", UNSET)
        lock_updates: list[DatabaseLock] | Unset = UNSET
        if _lock_updates is not UNSET:
            lock_updates = []
            for lock_updates_item_data in _lock_updates:
                lock_updates_item = DatabaseLock.from_dict(lock_updates_item_data)

                lock_updates.append(lock_updates_item)

        lock_deletions = cast(list[str], d.pop("lockDeletions", UNSET))

        _role_updates = d.pop("roleUpdates", UNSET)
        role_updates: list[DatabaseRole] | Unset = UNSET
        if _role_updates is not UNSET:
            role_updates = []
            for role_updates_item_data in _role_updates:
                role_updates_item = DatabaseRole.from_dict(role_updates_item_data)

                role_updates.append(role_updates_item)

        role_deletions = cast(list[str], d.pop("roleDeletions", UNSET))

        language_updates = cast(list[str], d.pop("languageUpdates", UNSET))

        language_deletions = cast(list[str], d.pop("languageDeletions", UNSET))

        original_language = d.pop("originalLanguage", UNSET)

        update_database_request = cls(
            resource_updates=resource_updates,
            resource_deletions=resource_deletions,
            lock_updates=lock_updates,
            lock_deletions=lock_deletions,
            role_updates=role_updates,
            role_deletions=role_deletions,
            language_updates=language_updates,
            language_deletions=language_deletions,
            original_language=original_language,
        )

        update_database_request.additional_properties = d
        return update_database_request

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
