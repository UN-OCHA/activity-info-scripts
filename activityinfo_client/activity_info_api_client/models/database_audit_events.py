from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Self, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.database_audit_events_resource_types_item import (
    DatabaseAuditEventsResourceTypesItem,
)
from ..models.database_audit_events_type import DatabaseAuditEventsType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.database_automation import DatabaseAutomation
    from ..models.user_ref import UserRef


T = TypeVar("T", bound="DatabaseAuditEvents")


@_attrs_define
class DatabaseAuditEvents:
    """
    Attributes:
        id (str): An opaque, id uniquely identifying this event within this result set.
        time (int): The time of this event, in seconds since 1970-01-01.
        description (str): A human-readable description of the event.
        type_ (DatabaseAuditEventsType): The type of event. Values: DATABASE_TREE, USER, RECORD, FORM, AUTOMATION
        resource_types (list[DatabaseAuditEventsResourceTypesItem]): One or more type(s) of the resource(s) affected
        resource_id (str): Id of the affected resource OR id of the database if more than one resource type is present
        added (bool): True if this event concerns an addition.
        updated (bool): True if this event concerns an update.
        deleted (bool): True if this event concerns an deletion.
        recovered (bool): True if this event concerns a recovery of data.
        reverted (bool): True if this event has been reverted by an administrator.
        user (UserRef | Unset):
        record_ref (str | Unset): The record ref, if this event concerns a record.
        form_id (str | Unset): The form id, if this event concerns a form.
        version (int | Unset): The version number of the affected resource.
        database_user_id (str | Unset): For user permission events, this is the id of the user who was affected.
        automation (DatabaseAutomation | Unset):
        merge_id (str | Unset): The merge id, if this event concerns a merge of records.
    """

    id: str
    time: int
    description: str
    type_: DatabaseAuditEventsType
    resource_types: list[DatabaseAuditEventsResourceTypesItem]
    resource_id: str
    added: bool
    updated: bool
    deleted: bool
    recovered: bool
    reverted: bool
    user: UserRef | Unset = UNSET
    record_ref: str | Unset = UNSET
    form_id: str | Unset = UNSET
    version: int | Unset = UNSET
    database_user_id: str | Unset = UNSET
    automation: DatabaseAutomation | Unset = UNSET
    merge_id: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        time = self.time

        description = self.description

        type_ = self.type_.value

        resource_types = []
        for resource_types_item_data in self.resource_types:
            resource_types_item = resource_types_item_data.value
            resource_types.append(resource_types_item)

        resource_id = self.resource_id

        added = self.added

        updated = self.updated

        deleted = self.deleted

        recovered = self.recovered

        reverted = self.reverted

        user: dict[str, Any] | Unset = UNSET
        if not isinstance(self.user, Unset):
            user = self.user.to_dict()

        record_ref = self.record_ref

        form_id = self.form_id

        version = self.version

        database_user_id = self.database_user_id

        automation: dict[str, Any] | Unset = UNSET
        if not isinstance(self.automation, Unset):
            automation = self.automation.to_dict()

        merge_id = self.merge_id

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "time": time,
                "description": description,
                "type": type_,
                "resourceTypes": resource_types,
                "resourceId": resource_id,
                "added": added,
                "updated": updated,
                "deleted": deleted,
                "recovered": recovered,
                "reverted": reverted,
            }
        )
        if user is not UNSET:
            field_dict["user"] = user
        if record_ref is not UNSET:
            field_dict["recordRef"] = record_ref
        if form_id is not UNSET:
            field_dict["formId"] = form_id
        if version is not UNSET:
            field_dict["version"] = version
        if database_user_id is not UNSET:
            field_dict["databaseUserId"] = database_user_id
        if automation is not UNSET:
            field_dict["automation"] = automation
        if merge_id is not UNSET:
            field_dict["mergeId"] = merge_id

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        from ..models.database_automation import DatabaseAutomation
        from ..models.user_ref import UserRef

        d = dict(src_dict)
        id = d.pop("id")

        time = d.pop("time")

        description = d.pop("description")

        type_ = DatabaseAuditEventsType(d.pop("type"))

        resource_types = []
        _resource_types = d.pop("resourceTypes")
        for resource_types_item_data in _resource_types:
            resource_types_item = DatabaseAuditEventsResourceTypesItem(
                resource_types_item_data
            )

            resource_types.append(resource_types_item)

        resource_id = d.pop("resourceId")

        added = d.pop("added")

        updated = d.pop("updated")

        deleted = d.pop("deleted")

        recovered = d.pop("recovered")

        reverted = d.pop("reverted")

        _user = d.pop("user", UNSET)
        user: UserRef | Unset
        if isinstance(_user, Unset):
            user = UNSET
        else:
            user = UserRef.from_dict(_user)

        record_ref = d.pop("recordRef", UNSET)

        form_id = d.pop("formId", UNSET)

        version = d.pop("version", UNSET)

        database_user_id = d.pop("databaseUserId", UNSET)

        _automation = d.pop("automation", UNSET)
        automation: DatabaseAutomation | Unset
        if isinstance(_automation, Unset):
            automation = UNSET
        else:
            automation = DatabaseAutomation.from_dict(_automation)

        merge_id = d.pop("mergeId", UNSET)

        database_audit_events = cls(
            id=id,
            time=time,
            description=description,
            type_=type_,
            resource_types=resource_types,
            resource_id=resource_id,
            added=added,
            updated=updated,
            deleted=deleted,
            recovered=recovered,
            reverted=reverted,
            user=user,
            record_ref=record_ref,
            form_id=form_id,
            version=version,
            database_user_id=database_user_id,
            automation=automation,
            merge_id=merge_id,
        )

        database_audit_events.additional_properties = d
        return database_audit_events

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
