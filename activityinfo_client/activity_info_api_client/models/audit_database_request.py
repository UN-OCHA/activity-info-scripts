from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.audit_database_request_type_filter_item import AuditDatabaseRequestTypeFilterItem
from ..types import UNSET, Unset

T = TypeVar("T", bound="AuditDatabaseRequest")


@_attrs_define
class AuditDatabaseRequest:
    """
    Attributes:
        start_time (int): The start time of the request, in milliseconds since the unix epoch. The results will include
            the first 100 - 150 events that occurred before this time.
        resource_filter (str | Unset): The id of a form, folder, or report. If provided, the result will only include
            events that relate to this resource.
        type_filter (list[AuditDatabaseRequestTypeFilterItem] | Unset): Only include events of the given types.
        end_time (int | Unset): The end time of the request, in milliseconds since the unix epoch. The results will
            include the events that occurred after the end time and before the start time.
    """

    start_time: int
    resource_filter: str | Unset = UNSET
    type_filter: list[AuditDatabaseRequestTypeFilterItem] | Unset = UNSET
    end_time: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        start_time = self.start_time

        resource_filter = self.resource_filter

        type_filter: list[str] | Unset = UNSET
        if not isinstance(self.type_filter, Unset):
            type_filter = []
            for type_filter_item_data in self.type_filter:
                type_filter_item = type_filter_item_data.value
                type_filter.append(type_filter_item)

        end_time = self.end_time

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "startTime": start_time,
            }
        )
        if resource_filter is not UNSET:
            field_dict["resourceFilter"] = resource_filter
        if type_filter is not UNSET:
            field_dict["typeFilter"] = type_filter
        if end_time is not UNSET:
            field_dict["endTime"] = end_time

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        start_time = d.pop("startTime")

        resource_filter = d.pop("resourceFilter", UNSET)

        _type_filter = d.pop("typeFilter", UNSET)
        type_filter: list[AuditDatabaseRequestTypeFilterItem] | Unset = UNSET
        if _type_filter is not UNSET:
            type_filter = []
            for type_filter_item_data in _type_filter:
                type_filter_item = AuditDatabaseRequestTypeFilterItem(type_filter_item_data)

                type_filter.append(type_filter_item)

        end_time = d.pop("endTime", UNSET)

        audit_database_request = cls(
            start_time=start_time,
            resource_filter=resource_filter,
            type_filter=type_filter,
            end_time=end_time,
        )

        audit_database_request.additional_properties = d
        return audit_database_request

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
