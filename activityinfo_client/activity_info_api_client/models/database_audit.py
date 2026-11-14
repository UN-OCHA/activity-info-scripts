from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Self, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.database_audit_events import DatabaseAuditEvents


T = TypeVar("T", bound="DatabaseAudit")


@_attrs_define
class DatabaseAudit:
    """
    Attributes:
        start_time (int): The start time of the request, in milliseconds since the unix epoch. The results will include
            the first 100 - 150 events that occurred before this time.
        end_time (int): The end time of the request, in milliseconds since the unix epoch. The results will include the
            events that occurred after the end time and before the start time.
        more_events (bool): True if there are more events earlier than endTime available, but not included in this
            response. Submit a new request with a startTime equal to the endTime to fetch the next batch.
        events (list[DatabaseAuditEvents] | Unset): List of matching auditable events.
    """

    start_time: int
    end_time: int
    more_events: bool
    events: list[DatabaseAuditEvents] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        start_time = self.start_time

        end_time = self.end_time

        more_events = self.more_events

        events: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.events, Unset):
            events = []
            for events_item_data in self.events:
                events_item = events_item_data.to_dict()
                events.append(events_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "startTime": start_time,
                "endTime": end_time,
                "moreEvents": more_events,
            }
        )
        if events is not UNSET:
            field_dict["events"] = events

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        from ..models.database_audit_events import DatabaseAuditEvents

        d = dict(src_dict)
        start_time = d.pop("startTime")

        end_time = d.pop("endTime")

        more_events = d.pop("moreEvents")

        _events = d.pop("events", UNSET)
        events: list[DatabaseAuditEvents] | Unset = UNSET
        if _events is not UNSET:
            events = []
            for events_item_data in _events:
                events_item = DatabaseAuditEvents.from_dict(events_item_data)

                events.append(events_item)

        database_audit = cls(
            start_time=start_time,
            end_time=end_time,
            more_events=more_events,
            events=events,
        )

        database_audit.additional_properties = d
        return database_audit

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
