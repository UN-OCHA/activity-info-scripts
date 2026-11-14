from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Self, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.database_lock_date_range import DatabaseLockDateRange


T = TypeVar("T", bound="DatabaseLock")


@_attrs_define
class DatabaseLock:
    """A record lock that has been defined for a database

    Attributes:
        id (str): This lock's id.
        label (str): A human-readable label for this lock
        resource_id (str): The resource (database, folder, form, or subform) to which this lock applies.
        deactivated (bool): Whether or not this lock is active. True when deactivated Default: False.
        date_range (DatabaseLockDateRange | None | Unset): The date range to which this lock applies, if it is a date-
            range lock.
        formula (str | Unset): The formula to which this lock applies, if it is a rule-based lock.
        message (str | Unset): user description of the rule-based lock.
    """

    id: str
    label: str
    resource_id: str
    deactivated: bool = False
    date_range: DatabaseLockDateRange | None | Unset = UNSET
    formula: str | Unset = UNSET
    message: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.database_lock_date_range import DatabaseLockDateRange

        id = self.id

        label = self.label

        resource_id = self.resource_id

        deactivated = self.deactivated

        date_range: dict[str, Any] | None | Unset
        if isinstance(self.date_range, Unset):
            date_range = UNSET
        elif isinstance(self.date_range, DatabaseLockDateRange):
            date_range = self.date_range.to_dict()
        else:
            date_range = self.date_range

        formula = self.formula

        message = self.message

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "label": label,
                "resourceId": resource_id,
                "deactivated": deactivated,
            }
        )
        if date_range is not UNSET:
            field_dict["dateRange"] = date_range
        if formula is not UNSET:
            field_dict["formula"] = formula
        if message is not UNSET:
            field_dict["message"] = message

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        from ..models.database_lock_date_range import DatabaseLockDateRange

        d = dict(src_dict)
        id = d.pop("id")

        label = d.pop("label")

        resource_id = d.pop("resourceId")

        deactivated = d.pop("deactivated")

        def _parse_date_range(data: object) -> DatabaseLockDateRange | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                date_range_type_0 = DatabaseLockDateRange.from_dict(data)

                return date_range_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(DatabaseLockDateRange | None | Unset, data)

        date_range = _parse_date_range(d.pop("dateRange", UNSET))

        formula = d.pop("formula", UNSET)

        message = d.pop("message", UNSET)

        database_lock = cls(
            id=id,
            label=label,
            resource_id=resource_id,
            deactivated=deactivated,
            date_range=date_range,
            formula=formula,
            message=message,
        )

        database_lock.additional_properties = d
        return database_lock

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
