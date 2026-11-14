from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Self, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.record_update_change import RecordUpdateChange


T = TypeVar("T", bound="RecordUpdateRequest")


@_attrs_define
class RecordUpdateRequest:
    """
    Attributes:
        changes (list[RecordUpdateChange]):
        from_import_context (bool | Unset): True if update request comes from bulk importer
    """

    changes: list[RecordUpdateChange]
    from_import_context: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        changes = []
        for changes_item_data in self.changes:
            changes_item = changes_item_data.to_dict()
            changes.append(changes_item)

        from_import_context = self.from_import_context

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "changes": changes,
            }
        )
        if from_import_context is not UNSET:
            field_dict["fromImportContext"] = from_import_context

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        from ..models.record_update_change import RecordUpdateChange

        d = dict(src_dict)
        changes = []
        _changes = d.pop("changes")
        for changes_item_data in _changes:
            changes_item = RecordUpdateChange.from_dict(changes_item_data)

            changes.append(changes_item)

        from_import_context = d.pop("fromImportContext", UNSET)

        record_update_request = cls(
            changes=changes,
            from_import_context=from_import_context,
        )

        record_update_request.additional_properties = d
        return record_update_request

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
