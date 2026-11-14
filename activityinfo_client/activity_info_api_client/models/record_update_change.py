from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.record_update_change_fields import RecordUpdateChangeFields


T = TypeVar("T", bound="RecordUpdateChange")


@_attrs_define
class RecordUpdateChange:
    """
    Attributes:
        form_id (str):
        record_id (str):
        fields (RecordUpdateChangeFields):
        parent_record_id (str | Unset):
        deleted (bool | Unset):  Default: False.
    """

    form_id: str
    record_id: str
    fields: RecordUpdateChangeFields
    parent_record_id: str | Unset = UNSET
    deleted: bool | Unset = False
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        form_id = self.form_id

        record_id = self.record_id

        fields = self.fields.to_dict()

        parent_record_id = self.parent_record_id

        deleted = self.deleted

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "formId": form_id,
                "recordId": record_id,
                "fields": fields,
            }
        )
        if parent_record_id is not UNSET:
            field_dict["parentRecordId"] = parent_record_id
        if deleted is not UNSET:
            field_dict["deleted"] = deleted

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.record_update_change_fields import RecordUpdateChangeFields

        d = dict(src_dict)
        form_id = d.pop("formId")

        record_id = d.pop("recordId")

        fields = RecordUpdateChangeFields.from_dict(d.pop("fields"))

        parent_record_id = d.pop("parentRecordId", UNSET)

        deleted = d.pop("deleted", UNSET)

        record_update_change = cls(
            form_id=form_id,
            record_id=record_id,
            fields=fields,
            parent_record_id=parent_record_id,
            deleted=deleted,
        )

        record_update_change.additional_properties = d
        return record_update_change

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
