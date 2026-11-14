from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Self, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.database import Database
    from ..models.form import Form


T = TypeVar("T", bound="AddUpdateFormResponse")


@_attrs_define
class AddUpdateFormResponse:
    """
    Attributes:
        database (Database):
        forms (list[Form]):
    """

    database: Database
    forms: list[Form]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        database = self.database.to_dict()

        forms = []
        for forms_item_data in self.forms:
            forms_item = forms_item_data.to_dict()
            forms.append(forms_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "database": database,
                "forms": forms,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        from ..models.database import Database
        from ..models.form import Form

        d = dict(src_dict)
        database = Database.from_dict(d.pop("database"))

        forms = []
        _forms = d.pop("forms")
        for forms_item_data in _forms:
            forms_item = Form.from_dict(forms_item_data)

            forms.append(forms_item)

        add_update_form_response = cls(
            database=database,
            forms=forms,
        )

        add_update_form_response.additional_properties = d
        return add_update_form_response

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
