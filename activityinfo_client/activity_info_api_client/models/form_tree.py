from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Self, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.form_tree_forms import FormTreeForms


T = TypeVar("T", bound="FormTree")


@_attrs_define
class FormTree:
    """
    Attributes:
        root (str): The id of the requested form
        forms (FormTreeForms):
    """

    root: str
    forms: FormTreeForms
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        root = self.root

        forms = self.forms.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "root": root,
                "forms": forms,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        from ..models.form_tree_forms import FormTreeForms

        d = dict(src_dict)
        root = d.pop("root")

        forms = FormTreeForms.from_dict(d.pop("forms"))

        form_tree = cls(
            root=root,
            forms=forms,
        )

        form_tree.additional_properties = d
        return form_tree

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
