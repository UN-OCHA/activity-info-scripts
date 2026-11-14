from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.database_resource import DatabaseResource
    from ..models.form_schema import FormSchema


T = TypeVar("T", bound="AddFormRequest")


@_attrs_define
class AddFormRequest:
    """
    Attributes:
        form_resource (DatabaseResource): A resource (folder, form or subform) that belongs to a database
        form_class (FormSchema):
    """

    form_resource: DatabaseResource
    form_class: FormSchema
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        form_resource = self.form_resource.to_dict()

        form_class = self.form_class.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "formResource": form_resource,
                "formClass": form_class,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.database_resource import DatabaseResource
        from ..models.form_schema import FormSchema

        d = dict(src_dict)
        form_resource = DatabaseResource.from_dict(d.pop("formResource"))

        form_class = FormSchema.from_dict(d.pop("formClass"))

        add_form_request = cls(
            form_resource=form_resource,
            form_class=form_class,
        )

        add_form_request.additional_properties = d
        return add_form_request

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
