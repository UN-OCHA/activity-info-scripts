from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Self, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.form_schema_element import FormSchemaElement


T = TypeVar("T", bound="FormSchema")


@_attrs_define
class FormSchema:
    """
    Attributes:
        id (str): The immutable CUID for this form. Must be globally unique within an ActivityInfo server
        label (str): A human-readable label for this form
        schema_version (int): A monotonically increasing version number of this schema assigned by the server upon
            updates
        database_id (str): The id of the database to which this form belongs
        elements (list[FormSchemaElement]): This form's fields, section headers, and other elements.
        parent_form_id (str | Unset): The id of this form's parent, if this form is a subform. Note that subforms can
            only be created by adding a subform field to the parent form.
        record_label_field_id (str | Unset):
    """

    id: str
    label: str
    schema_version: int
    database_id: str
    elements: list[FormSchemaElement]
    parent_form_id: str | Unset = UNSET
    record_label_field_id: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        label = self.label

        schema_version = self.schema_version

        database_id = self.database_id

        elements = []
        for elements_item_data in self.elements:
            elements_item = elements_item_data.to_dict()
            elements.append(elements_item)

        parent_form_id = self.parent_form_id

        record_label_field_id = self.record_label_field_id

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "label": label,
                "schemaVersion": schema_version,
                "databaseId": database_id,
                "elements": elements,
            }
        )
        if parent_form_id is not UNSET:
            field_dict["parentFormId"] = parent_form_id
        if record_label_field_id is not UNSET:
            field_dict["recordLabelFieldId"] = record_label_field_id

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        from ..models.form_schema_element import FormSchemaElement

        d = dict(src_dict)
        id = d.pop("id")

        label = d.pop("label")

        schema_version = d.pop("schemaVersion")

        database_id = d.pop("databaseId")

        elements = []
        _elements = d.pop("elements")
        for elements_item_data in _elements:
            elements_item = FormSchemaElement.from_dict(elements_item_data)

            elements.append(elements_item)

        parent_form_id = d.pop("parentFormId", UNSET)

        record_label_field_id = d.pop("recordLabelFieldId", UNSET)

        form_schema = cls(
            id=id,
            label=label,
            schema_version=schema_version,
            database_id=database_id,
            elements=elements,
            parent_form_id=parent_form_id,
            record_label_field_id=record_label_field_id,
        )

        form_schema.additional_properties = d
        return form_schema

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
