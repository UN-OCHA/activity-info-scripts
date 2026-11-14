from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="TranslationDictionaryId")


@_attrs_define
class TranslationDictionaryId:
    """
    Attributes:
        database_id (str): The database to which this dictionary belongs
        dictionary_id (str): The dictionary id. This will either be "database" or "form/{formId}."
    """

    database_id: str
    dictionary_id: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        database_id = self.database_id

        dictionary_id = self.dictionary_id

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "databaseId": database_id,
                "dictionaryId": dictionary_id,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        database_id = d.pop("databaseId")

        dictionary_id = d.pop("dictionaryId")

        translation_dictionary_id = cls(
            database_id=database_id,
            dictionary_id=dictionary_id,
        )

        translation_dictionary_id.additional_properties = d
        return translation_dictionary_id

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
