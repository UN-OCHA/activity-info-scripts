from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.translation_dictionary_id import TranslationDictionaryId
    from ..models.translation_string import TranslationString


T = TypeVar("T", bound="TranslationDictionary")


@_attrs_define
class TranslationDictionary:
    """
    Attributes:
        id (TranslationDictionaryId):
        version (int): The current version number of the dictionary. The version number is incremented each time the
            dictionary is updated.
        language (str): The two-letter ISO 639-1 code for the language, e.g. 'en' for English.
        translated_strings (list[TranslationString]): The list of translated strings.
    """

    id: TranslationDictionaryId
    version: int
    language: str
    translated_strings: list[TranslationString]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id.to_dict()

        version = self.version

        language = self.language

        translated_strings = []
        for translated_strings_item_data in self.translated_strings:
            translated_strings_item = translated_strings_item_data.to_dict()
            translated_strings.append(translated_strings_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "version": version,
                "language": language,
                "translatedStrings": translated_strings,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.translation_dictionary_id import TranslationDictionaryId
        from ..models.translation_string import TranslationString

        d = dict(src_dict)
        id = TranslationDictionaryId.from_dict(d.pop("id"))

        version = d.pop("version")

        language = d.pop("language")

        translated_strings = []
        _translated_strings = d.pop("translatedStrings")
        for translated_strings_item_data in _translated_strings:
            translated_strings_item = TranslationString.from_dict(translated_strings_item_data)

            translated_strings.append(translated_strings_item)

        translation_dictionary = cls(
            id=id,
            version=version,
            language=language,
            translated_strings=translated_strings,
        )

        translation_dictionary.additional_properties = d
        return translation_dictionary

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
