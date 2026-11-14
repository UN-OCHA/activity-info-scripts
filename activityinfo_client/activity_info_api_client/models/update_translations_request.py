from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.translation_string import TranslationString


T = TypeVar("T", bound="UpdateTranslationsRequest")


@_attrs_define
class UpdateTranslationsRequest:
    """
    Attributes:
        strings (list[TranslationString]):
    """

    strings: list[TranslationString]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        strings = []
        for strings_item_data in self.strings:
            strings_item = strings_item_data.to_dict()
            strings.append(strings_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "strings": strings,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.translation_string import TranslationString

        d = dict(src_dict)
        strings = []
        _strings = d.pop("strings")
        for strings_item_data in _strings:
            strings_item = TranslationString.from_dict(strings_item_data)

            strings.append(strings_item)

        update_translations_request = cls(
            strings=strings,
        )

        update_translations_request.additional_properties = d
        return update_translations_request

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
