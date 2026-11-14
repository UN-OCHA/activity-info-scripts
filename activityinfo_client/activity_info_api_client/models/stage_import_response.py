from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="StageImportResponse")


@_attrs_define
class StageImportResponse:
    """
    Attributes:
        upload_url (str): Pre-signed URL to upload staged import payload.
        import_id (str): Identifier used when starting an importRecords job.
    """

    upload_url: str
    import_id: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        upload_url = self.upload_url

        import_id = self.import_id

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "uploadUrl": upload_url,
                "importId": import_id,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        upload_url = d.pop("uploadUrl")

        import_id = d.pop("importId")

        stage_import_response = cls(
            upload_url=upload_url,
            import_id=import_id,
        )

        stage_import_response.additional_properties = d
        return stage_import_response

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
