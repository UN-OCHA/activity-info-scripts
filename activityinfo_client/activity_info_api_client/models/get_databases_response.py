from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="GetDatabasesResponse")


@_attrs_define
class GetDatabasesResponse:
    """
    Attributes:
        database_id (str): The globally-unique id of this database.
        label (str): The human-readable name of this database.
        description (str | Unset): A longer, optional description for this database.
        owner_id (str | Unset): The id of the user who owns this database.
        billing_account_id (int | Unset): The id of the billing account to which this database belongs
        suspended (bool | Unset): True if this database belongs to a billing account that has been suspended.
        published_template (bool | Unset): True if this database has been published as a template.
        languages (list[str] | Unset): The list of languages that have been defined for this database, including the
            original language, if set, and any translations.
    """

    database_id: str
    label: str
    description: str | Unset = UNSET
    owner_id: str | Unset = UNSET
    billing_account_id: int | Unset = UNSET
    suspended: bool | Unset = UNSET
    published_template: bool | Unset = UNSET
    languages: list[str] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        database_id = self.database_id

        label = self.label

        description = self.description

        owner_id = self.owner_id

        billing_account_id = self.billing_account_id

        suspended = self.suspended

        published_template = self.published_template

        languages: list[str] | Unset = UNSET
        if not isinstance(self.languages, Unset):
            languages = self.languages

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "databaseId": database_id,
                "label": label,
            }
        )
        if description is not UNSET:
            field_dict["description"] = description
        if owner_id is not UNSET:
            field_dict["ownerId"] = owner_id
        if billing_account_id is not UNSET:
            field_dict["billingAccountId"] = billing_account_id
        if suspended is not UNSET:
            field_dict["suspended"] = suspended
        if published_template is not UNSET:
            field_dict["publishedTemplate"] = published_template
        if languages is not UNSET:
            field_dict["languages"] = languages

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        database_id = d.pop("databaseId")

        label = d.pop("label")

        description = d.pop("description", UNSET)

        owner_id = d.pop("ownerId", UNSET)

        billing_account_id = d.pop("billingAccountId", UNSET)

        suspended = d.pop("suspended", UNSET)

        published_template = d.pop("publishedTemplate", UNSET)

        languages = cast(list[str], d.pop("languages", UNSET))

        get_databases_response = cls(
            database_id=database_id,
            label=label,
            description=description,
            owner_id=owner_id,
            billing_account_id=billing_account_id,
            suspended=suspended,
            published_template=published_template,
            languages=languages,
        )

        get_databases_response.additional_properties = d
        return get_databases_response

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
