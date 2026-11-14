from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.user_preflight_response_activation_status import UserPreflightResponseActivationStatus
from ..types import UNSET, Unset

T = TypeVar("T", bound="UserPreflightResponse")


@_attrs_define
class UserPreflightResponse:
    """
    Attributes:
        activation_status (UserPreflightResponseActivationStatus):
        added_to_database (bool):
        valid_email (bool):
        user_id (str | Unset):
        name (str | Unset):
        localized_error_message (str | Unset):
    """

    activation_status: UserPreflightResponseActivationStatus
    added_to_database: bool
    valid_email: bool
    user_id: str | Unset = UNSET
    name: str | Unset = UNSET
    localized_error_message: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        activation_status = self.activation_status.value

        added_to_database = self.added_to_database

        valid_email = self.valid_email

        user_id = self.user_id

        name = self.name

        localized_error_message = self.localized_error_message

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "activationStatus": activation_status,
                "addedToDatabase": added_to_database,
                "validEmail": valid_email,
            }
        )
        if user_id is not UNSET:
            field_dict["userId"] = user_id
        if name is not UNSET:
            field_dict["name"] = name
        if localized_error_message is not UNSET:
            field_dict["localizedErrorMessage"] = localized_error_message

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        activation_status = UserPreflightResponseActivationStatus(d.pop("activationStatus"))

        added_to_database = d.pop("addedToDatabase")

        valid_email = d.pop("validEmail")

        user_id = d.pop("userId", UNSET)

        name = d.pop("name", UNSET)

        localized_error_message = d.pop("localizedErrorMessage", UNSET)

        user_preflight_response = cls(
            activation_status=activation_status,
            added_to_database=added_to_database,
            valid_email=valid_email,
            user_id=user_id,
            name=name,
            localized_error_message=localized_error_message,
        )

        user_preflight_response.additional_properties = d
        return user_preflight_response

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
