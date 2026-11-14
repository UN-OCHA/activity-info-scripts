from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Self, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.user_activation_status import UserActivationStatus
from ..models.user_delivery_status import UserDeliveryStatus
from ..models.user_user_license_type import UserUserLicenseType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.user_role import UserRole


T = TypeVar("T", bound="User")


@_attrs_define
class User:
    """
    Attributes:
        database_id (str):
        user_id (str):
        name (str): The user's name, provided either when invited or by the user themselves after activating their
            account.
        email (str): This user's email address.
        role (UserRole):
        version (int | Unset): A monotonically-increasing version number. This version is incremented each time a change
            is made to the users permissions.
        invite_date (datetime.date | Unset): The date on which the user was invited to this database.
        invite_time (float | Unset):
        delivery_status (UserDeliveryStatus | Unset):
        invite_accepted (bool | Unset): True if the user has accepted the invitation to create an ActivityInfo account.
        locked (bool | Unset): True if this user's account has been locked following too many failed authentication
            attempts.
        user_license_type (UserUserLicenseType | Unset): The type of user license required for this user, in this
            database.
        last_login_date (datetime.date | None | Unset): Date of last login on the platform.
        last_login_time (float | Unset):
        activation_status (UserActivationStatus | Unset): The user's current account status.
    """

    database_id: str
    user_id: str
    name: str
    email: str
    role: UserRole
    version: int | Unset = UNSET
    invite_date: datetime.date | Unset = UNSET
    invite_time: float | Unset = UNSET
    delivery_status: UserDeliveryStatus | Unset = UNSET
    invite_accepted: bool | Unset = UNSET
    locked: bool | Unset = UNSET
    user_license_type: UserUserLicenseType | Unset = UNSET
    last_login_date: datetime.date | None | Unset = UNSET
    last_login_time: float | Unset = UNSET
    activation_status: UserActivationStatus | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        database_id = self.database_id

        user_id = self.user_id

        name = self.name

        email = self.email

        role = self.role.to_dict()

        version = self.version

        invite_date: str | Unset = UNSET
        if not isinstance(self.invite_date, Unset):
            invite_date = self.invite_date.isoformat()

        invite_time = self.invite_time

        delivery_status: str | Unset = UNSET
        if not isinstance(self.delivery_status, Unset):
            delivery_status = self.delivery_status.value

        invite_accepted = self.invite_accepted

        locked = self.locked

        user_license_type: str | Unset = UNSET
        if not isinstance(self.user_license_type, Unset):
            user_license_type = self.user_license_type.value

        last_login_date: None | str | Unset
        if isinstance(self.last_login_date, Unset):
            last_login_date = UNSET
        elif isinstance(self.last_login_date, datetime.date):
            last_login_date = self.last_login_date.isoformat()
        else:
            last_login_date = self.last_login_date

        last_login_time = self.last_login_time

        activation_status: str | Unset = UNSET
        if not isinstance(self.activation_status, Unset):
            activation_status = self.activation_status.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "databaseId": database_id,
                "userId": user_id,
                "name": name,
                "email": email,
                "role": role,
            }
        )
        if version is not UNSET:
            field_dict["version"] = version
        if invite_date is not UNSET:
            field_dict["inviteDate"] = invite_date
        if invite_time is not UNSET:
            field_dict["inviteTime"] = invite_time
        if delivery_status is not UNSET:
            field_dict["deliveryStatus"] = delivery_status
        if invite_accepted is not UNSET:
            field_dict["inviteAccepted"] = invite_accepted
        if locked is not UNSET:
            field_dict["locked"] = locked
        if user_license_type is not UNSET:
            field_dict["userLicenseType"] = user_license_type
        if last_login_date is not UNSET:
            field_dict["lastLoginDate"] = last_login_date
        if last_login_time is not UNSET:
            field_dict["lastLoginTime"] = last_login_time
        if activation_status is not UNSET:
            field_dict["activationStatus"] = activation_status

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        from ..models.user_role import UserRole

        d = dict(src_dict)
        database_id = d.pop("databaseId")

        user_id = d.pop("userId")

        name = d.pop("name")

        email = d.pop("email")

        role = UserRole.from_dict(d.pop("role"))

        version = d.pop("version", UNSET)

        _invite_date = d.pop("inviteDate", UNSET)
        invite_date: datetime.date | Unset
        if isinstance(_invite_date, Unset):
            invite_date = UNSET
        else:
            invite_date = datetime.date.fromisoformat(_invite_date)

        invite_time = d.pop("inviteTime", UNSET)

        _delivery_status = d.pop("deliveryStatus", UNSET)
        delivery_status: UserDeliveryStatus | Unset
        if isinstance(_delivery_status, Unset):
            delivery_status = UNSET
        else:
            delivery_status = UserDeliveryStatus(_delivery_status)

        invite_accepted = d.pop("inviteAccepted", UNSET)

        locked = d.pop("locked", UNSET)

        _user_license_type = d.pop("userLicenseType", UNSET)
        user_license_type: UserUserLicenseType | Unset
        if isinstance(_user_license_type, Unset):
            user_license_type = UNSET
        else:
            user_license_type = UserUserLicenseType(_user_license_type)

        def _parse_last_login_date(data: object) -> datetime.date | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                last_login_date_type_0 = datetime.date.fromisoformat(data)

                return last_login_date_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(datetime.date | None | Unset, data)

        last_login_date = _parse_last_login_date(d.pop("lastLoginDate", UNSET))

        last_login_time = d.pop("lastLoginTime", UNSET)

        _activation_status = d.pop("activationStatus", UNSET)
        activation_status: UserActivationStatus | Unset
        if isinstance(_activation_status, Unset):
            activation_status = UNSET
        else:
            activation_status = UserActivationStatus(_activation_status)

        user = cls(
            database_id=database_id,
            user_id=user_id,
            name=name,
            email=email,
            role=role,
            version=version,
            invite_date=invite_date,
            invite_time=invite_time,
            delivery_status=delivery_status,
            invite_accepted=invite_accepted,
            locked=locked,
            user_license_type=user_license_type,
            last_login_date=last_login_date,
            last_login_time=last_login_time,
            activation_status=activation_status,
        )

        user.additional_properties = d
        return user

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
