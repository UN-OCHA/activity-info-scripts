from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.field_condition import FieldCondition
    from ..models.permission import Permission


T = TypeVar("T", bound="Grant")


@_attrs_define
class Grant:
    """A set of permissions for a single database resource (database, folder, form, subform) that is either associated with
    a role or given directly to a user.

        Attributes:
            resource_id (str): the database, folder, form or subform being granted access to.
            optional (bool): An optional resource is granted selectively to individual users assigned to a role. Default:
                False.
            operations (list[Permission]): The set of operations (add/edit/delete etc.) and their record-level conditions
                permitted on this resource
            conditions (list[FieldCondition] | Unset): The set of field-level conditions applied to this resource
    """

    resource_id: str
    operations: list[Permission]
    optional: bool = False
    conditions: list[FieldCondition] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        resource_id = self.resource_id

        optional = self.optional

        operations = []
        for operations_item_data in self.operations:
            operations_item = operations_item_data.to_dict()
            operations.append(operations_item)

        conditions: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.conditions, Unset):
            conditions = []
            for conditions_item_data in self.conditions:
                conditions_item = conditions_item_data.to_dict()
                conditions.append(conditions_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "resourceId": resource_id,
                "optional": optional,
                "operations": operations,
            }
        )
        if conditions is not UNSET:
            field_dict["conditions"] = conditions

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.field_condition import FieldCondition
        from ..models.permission import Permission

        d = dict(src_dict)
        resource_id = d.pop("resourceId")

        optional = d.pop("optional")

        operations = []
        _operations = d.pop("operations")
        for operations_item_data in _operations:
            operations_item = Permission.from_dict(operations_item_data)

            operations.append(operations_item)

        _conditions = d.pop("conditions", UNSET)
        conditions: list[FieldCondition] | Unset = UNSET
        if _conditions is not UNSET:
            conditions = []
            for conditions_item_data in _conditions:
                conditions_item = FieldCondition.from_dict(conditions_item_data)

                conditions.append(conditions_item)

        grant = cls(
            resource_id=resource_id,
            optional=optional,
            operations=operations,
            conditions=conditions,
        )

        grant.additional_properties = d
        return grant

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
