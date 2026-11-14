from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.field_condition_criteria import FieldConditionCriteria
from ..models.field_condition_operations_item import FieldConditionOperationsItem

if TYPE_CHECKING:
    from ..models.field_condition_rule import FieldConditionRule


T = TypeVar("T", bound="FieldCondition")


@_attrs_define
class FieldCondition:
    """A field-level condition associated with a Grant for a single database resource (database, folder, form, subform)

    Attributes:
        operations (list[FieldConditionOperationsItem]): The field operations (view and/or edit) allowed for this
            condition
        criteria (FieldConditionCriteria): Whether the condition requires all or any of the condition's rules to be met
        rules (list[FieldConditionRule]): The condition's rules
    """

    operations: list[FieldConditionOperationsItem]
    criteria: FieldConditionCriteria
    rules: list[FieldConditionRule]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        operations = []
        for operations_item_data in self.operations:
            operations_item = operations_item_data.value
            operations.append(operations_item)

        criteria = self.criteria.value

        rules = []
        for rules_item_data in self.rules:
            rules_item = rules_item_data.to_dict()
            rules.append(rules_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "operations": operations,
                "criteria": criteria,
                "rules": rules,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.field_condition_rule import FieldConditionRule

        d = dict(src_dict)
        operations = []
        _operations = d.pop("operations")
        for operations_item_data in _operations:
            operations_item = FieldConditionOperationsItem(operations_item_data)

            operations.append(operations_item)

        criteria = FieldConditionCriteria(d.pop("criteria"))

        rules = []
        _rules = d.pop("rules")
        for rules_item_data in _rules:
            rules_item = FieldConditionRule.from_dict(rules_item_data)

            rules.append(rules_item)

        field_condition = cls(
            operations=operations,
            criteria=criteria,
            rules=rules,
        )

        field_condition.additional_properties = d
        return field_condition

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
