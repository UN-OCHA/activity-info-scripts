from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Self, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.database_automation_trigger import DatabaseAutomationTrigger
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.database_automation_action import DatabaseAutomationAction


T = TypeVar("T", bound="DatabaseAutomation")


@_attrs_define
class DatabaseAutomation:
    """
    Attributes:
        automation_id (str): The automation's id
        label (str | Unset): A human-readable label for this automation
        resource_id (str | Unset): The resource (form, or subform) to which this automation applies.
        trigger (DatabaseAutomationTrigger | Unset): The trigger that applies to this automation. Values: RECORD_ADDED,
            RECORD_EDITED, RECORD_DELETED
        trigger_formula (str | Unset): The formula to which this automation applies. currently not supported
        action (DatabaseAutomationAction | Unset):
        active (bool | Unset): Determines whether this automation is active or not
    """

    automation_id: str
    label: str | Unset = UNSET
    resource_id: str | Unset = UNSET
    trigger: DatabaseAutomationTrigger | Unset = UNSET
    trigger_formula: str | Unset = UNSET
    action: DatabaseAutomationAction | Unset = UNSET
    active: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        automation_id = self.automation_id

        label = self.label

        resource_id = self.resource_id

        trigger: str | Unset = UNSET
        if not isinstance(self.trigger, Unset):
            trigger = self.trigger.value

        trigger_formula = self.trigger_formula

        action: dict[str, Any] | Unset = UNSET
        if not isinstance(self.action, Unset):
            action = self.action.to_dict()

        active = self.active

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "automationId": automation_id,
            }
        )
        if label is not UNSET:
            field_dict["label"] = label
        if resource_id is not UNSET:
            field_dict["resourceId"] = resource_id
        if trigger is not UNSET:
            field_dict["trigger"] = trigger
        if trigger_formula is not UNSET:
            field_dict["triggerFormula"] = trigger_formula
        if action is not UNSET:
            field_dict["action"] = action
        if active is not UNSET:
            field_dict["active"] = active

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        from ..models.database_automation_action import DatabaseAutomationAction

        d = dict(src_dict)
        automation_id = d.pop("automationId")

        label = d.pop("label", UNSET)

        resource_id = d.pop("resourceId", UNSET)

        _trigger = d.pop("trigger", UNSET)
        trigger: DatabaseAutomationTrigger | Unset
        if isinstance(_trigger, Unset):
            trigger = UNSET
        else:
            trigger = DatabaseAutomationTrigger(_trigger)

        trigger_formula = d.pop("triggerFormula", UNSET)

        _action = d.pop("action", UNSET)
        action: DatabaseAutomationAction | Unset
        if isinstance(_action, Unset):
            action = UNSET
        else:
            action = DatabaseAutomationAction.from_dict(_action)

        active = d.pop("active", UNSET)

        database_automation = cls(
            automation_id=automation_id,
            label=label,
            resource_id=resource_id,
            trigger=trigger,
            trigger_formula=trigger_formula,
            action=action,
            active=active,
        )

        database_automation.additional_properties = d
        return database_automation

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
