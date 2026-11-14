from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Self, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.job_request_descriptor import JobRequestDescriptor


T = TypeVar("T", bound="JobRequest")


@_attrs_define
class JobRequest:
    """
    Attributes:
        type_ (str | Unset): The type of the job to start.
        descriptor (JobRequestDescriptor | Unset): The object containing the elements required for the specific type of
            job
    """

    type_: str | Unset = UNSET
    descriptor: JobRequestDescriptor | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_

        descriptor: dict[str, Any] | Unset = UNSET
        if not isinstance(self.descriptor, Unset):
            descriptor = self.descriptor.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if type_ is not UNSET:
            field_dict["type"] = type_
        if descriptor is not UNSET:
            field_dict["descriptor"] = descriptor

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        from ..models.job_request_descriptor import JobRequestDescriptor

        d = dict(src_dict)
        type_ = d.pop("type", UNSET)

        _descriptor = d.pop("descriptor", UNSET)
        descriptor: JobRequestDescriptor | Unset
        if isinstance(_descriptor, Unset):
            descriptor = UNSET
        else:
            descriptor = JobRequestDescriptor.from_dict(_descriptor)

        job_request = cls(
            type_=type_,
            descriptor=descriptor,
        )

        job_request.additional_properties = d
        return job_request

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
