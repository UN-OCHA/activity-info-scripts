from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.job_status_state import JobStatusState
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.job_status_descriptor import JobStatusDescriptor
    from ..models.job_status_job_result import JobStatusJobResult
    from ..models.message_response import MessageResponse


T = TypeVar("T", bound="JobStatus")


@_attrs_define
class JobStatus:
    """
    Attributes:
        id (str | Unset): The job's id
        user_id (str | Unset): The id of the user who started the job
        descriptor (JobStatusDescriptor | Unset): The job descriptor used to start this job
        state (JobStatusState | Unset): The job's current state
        job_result (JobStatusJobResult | Unset):
        percent_complete (float | Unset):
        error (MessageResponse | Unset):
    """

    id: str | Unset = UNSET
    user_id: str | Unset = UNSET
    descriptor: JobStatusDescriptor | Unset = UNSET
    state: JobStatusState | Unset = UNSET
    job_result: JobStatusJobResult | Unset = UNSET
    percent_complete: float | Unset = UNSET
    error: MessageResponse | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        user_id = self.user_id

        descriptor: dict[str, Any] | Unset = UNSET
        if not isinstance(self.descriptor, Unset):
            descriptor = self.descriptor.to_dict()

        state: str | Unset = UNSET
        if not isinstance(self.state, Unset):
            state = self.state.value

        job_result: dict[str, Any] | Unset = UNSET
        if not isinstance(self.job_result, Unset):
            job_result = self.job_result.to_dict()

        percent_complete = self.percent_complete

        error: dict[str, Any] | Unset = UNSET
        if not isinstance(self.error, Unset):
            error = self.error.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if id is not UNSET:
            field_dict["id"] = id
        if user_id is not UNSET:
            field_dict["userId"] = user_id
        if descriptor is not UNSET:
            field_dict["descriptor"] = descriptor
        if state is not UNSET:
            field_dict["state"] = state
        if job_result is not UNSET:
            field_dict["jobResult"] = job_result
        if percent_complete is not UNSET:
            field_dict["percentComplete"] = percent_complete
        if error is not UNSET:
            field_dict["error"] = error

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.job_status_descriptor import JobStatusDescriptor
        from ..models.job_status_job_result import JobStatusJobResult
        from ..models.message_response import MessageResponse

        d = dict(src_dict)
        id = d.pop("id", UNSET)

        user_id = d.pop("userId", UNSET)

        _descriptor = d.pop("descriptor", UNSET)
        descriptor: JobStatusDescriptor | Unset
        if isinstance(_descriptor, Unset):
            descriptor = UNSET
        else:
            descriptor = JobStatusDescriptor.from_dict(_descriptor)

        _state = d.pop("state", UNSET)
        state: JobStatusState | Unset
        if isinstance(_state, Unset):
            state = UNSET
        else:
            state = JobStatusState(_state)

        _job_result = d.pop("jobResult", UNSET)
        job_result: JobStatusJobResult | Unset
        if isinstance(_job_result, Unset):
            job_result = UNSET
        else:
            job_result = JobStatusJobResult.from_dict(_job_result)

        percent_complete = d.pop("percentComplete", UNSET)

        _error = d.pop("error", UNSET)
        error: MessageResponse | Unset
        if isinstance(_error, Unset):
            error = UNSET
        else:
            error = MessageResponse.from_dict(_error)

        job_status = cls(
            id=id,
            user_id=user_id,
            descriptor=descriptor,
            state=state,
            job_result=job_result,
            percent_complete=percent_complete,
            error=error,
        )

        job_status.additional_properties = d
        return job_status

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
