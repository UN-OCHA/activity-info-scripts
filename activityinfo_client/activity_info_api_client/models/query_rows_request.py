from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Self, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.query_column import QueryColumn
    from ..models.query_filter_set import QueryFilterSet
    from ..models.query_rows_request_validation import QueryRowsRequestValidation
    from ..models.query_sort import QuerySort
    from ..models.query_source import QuerySource


T = TypeVar("T", bound="QueryRowsRequest")


@_attrs_define
class QueryRowsRequest:
    """
    Attributes:
        row_sources (list[QuerySource]): Exactly one RowSource is required.
        columns (list[QueryColumn]): The list of columns to retrieve
        form_id (str | Unset): CUID of the root form to query. This is deprecated in favour of rowSources
        filter_ (str | Unset): A boolean-valued ActivityInfo formula to filter the records included in this query.
            Resulting rows must match both the filter formula, if provided, and all the provided filterSets. Filter sets are
            more efficient than formulas when you have a large number of values.
        filter_sets (list[QueryFilterSet] | Unset): Set-based filters to apply to records returned by the query, each of
            which is a pair of a formula and a set of values to include. Resulting rows must match both the filter formula,
            if provided, and all the provided filterSets. Filter sets are more efficient than formulas when you have a large
            number of values.
        sort (list[QuerySort] | Unset): Sort instructions specified with a formula and a direction. Only one sort
            instruction can be specified, so sorting on multiple columns must be done with a single formula expression.
        truncate_strings (bool | Unset):
        validation (QueryRowsRequestValidation | Unset):
        drafts (str | Unset):
        tags (list[str] | Unset):
    """

    row_sources: list[QuerySource]
    columns: list[QueryColumn]
    form_id: str | Unset = UNSET
    filter_: str | Unset = UNSET
    filter_sets: list[QueryFilterSet] | Unset = UNSET
    sort: list[QuerySort] | Unset = UNSET
    truncate_strings: bool | Unset = UNSET
    validation: QueryRowsRequestValidation | Unset = UNSET
    drafts: str | Unset = UNSET
    tags: list[str] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        row_sources = []
        for row_sources_item_data in self.row_sources:
            row_sources_item = row_sources_item_data.to_dict()
            row_sources.append(row_sources_item)

        columns = []
        for columns_item_data in self.columns:
            columns_item = columns_item_data.to_dict()
            columns.append(columns_item)

        form_id = self.form_id

        filter_ = self.filter_

        filter_sets: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.filter_sets, Unset):
            filter_sets = []
            for filter_sets_item_data in self.filter_sets:
                filter_sets_item = filter_sets_item_data.to_dict()
                filter_sets.append(filter_sets_item)

        sort: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.sort, Unset):
            sort = []
            for sort_item_data in self.sort:
                sort_item = sort_item_data.to_dict()
                sort.append(sort_item)

        truncate_strings = self.truncate_strings

        validation: dict[str, Any] | Unset = UNSET
        if not isinstance(self.validation, Unset):
            validation = self.validation.to_dict()

        drafts = self.drafts

        tags: list[str] | Unset = UNSET
        if not isinstance(self.tags, Unset):
            tags = self.tags

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "rowSources": row_sources,
                "columns": columns,
            }
        )
        if form_id is not UNSET:
            field_dict["formId"] = form_id
        if filter_ is not UNSET:
            field_dict["filter"] = filter_
        if filter_sets is not UNSET:
            field_dict["filterSets"] = filter_sets
        if sort is not UNSET:
            field_dict["sort"] = sort
        if truncate_strings is not UNSET:
            field_dict["truncateStrings"] = truncate_strings
        if validation is not UNSET:
            field_dict["validation"] = validation
        if drafts is not UNSET:
            field_dict["drafts"] = drafts
        if tags is not UNSET:
            field_dict["tags"] = tags

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        from ..models.query_column import QueryColumn
        from ..models.query_filter_set import QueryFilterSet
        from ..models.query_rows_request_validation import QueryRowsRequestValidation
        from ..models.query_sort import QuerySort
        from ..models.query_source import QuerySource

        d = dict(src_dict)
        row_sources = []
        _row_sources = d.pop("rowSources")
        for row_sources_item_data in _row_sources:
            row_sources_item = QuerySource.from_dict(row_sources_item_data)

            row_sources.append(row_sources_item)

        columns = []
        _columns = d.pop("columns")
        for columns_item_data in _columns:
            columns_item = QueryColumn.from_dict(columns_item_data)

            columns.append(columns_item)

        form_id = d.pop("formId", UNSET)

        filter_ = d.pop("filter", UNSET)

        _filter_sets = d.pop("filterSets", UNSET)
        filter_sets: list[QueryFilterSet] | Unset = UNSET
        if _filter_sets is not UNSET:
            filter_sets = []
            for filter_sets_item_data in _filter_sets:
                filter_sets_item = QueryFilterSet.from_dict(filter_sets_item_data)

                filter_sets.append(filter_sets_item)

        _sort = d.pop("sort", UNSET)
        sort: list[QuerySort] | Unset = UNSET
        if _sort is not UNSET:
            sort = []
            for sort_item_data in _sort:
                sort_item = QuerySort.from_dict(sort_item_data)

                sort.append(sort_item)

        truncate_strings = d.pop("truncateStrings", UNSET)

        _validation = d.pop("validation", UNSET)
        validation: QueryRowsRequestValidation | Unset
        if isinstance(_validation, Unset):
            validation = UNSET
        else:
            validation = QueryRowsRequestValidation.from_dict(_validation)

        drafts = d.pop("drafts", UNSET)

        tags = cast(list[str], d.pop("tags", UNSET))

        query_rows_request = cls(
            row_sources=row_sources,
            columns=columns,
            form_id=form_id,
            filter_=filter_,
            filter_sets=filter_sets,
            sort=sort,
            truncate_strings=truncate_strings,
            validation=validation,
            drafts=drafts,
            tags=tags,
        )

        query_rows_request.additional_properties = d
        return query_rows_request

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
