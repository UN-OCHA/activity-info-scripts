from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.message_response import MessageResponse
from ...models.query_rows_request import QueryRowsRequest
from ...models.query_rows_response_200_item import QueryRowsResponse200Item
from ...types import Response


def _get_kwargs(
    *,
    body: QueryRowsRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/query/rows",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> MessageResponse | list[QueryRowsResponse200Item] | None:
    if response.status_code == 200:
        response_200 = []
        _response_200 = response.json()
        for response_200_item_data in _response_200:
            response_200_item = QueryRowsResponse200Item.from_dict(response_200_item_data)

            response_200.append(response_200_item)

        return response_200

    if response.status_code == 401:
        response_401 = MessageResponse.from_dict(response.json())

        return response_401

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[MessageResponse | list[QueryRowsResponse200Item]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: QueryRowsRequest,
) -> Response[MessageResponse | list[QueryRowsResponse200Item]]:
    """Query Rows

     Queries records as rows using column expressions, optional filter formula, filter sets, and sorting.

    Args:
        body (QueryRowsRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[MessageResponse | list[QueryRowsResponse200Item]]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
    body: QueryRowsRequest,
) -> MessageResponse | list[QueryRowsResponse200Item] | None:
    """Query Rows

     Queries records as rows using column expressions, optional filter formula, filter sets, and sorting.

    Args:
        body (QueryRowsRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        MessageResponse | list[QueryRowsResponse200Item]
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: QueryRowsRequest,
) -> Response[MessageResponse | list[QueryRowsResponse200Item]]:
    """Query Rows

     Queries records as rows using column expressions, optional filter formula, filter sets, and sorting.

    Args:
        body (QueryRowsRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[MessageResponse | list[QueryRowsResponse200Item]]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: QueryRowsRequest,
) -> MessageResponse | list[QueryRowsResponse200Item] | None:
    """Query Rows

     Queries records as rows using column expressions, optional filter formula, filter sets, and sorting.

    Args:
        body (QueryRowsRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        MessageResponse | list[QueryRowsResponse200Item]
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
