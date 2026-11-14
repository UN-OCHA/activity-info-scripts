from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.get_form_records_response_200_item import GetFormRecordsResponse200Item
from ...models.message_response import MessageResponse
from ...types import Response


def _get_kwargs(
    form_id: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/form/{form_id}/query".format(
            form_id=quote(str(form_id), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> MessageResponse | list[GetFormRecordsResponse200Item] | None:
    if response.status_code == 200:
        response_200 = []
        _response_200 = response.json()
        for response_200_item_data in _response_200:
            response_200_item = GetFormRecordsResponse200Item.from_dict(response_200_item_data)

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
) -> Response[MessageResponse | list[GetFormRecordsResponse200Item]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    form_id: str,
    *,
    client: AuthenticatedClient | Client,
) -> Response[MessageResponse | list[GetFormRecordsResponse200Item]]:
    """Get Form Records

     Query and retrieve all records for a specific form.

    Args:
        form_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[MessageResponse | list[GetFormRecordsResponse200Item]]
    """

    kwargs = _get_kwargs(
        form_id=form_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    form_id: str,
    *,
    client: AuthenticatedClient | Client,
) -> MessageResponse | list[GetFormRecordsResponse200Item] | None:
    """Get Form Records

     Query and retrieve all records for a specific form.

    Args:
        form_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        MessageResponse | list[GetFormRecordsResponse200Item]
    """

    return sync_detailed(
        form_id=form_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    form_id: str,
    *,
    client: AuthenticatedClient | Client,
) -> Response[MessageResponse | list[GetFormRecordsResponse200Item]]:
    """Get Form Records

     Query and retrieve all records for a specific form.

    Args:
        form_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[MessageResponse | list[GetFormRecordsResponse200Item]]
    """

    kwargs = _get_kwargs(
        form_id=form_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    form_id: str,
    *,
    client: AuthenticatedClient | Client,
) -> MessageResponse | list[GetFormRecordsResponse200Item] | None:
    """Get Form Records

     Query and retrieve all records for a specific form.

    Args:
        form_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        MessageResponse | list[GetFormRecordsResponse200Item]
    """

    return (
        await asyncio_detailed(
            form_id=form_id,
            client=client,
        )
    ).parsed
