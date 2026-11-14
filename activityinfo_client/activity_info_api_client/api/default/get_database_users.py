from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.message_response import MessageResponse
from ...models.user import User
from ...types import Response


def _get_kwargs(
    database_id: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/databases/{database_id}/users".format(
            database_id=quote(str(database_id), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> MessageResponse | list[User] | None:
    if response.status_code == 200:
        response_200 = []
        _response_200 = response.json()
        for response_200_item_data in _response_200:
            response_200_item = User.from_dict(response_200_item_data)

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
) -> Response[MessageResponse | list[User]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    database_id: str,
    *,
    client: AuthenticatedClient | Client,
) -> Response[MessageResponse | list[User]]:
    """Get Database Users

     List all users who have access to the specified database.

    Args:
        database_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[MessageResponse | list[User]]
    """

    kwargs = _get_kwargs(
        database_id=database_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    database_id: str,
    *,
    client: AuthenticatedClient | Client,
) -> MessageResponse | list[User] | None:
    """Get Database Users

     List all users who have access to the specified database.

    Args:
        database_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        MessageResponse | list[User]
    """

    return sync_detailed(
        database_id=database_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    database_id: str,
    *,
    client: AuthenticatedClient | Client,
) -> Response[MessageResponse | list[User]]:
    """Get Database Users

     List all users who have access to the specified database.

    Args:
        database_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[MessageResponse | list[User]]
    """

    kwargs = _get_kwargs(
        database_id=database_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    database_id: str,
    *,
    client: AuthenticatedClient | Client,
) -> MessageResponse | list[User] | None:
    """Get Database Users

     List all users who have access to the specified database.

    Args:
        database_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        MessageResponse | list[User]
    """

    return (
        await asyncio_detailed(
            database_id=database_id,
            client=client,
        )
    ).parsed
