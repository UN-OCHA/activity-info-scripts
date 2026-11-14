from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.message_response import MessageResponse
from ...models.update_user_request import UpdateUserRequest
from ...models.user import User
from ...types import UNSET, Response, Unset


def _get_kwargs(
    database_id: str,
    user_id: str,
    *,
    body: UpdateUserRequest | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/databases/{database_id}/users/{user_id}/role".format(
            database_id=quote(str(database_id), safe=""),
            user_id=quote(str(user_id), safe=""),
        ),
    }

    if not isinstance(body, Unset):
        _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> MessageResponse | User | None:
    if response.status_code == 200:
        response_200 = User.from_dict(response.json())

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
) -> Response[MessageResponse | User]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    database_id: str,
    user_id: str,
    *,
    client: AuthenticatedClient | Client,
    body: UpdateUserRequest | Unset = UNSET,
) -> Response[MessageResponse | User]:
    """Update Database User Role

     Modify the assigned role for an existing database user.

    Args:
        database_id (str):
        user_id (str):
        body (UpdateUserRequest | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[MessageResponse | User]
    """

    kwargs = _get_kwargs(
        database_id=database_id,
        user_id=user_id,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    database_id: str,
    user_id: str,
    *,
    client: AuthenticatedClient | Client,
    body: UpdateUserRequest | Unset = UNSET,
) -> MessageResponse | User | None:
    """Update Database User Role

     Modify the assigned role for an existing database user.

    Args:
        database_id (str):
        user_id (str):
        body (UpdateUserRequest | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        MessageResponse | User
    """

    return sync_detailed(
        database_id=database_id,
        user_id=user_id,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    database_id: str,
    user_id: str,
    *,
    client: AuthenticatedClient | Client,
    body: UpdateUserRequest | Unset = UNSET,
) -> Response[MessageResponse | User]:
    """Update Database User Role

     Modify the assigned role for an existing database user.

    Args:
        database_id (str):
        user_id (str):
        body (UpdateUserRequest | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[MessageResponse | User]
    """

    kwargs = _get_kwargs(
        database_id=database_id,
        user_id=user_id,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    database_id: str,
    user_id: str,
    *,
    client: AuthenticatedClient | Client,
    body: UpdateUserRequest | Unset = UNSET,
) -> MessageResponse | User | None:
    """Update Database User Role

     Modify the assigned role for an existing database user.

    Args:
        database_id (str):
        user_id (str):
        body (UpdateUserRequest | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        MessageResponse | User
    """

    return (
        await asyncio_detailed(
            database_id=database_id,
            user_id=user_id,
            client=client,
            body=body,
        )
    ).parsed
