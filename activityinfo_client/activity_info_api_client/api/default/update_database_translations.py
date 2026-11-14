from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.message_response import MessageResponse
from ...models.update_translations_request import UpdateTranslationsRequest
from ...types import UNSET, Response, Unset


def _get_kwargs(
    database_id: str,
    language_code: str,
    *,
    body: UpdateTranslationsRequest | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/databases/{database_id}/translations/{language_code}".format(
            database_id=quote(str(database_id), safe=""),
            language_code=quote(str(language_code), safe=""),
        ),
    }

    if not isinstance(body, Unset):
        _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> MessageResponse | None:
    if response.status_code == 200:
        response_200 = MessageResponse.from_dict(response.json())

        return response_200

    if response.status_code == 401:
        response_401 = MessageResponse.from_dict(response.json())

        return response_401

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Response[MessageResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    database_id: str,
    language_code: str,
    *,
    client: AuthenticatedClient | Client,
    body: UpdateTranslationsRequest | Unset = UNSET,
) -> Response[MessageResponse]:
    """Update Database Translations

     Update global translations for a specific database.

    Args:
        database_id (str):
        language_code (str):
        body (UpdateTranslationsRequest | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[MessageResponse]
    """

    kwargs = _get_kwargs(
        database_id=database_id,
        language_code=language_code,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    database_id: str,
    language_code: str,
    *,
    client: AuthenticatedClient | Client,
    body: UpdateTranslationsRequest | Unset = UNSET,
) -> MessageResponse | None:
    """Update Database Translations

     Update global translations for a specific database.

    Args:
        database_id (str):
        language_code (str):
        body (UpdateTranslationsRequest | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        MessageResponse
    """

    return sync_detailed(
        database_id=database_id,
        language_code=language_code,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    database_id: str,
    language_code: str,
    *,
    client: AuthenticatedClient | Client,
    body: UpdateTranslationsRequest | Unset = UNSET,
) -> Response[MessageResponse]:
    """Update Database Translations

     Update global translations for a specific database.

    Args:
        database_id (str):
        language_code (str):
        body (UpdateTranslationsRequest | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[MessageResponse]
    """

    kwargs = _get_kwargs(
        database_id=database_id,
        language_code=language_code,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    database_id: str,
    language_code: str,
    *,
    client: AuthenticatedClient | Client,
    body: UpdateTranslationsRequest | Unset = UNSET,
) -> MessageResponse | None:
    """Update Database Translations

     Update global translations for a specific database.

    Args:
        database_id (str):
        language_code (str):
        body (UpdateTranslationsRequest | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        MessageResponse
    """

    return (
        await asyncio_detailed(
            database_id=database_id,
            language_code=language_code,
            client=client,
            body=body,
        )
    ).parsed
