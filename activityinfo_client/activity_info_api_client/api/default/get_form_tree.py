from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.form_tree import FormTree
from ...models.message_response import MessageResponse
from ...types import Response


def _get_kwargs(
    form_id: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/form/{form_id}/tree".format(
            form_id=quote(str(form_id), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> FormTree | MessageResponse | None:
    if response.status_code == 200:
        response_200 = FormTree.from_dict(response.json())

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
) -> Response[FormTree | MessageResponse]:
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
) -> Response[FormTree | MessageResponse]:
    """Get Form Tree

     Fetch the form and all its related forms (references).

    Args:
        form_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[FormTree | MessageResponse]
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
) -> FormTree | MessageResponse | None:
    """Get Form Tree

     Fetch the form and all its related forms (references).

    Args:
        form_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        FormTree | MessageResponse
    """

    return sync_detailed(
        form_id=form_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    form_id: str,
    *,
    client: AuthenticatedClient | Client,
) -> Response[FormTree | MessageResponse]:
    """Get Form Tree

     Fetch the form and all its related forms (references).

    Args:
        form_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[FormTree | MessageResponse]
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
) -> FormTree | MessageResponse | None:
    """Get Form Tree

     Fetch the form and all its related forms (references).

    Args:
        form_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        FormTree | MessageResponse
    """

    return (
        await asyncio_detailed(
            form_id=form_id,
            client=client,
        )
    ).parsed
