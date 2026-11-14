from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.add_update_form_response import AddUpdateFormResponse
from ...models.form_schema import FormSchema
from ...models.message_response import MessageResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    form_id: str,
    *,
    body: FormSchema | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/form/{form_id}/schema".format(
            form_id=quote(str(form_id), safe=""),
        ),
    }

    if not isinstance(body, Unset):
        _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> AddUpdateFormResponse | MessageResponse | None:
    if response.status_code == 200:
        response_200 = AddUpdateFormResponse.from_dict(response.json())

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
) -> Response[AddUpdateFormResponse | MessageResponse]:
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
    body: FormSchema | Unset = UNSET,
) -> Response[AddUpdateFormResponse | MessageResponse]:
    """Update Form Schema

     Push a modified schema back to the server to update a form's design.

    Args:
        form_id (str):
        body (FormSchema | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[AddUpdateFormResponse | MessageResponse]
    """

    kwargs = _get_kwargs(
        form_id=form_id,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    form_id: str,
    *,
    client: AuthenticatedClient | Client,
    body: FormSchema | Unset = UNSET,
) -> AddUpdateFormResponse | MessageResponse | None:
    """Update Form Schema

     Push a modified schema back to the server to update a form's design.

    Args:
        form_id (str):
        body (FormSchema | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        AddUpdateFormResponse | MessageResponse
    """

    return sync_detailed(
        form_id=form_id,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    form_id: str,
    *,
    client: AuthenticatedClient | Client,
    body: FormSchema | Unset = UNSET,
) -> Response[AddUpdateFormResponse | MessageResponse]:
    """Update Form Schema

     Push a modified schema back to the server to update a form's design.

    Args:
        form_id (str):
        body (FormSchema | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[AddUpdateFormResponse | MessageResponse]
    """

    kwargs = _get_kwargs(
        form_id=form_id,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    form_id: str,
    *,
    client: AuthenticatedClient | Client,
    body: FormSchema | Unset = UNSET,
) -> AddUpdateFormResponse | MessageResponse | None:
    """Update Form Schema

     Push a modified schema back to the server to update a form's design.

    Args:
        form_id (str):
        body (FormSchema | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        AddUpdateFormResponse | MessageResponse
    """

    return (
        await asyncio_detailed(
            form_id=form_id,
            client=client,
            body=body,
        )
    ).parsed
