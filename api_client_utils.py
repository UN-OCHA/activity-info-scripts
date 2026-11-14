import os
import traceback
from typing import Any, Callable, TypeVar
from http import HTTPStatus
from dotenv import load_dotenv

import typer
import httpx

from rich.console import Console

from activityinfo_client.activity_info_api_client import Client, errors
from activityinfo_client.activity_info_api_client.types import Response

# Load environment variables from a .env file if it exists
load_dotenv()

# Initialize a rich Console for stylized CLI output
console = Console()

T = TypeVar("T")

def get_client() -> Client:
    token = os.getenv("API_TOKEN")
    base_url = os.getenv("ACTIVITYINFO_BASE_URL", "https://www.activityinfo.org/resources/")
    return Client(
        base_url=base_url,
        headers={"Authorization": f"Bearer {token}"},
        raise_on_unexpected_status=True
    )




def format_call(
    func: Callable[..., Any],
    args: tuple[object, ...],
    kwargs: dict[str, object],
) -> str:
    """
    Return a string representation of a function call and its arguments.
    All arguments that are classes show <ClassName> instead of value.

    Returns:
        (str)
    """
    func_name = f"{func.__module__}.{func.__name__}"

    def safe(v):
        # simple primitives → show actual value
        if isinstance(v, (str, int, float, bool, type(None))):
            return repr(v)

        # everything else → show class name
        return f"<{v.__class__.__name__}>"

    args_str = ", ".join(safe(a) for a in args)

    kwargs_str = ", ".join(
        f"{k}={safe(v)}"
        for k, v in kwargs.items()
    )

    all_args = ", ".join(filter(None, [args_str, kwargs_str]))

    return f"{func_name}({all_args})"


def call_api(
        func: Callable[..., Response[T]],
        *args: object,
        message: str = "API request failed",
        expected_statuses: set[int] | None = None,
        exit_on_error: bool = True,
        **kwargs: object,
) -> tuple[T | None, str | None]:

    """
    Call an API function in /activityinfo_client with full error handling.

    Returns:
        (result, error_message)
    """

    if expected_statuses is None:
        expected_statuses = {200, 201, 202, 204}

    function_string = format_call(func, args, kwargs)
    error_header = (
        f"[bold red]API request error:[/bold red] [red]{message}[/red]\n" +
        f"[red]function: {function_string}[/red]"
    )

    try:
        # Step 1: perform API request
        response = func(*args, **kwargs)

        # Step 2: check HTTP status code
        status_code = int(response.status_code)

        if status_code in expected_statuses:
            return response.parsed, None

        # Step 3: handle documented errors (e.g. 400, 401)
        error_msg = (
            f"error type: Documented API error\n" +
            f"status code: {status_code}\n"
        )
        if status_code in HTTPStatus._value2member_map_:
            error_msg += f"status description: {HTTPStatus(status_code).phrase}\n"
        if response.parsed is not None:
            error_msg += "response: " + str(response.parsed) + "\n"
        elif response.content:
            error_msg += "response: " + response.content.decode(errors='replace') + "\n"

    except errors.UnexpectedStatus as e:
        # Step 4: undocumented HTTP status
        error_msg = f"error type: Unexpected API status\n"
        if hasattr(e, "status_code"):
            error_msg += f"status code: {e.status_code}\n"
        if hasattr(e, "content") and e.content:
            error_msg += "content: " + e.content.decode(errors='replace') + "\n"

    except httpx.TimeoutException as e:
        error_msg = (
            f"error type: API Timeout\n" +
            f"message: {str(e)}\n"
        )

    except httpx.HTTPError as e:
        error_msg = (
            f"error type: HTTP Error\n" +
            f"message: {str(e)}\n"
        )

    except Exception as e:
        error_msg = (
            f"error type: Unexpected Error\n" +
            f"message: {str(e)}\n"
        )
        if os.getenv("API_DEBUG_TRACEBACK", "").lower() in {"1", "true", "yes"}:
            error_msg += "traceback:\n" + traceback.format_exc()

    console.print(error_header)
    console.print(f"[red]{error_msg}[/red]")

    if exit_on_error:
        raise typer.Exit(code=1)

    return None, error_msg
