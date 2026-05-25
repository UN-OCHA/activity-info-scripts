import asyncio
import os
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
from typing import Awaitable, Dict, Any, Callable, TypeVar

import typer
from dotenv import load_dotenv
from rich.console import Console

from activityinfo.client import Configuration, ApiClient, DefaultApi

# Load environment variables from a .env file if it exists
load_dotenv()

# Initialize a rich Console for stylized CLI output
console = Console()

# Clients created via get_client() while run_cli_async is active are closed on exit.
_pending_api_clients: ContextVar[list[DefaultApi] | None] = ContextVar(
    "_pending_api_clients", default=None
)

T = TypeVar("T")


def get_client() -> DefaultApi:
    configuration = Configuration(
        host=os.getenv("ACTIVITYINFO_BASE_URL", "https://www.activityinfo.org/resources/"),
        access_token=os.getenv("API_TOKEN")
    )
    api_client = ApiClient(configuration)
    client = DefaultApi(api_client)
    pending = _pending_api_clients.get()
    if pending is not None:
        pending.append(client)
    return client


async def _close_pending_clients(clients: list[DefaultApi]) -> None:
    for client in clients:
        await client.api_client.close()


async def run_cli_async(coro: Awaitable[T]) -> T:
    """
    Run a coroutine for a one-shot CLI command and close any ApiClients
    created via get_client() during the run.
    """
    clients: list[DefaultApi] = []
    token = _pending_api_clients.set(clients)
    try:
        return await coro
    finally:
        _pending_api_clients.reset(token)
        await _close_pending_clients(clients)


def run_cli(coro: Awaitable[T]) -> T:
    """Sync entry point for Typer commands: asyncio.run + client cleanup."""
    return asyncio.run(run_cli_async(coro))


@contextmanager
def handle_api_errors(message: str):
    """
    Context manager to handle API errors and display them nicely in the console.
    """
    try:
        yield
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {message}: {e}")
        # In a CLI tool, we usually want to exit on fatal API errors
        raise typer.Exit(code=1)


def build_nested_dict(flat_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Converts a flat dictionary with dot-separated keys into a nested dictionary.
    Example: {'a.b': 1, 'a.c': 2, 'd': 3} -> {'a': {'b': 1, 'c': 2}, 'd': 3}
    """
    nested: Dict[str, Any] = {}
    for key, value in flat_dict.items():
        parts = key.split(".")
        curr = nested
        for part in parts[:-1]:
            if part not in curr:
                curr[part] = {}
            curr = curr[part]
        curr[parts[-1]] = value
    return nested


def wrap_async(f: Callable[..., Awaitable[T]]) -> Callable[..., T]:
    @wraps(f)
    def wrapper(*args, **kwargs):
        return run_cli(f(*args, **kwargs))

    return wrapper
