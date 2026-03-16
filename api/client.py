import logging
import os
from time import sleep
from typing import Optional, Dict, Any

import httpx


# ---------- Custom API Exceptions ----------

class APIError(Exception):
    """Base exception for all ActivityInfo API related errors."""
    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code


class AuthenticationError(APIError):
    """Raised when the API returns a 401 Unauthorized response."""
    pass


class APITimeoutError(APIError):
    """Raised when an API request exceeds the configured timeout limits."""
    pass


# ---------- Constants and Logger Setup ----------

# Default base URL, can be overridden by environment variables
BASE_URL = os.getenv("ACTIVITYINFO_BASE_URL", "https://www.activityinfo.org/resources/")
DEFAULT_HEADERS = {
    "Accept": "application/json",
}

# Dedicated logger for full HTTP request/response debugging
logger = logging.getLogger("httpx_full")


def log_request(request: httpx.Request):
    """Callback to log details of an outgoing HTTP request."""
    body = request.content.decode() if request.content else None
    logger.info(f"Request: {request.method} {request.url}")
    logger.info(f"Request headers: {request.headers}")
    logger.info(f"Request body: {body}")


def log_response(response: httpx.Response):
    """Callback to log details of an incoming HTTP response."""
    request = response.request
    logger.info(f"Response: {request.method} {request.url} -> {response.status_code}")
    logger.info(f"Response headers: {response.headers}")

    # Read the response body safely and handle potential binary data
    content = response.read()
    try:
        logger.info(f"Response body: {content.decode('utf-8', errors='replace')}")
    except Exception:
        logger.info(f"Response body (binary): {content!r}")


class ActivityInfoHTTPClient:
    """
    A specialized HTTP client for the ActivityInfo API built on top of httpx.
    
    Handles authentication, request/response logging, automatic retries with 
    exponential backoff, and standard error parsing.
    """
    def __init__(
            self,
            base_url: str,
            *,
            api_token: Optional[str] = None,
            timeout=httpx.Timeout(
                connect=10.0,
                read=60.0,
                write=10.0,
                pool=60.0
            ),
    ):
        """
        Initialize the HTTP client.
        
        Args:
            base_url (str): The target API base URL.
            api_token (str, optional): Personal API token for Bearer authentication.
            timeout (httpx.Timeout): Custom timeout configuration for the client.
        """
        headers = dict(DEFAULT_HEADERS)
        if api_token:
            headers["Authorization"] = f"Bearer {api_token}"

        # Initialize logging handlers if they haven't been set up yet
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("%(message)s"))
            logger.addHandler(handler)

        # Initialize the underlying httpx client with event hooks for logging
        self._client = httpx.Client(
            base_url=base_url,
            headers=headers,
            timeout=timeout,
            event_hooks={
                "request": [log_request],
                "response": [log_response],
            }
        )

    def request(
            self,
            method: str,
            path: str,
            *,
            params: Dict[str, Any] | None = None,
            json: Dict[str, Any] | None = None,
            retries: int = 3,
    ) -> Any:
        """
        Execute an HTTP request with automatic retries and error handling.
        
        Args:
            method (str): HTTP method (GET, POST, etc.).
            path (str): Relative path from the base URL.
            params (dict, optional): Query string parameters.
            json (dict, optional): Request body to be sent as JSON.
            retries (int): Number of times to retry on timeout.
            
        Returns:
            The parsed JSON response body or None if the response is empty.
            
        Raises:
            AuthenticationError: If 401 is returned.
            APIError: For other 4xx/5xx errors or if the API returns an error code in the body.
            APITimeoutError: If all retry attempts time out.
        """
        for attempt in range(retries):
            try:
                response = self._client.request(
                    method,
                    path,
                    params=params,
                    json=json,
                )

                # Handle authentication failures immediately
                if response.status_code == 401:
                    raise AuthenticationError("Invalid API key")

                # Raise generic API errors for other 400+ status codes
                if response.status_code >= 400:
                    raise APIError(
                        f"Error requesting {method} {response.url}: {response.status_code}: {response.text}",
                        status_code=response.status_code
                    )

                # Return None if there is no content (e.g., successful DELETE)
                if not response.content:
                    return None

                # Verify that we received JSON before attempting to parse it
                content_type = response.headers.get("content-type", "")
                if "application/json" not in content_type.lower():
                    return None

                json_data = response.json()
                
                # Check if the JSON body itself contains an error message (common in ActivityInfo)
                if isinstance(json_data, dict) and json_data.get("code") in ["BAD_REQUEST", "UNAUTHORIZED", "FORBIDDEN",
                                                                             "NOT_FOUND", "INTERNAL_ERROR"]:
                    raise APIError(
                        f"API returned error code {json_data.get('code')}: {json_data.get('message') or json_data.get('localizedMessage') or 'No message'}",
                        status_code=response.status_code
                    )

                return json_data

            except httpx.TimeoutException as exc:
                # Retry on timeout with exponential backoff
                if attempt == retries - 1:
                    raise APITimeoutError("Request timed out") from exc
                sleep(2 ** attempt)
        
        return None

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self._client.close()

    def __enter__(self) -> "ActivityInfoHTTPClient":
        """Support for context manager 'with' statement."""
        return self

    def __exit__(self, *exc) -> None:
        """Ensure the client is closed when exiting the context."""
        self.close()
