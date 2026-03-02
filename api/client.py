import logging
import os
from time import sleep
from typing import Optional, Dict, Any

import httpx


# ---------- Exceptions ----------

class APIError(Exception):
    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code


class AuthenticationError(APIError):
    pass


class APITimeoutError(APIError):
    pass


# ---------- Client ----------

BASE_URL = os.getenv("ACTIVITYINFO_BASE_URL", "https://www.activityinfo.org/resources/")
DEFAULT_HEADERS = {
    "Accept": "application/json",
}

logger = logging.getLogger("httpx_full")


def log_request(request: httpx.Request):
    body = request.content.decode() if request.content else None
    logger.info(f"Request: {request.method} {request.url}")
    logger.info(f"Request headers: {request.headers}")
    logger.info(f"Request body: {body}")


def log_response(response: httpx.Response):
    request = response.request
    logger.info(f"Response: {request.method} {request.url} -> {response.status_code}")
    logger.info(f"Response headers: {response.headers}")

    # This reads the body safely
    content = response.read()
    try:
        logger.info(f"Response body: {content.decode('utf-8', errors='replace')}")
    except Exception:
        logger.info(f"Response body (binary): {content!r}")


class ActivityInfoHTTPClient:
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
        headers = dict(DEFAULT_HEADERS)
        if api_token:
            headers["Authorization"] = f"Bearer {api_token}"

        # Force logging for tests
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("%(message)s"))
            logger.addHandler(handler)

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
        for attempt in range(retries):
            try:
                response = self._client.request(
                    method,
                    path,
                    params=params,
                    json=json,
                )

                if response.status_code == 401:
                    raise AuthenticationError("Invalid API key")

                if response.status_code >= 400:
                    raise APIError(
                        f"Error requesting {method} {response.url}: {response.status_code}: {response.text}",
                        status_code=response.status_code
                    )

                if not response.content:
                    return None

                content_type = response.headers.get("content-type", "")
                if "application/json" not in content_type.lower():
                    return None

                json_data = response.json()
                if isinstance(json_data, dict) and json_data.get("code") in ["BAD_REQUEST", "UNAUTHORIZED", "FORBIDDEN",
                                                                             "NOT_FOUND", "INTERNAL_ERROR"]:
                    raise APIError(
                        f"API returned error code {json_data.get('code')}: {json_data.get('message') or json_data.get('localizedMessage') or 'No message'}",
                        status_code=response.status_code
                    )

                return json_data

            except httpx.TimeoutException as exc:
                if attempt == retries - 1:
                    raise APITimeoutError("Request timed out") from exc
                sleep(2 ** attempt)
        return None

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "ActivityInfoHTTPClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
