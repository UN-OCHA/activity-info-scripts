from .client import ActivityInfoHTTPClient
from .endpoints import ActivityInfoEndpoints


class ActivityInfoClient:
    def __init__(self, base_url: str, api_token: str | None = None):
        self._http = ActivityInfoHTTPClient(
            base_url=base_url,
            api_token=api_token,
        )
        self.api = ActivityInfoEndpoints(self._http)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "ActivityInfoClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
