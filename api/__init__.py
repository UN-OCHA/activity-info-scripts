from .client import ActivityInfoHTTPClient
from .endpoints import ActivityInfoEndpoints


class ActivityInfoClient:
    """
    A high-level client for interacting with the ActivityInfo API.
    
    This class orchestrates the underlying HTTP client and provides access to 
    various API endpoints. It supports both manual initialization and 
    context manager usage (via 'with' statements) for automatic resource cleanup.
    
    Attributes:
        api (ActivityInfoEndpoints): An object containing methods for specific API calls.
    """
    def __init__(self, base_url: str, api_token: str | None = None):
        """
        Initialize the client with connection details.
        
        Args:
            base_url (str): The base endpoint for the ActivityInfo resource server.
            api_token (str, optional): The user's personal API token for authentication.
        """
        # Initialize the low-level HTTP client that handles authentication and requests
        self._http = ActivityInfoHTTPClient(
            base_url=base_url,
            api_token=api_token,
        )
        # Instantiate the endpoint collection, passing the HTTP client for execution
        self.api = ActivityInfoEndpoints(self._http)

    def close(self) -> None:
        """Close the underlying HTTP connection pool."""
        self._http.close()

    def __enter__(self) -> "ActivityInfoClient":
        """Support for context manager 'with' statement."""
        return self

    def __exit__(self, *exc) -> None:
        """Ensure resources are released when exiting the context."""
        self.close()
