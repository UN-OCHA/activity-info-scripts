import time

import httpx
import pytest
from testcontainers.core.container import DockerContainer  # type: ignore

from api import ActivityInfoClient


def bootstrap_admin(base_url):
    admin_data = {
        "name": "Cyrus",
        "email": "cyrus.pellet@un.org",
        "locale": "en",
        "password": "test12345678",
        "password2": "test12345678"
    }

    # We use a single client to persist the session cookie from the init step
    with httpx.Client(base_url=base_url, follow_redirects=True) as client:
        # 1. Initialize the Admin
        print("👤 Bootstrapping admin...")
        init_resp = client.post("/init/admin", data=admin_data)
        init_resp.raise_for_status()

        # 2. Generate the Token
        # Based on your curl, the path is /resources/accounts/tokens/generate
        print("🔑 Requesting Personal Access Token...")
        token_payload = {
            "label": "TestHarness",
            "scope": "READ_WRITE"
        }

        # We don't need all the browser headers, but keeping Content-Type is vital
        token_resp = client.post(
            "/resources/accounts/tokens/generate",
            json=token_payload
        )

        if token_resp.status_code != 200:
            print(f"❌ Failed! Status: {token_resp.status_code}")
            print(f"Body: {token_resp.text}")
            token_resp.raise_for_status()

        # Extract the token from the response
        # Note: Check if the response is {"token": "..."} or just a string
        data = token_resp.json()
        return data.get("token") or data.get("key")  # Adjust based on actual JSON key


def wait_for_ready(url, timeout=60):
    """
    Polls the ActivityInfo login page.
    Catches connection resets and timeouts during the JVM startup.
    """
    start_time = time.time()
    print(f"\n⏳ Waiting for ActivityInfo to initialize at {url}...")

    while time.time() - start_time < timeout:
        try:
            # Using a short timeout for the individual check
            with httpx.Client(follow_redirects=True, timeout=2.0) as client:
                response = client.get(url)
                if response.status_code == 200:
                    print("✅ Server is responsive.")
                    return True
        except (httpx.ConnectError, httpx.ReadError, httpx.RemoteProtocolError):
            # Errno 54 (Connection Reset) or ConnectError usually means
            # the port is open but the app isn't ready.
            pass

        time.sleep(2)

    pytest.fail(f"ActivityInfo failed to start at {url} within {timeout}s")


@pytest.fixture(scope="session")
def ai_setup():
    # Use 8080 as it's the standard internal port for the AI container
    container = DockerContainer("activityinfo/activityinfo:latest")
    container.with_exposed_ports(8081)

    with container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(8081)
        base_url = f"http://{host}:{port}"

        # Wait for the login page to be reachable
        wait_for_ready(f"{base_url}/login")

        # Bootstrap and get the real token
        token = bootstrap_admin(base_url)

        yield {"url": base_url, "token": token}


@pytest.fixture
def api_client(ai_setup):
    # Now we pass the real URL and the real Token generated during bootstrap
    return ActivityInfoClient(f"{ai_setup['url']}/resources/", ai_setup["token"])
