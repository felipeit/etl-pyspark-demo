import time

import pytest


def require_docker() -> None:
    try:
        import docker

        docker.from_env().ping()
    except Exception as exc:  # pragma: no cover - runtime environment check
        pytest.skip(f"Docker engine unavailable: {exc}")


def wait_for_http_ready(url: str, timeout_s: float = 20.0, accepted: tuple[int, ...] = (200,)) -> int:
    import requests

    deadline = time.time() + timeout_s
    last_error = None
    while time.time() < deadline:
        try:
            response = requests.get(url, timeout=2)
            if response.status_code in accepted:
                return response.status_code
        except Exception as exc:  # pragma: no cover - transient startup checks
            last_error = exc
        time.sleep(0.5)

    if last_error:
        raise AssertionError(f"Service did not become ready at {url}: {last_error}")
    raise AssertionError(f"Service did not become ready at {url}")


def wait_for_exposed_port(container, internal_port: int, timeout_s: float = 20.0) -> str:
    deadline = time.time() + timeout_s
    last_error = None
    while time.time() < deadline:
        try:
            port = container.get_exposed_port(internal_port)
            if port:
                return str(port)
        except Exception as exc:  # pragma: no cover - transient startup checks
            last_error = exc
        time.sleep(0.5)

    if last_error:
        raise AssertionError(f"Port mapping not available for {internal_port}: {last_error}")
    raise AssertionError(f"Port mapping not available for {internal_port}")
