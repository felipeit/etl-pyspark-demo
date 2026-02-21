import pytest

from tests.integration._container_utils import require_docker, wait_for_exposed_port, wait_for_http_ready


def test_testcontainers_can_start_nginx():
    require_docker()
    DockerContainer = pytest.importorskip("testcontainers.core.container").DockerContainer

    with DockerContainer("nginx:stable").with_exposed_ports(80) as nginx:
        host_port = wait_for_exposed_port(nginx, 80)
        status = wait_for_http_ready(f"http://127.0.0.1:{host_port}")
        assert status == 200
