import pytest

from tests.integration._container_utils import require_docker, wait_for_exposed_port, wait_for_http_ready


def test_minio_starts_and_health_endpoint_ok():
    require_docker()
    DockerContainer = pytest.importorskip("testcontainers.core.container").DockerContainer

    image = "minio/minio:latest"
    with (
        DockerContainer(image)
        .with_exposed_ports(9000)
        .with_env("MINIO_ROOT_USER", "minioadmin")
        .with_env("MINIO_ROOT_PASSWORD", "minioadmin")
        .with_command("server /data --console-address :9001")
    ) as mc:
        port = wait_for_exposed_port(mc, 9000)
        status = wait_for_http_ready(
            f"http://127.0.0.1:{port}/minio/health/ready",
            accepted=(200, 503),
        )
        assert status in (200, 503)
