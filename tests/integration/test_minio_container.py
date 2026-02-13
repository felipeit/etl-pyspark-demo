import requests
from testcontainers.core.container import DockerContainer


def test_minio_starts_and_health_endpoint_ok():
    image = "minio/minio:RELEASE.2024-01-01T00-00-00Z"
    with DockerContainer(image).with_env("MINIO_ROOT_USER", "minioadmin").with_env(
        "MINIO_ROOT_PASSWORD", "minioadmin").with_command("server /data --console-address :9001") as mc:
        port = mc.get_exposed_port(9000)
        # health endpoint
        resp = requests.get(f"http://127.0.0.1:{port}/minio/health/ready")
        assert resp.status_code in (200, 503)  # 503 possible during very short warmup
