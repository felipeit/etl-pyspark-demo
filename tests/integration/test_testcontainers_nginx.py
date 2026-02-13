import requests

from testcontainers.core.container import DockerContainer


def test_testcontainers_can_start_nginx():
    with DockerContainer("nginx:stable") as nginx:
        host_port = nginx.get_exposed_port(80)
        resp = requests.get(f"http://127.0.0.1:{host_port}")
        assert resp.status_code == 200
