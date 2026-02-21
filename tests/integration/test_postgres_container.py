import pytest

from tests.integration._container_utils import require_docker


def test_postgres_container_runs_and_accepts_connections():
    require_docker()
    sa = pytest.importorskip("sqlalchemy")
    PostgresContainer = pytest.importorskip("testcontainers.postgres").PostgresContainer
    pytest.importorskip("psycopg2")

    with PostgresContainer("postgres:15-alpine") as postgres:
        engine = sa.create_engine(postgres.get_connection_url())
        with engine.connect() as conn:
            result = conn.execute(sa.text("SELECT 1 + 1"))
            assert result.scalar() == 2
