import sqlalchemy as sa
from testcontainers.postgres import PostgresContainer


def test_postgres_container_runs_and_accepts_connections():
    with PostgresContainer("postgres:15-alpine") as postgres:
        engine = sa.create_engine(postgres.get_connection_url())
        with engine.connect() as conn:
            result = conn.execute(sa.text("SELECT 1 + 1"))
            assert result.scalar() == 2
