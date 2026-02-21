from typing import Any
import csv
import json
import pytest

try:
    import pandas as pd
except Exception:  # pragma: no cover - optional dependency
    pd = None

try:
    from pyspark.sql import SparkSession
except Exception:  # pragma: no cover - optional dependency
    SparkSession = None

from src.domain.loader import DatabaseLoader
from src.infra.orm.db_sqlalchemy import DbSQLAlchemy
from src.infra.repository.csv_file_repository import inMemoryCSVRepository


@pytest.fixture(scope='session')
async def spark_fixture() -> Any:
    if SparkSession is None:
        yield None
        return
    spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()
    yield spark


def _build_rows() -> tuple[list[tuple[str, ...]], list[str]]:
    rows = []
    for i in range(0, 100):
        email = f"tester{i}@gmail.com"
        phone = str(i) + "0000000000"[len(str(i)):]
        fname = f"test{i}"
        lname = f"test{i}"
        dob = "199{a}-{a}-0{a}".replace("{a}", str(len(str(i))))
        rows.append((fname, lname, email, phone, dob, str(i)))
    columns = [
        'First Name',
        'Last Name',
        'Email Address',
        'Phone Number',
        'Date Of Birth',
        'Current Loyalty Point Total',
    ]
    return rows, columns


@pytest.fixture
async def csv_test(tmp_path) -> str:
    rows, columns = _build_rows()
    csv_path = tmp_path / "random.csv"

    if pd is not None:
        pd.DataFrame(rows, columns=columns).to_csv(csv_path, index=False)
    else:
        with csv_path.open("w", newline="", encoding="utf-8") as fp:
            writer = csv.writer(fp)
            writer.writerow(columns)
            writer.writerows(rows)

    return str(csv_path)


@pytest.fixture
async def json_test(tmp_path) -> str:
    rows, columns = _build_rows()
    json_path = tmp_path / "random.json"

    if pd is not None:
        pd.DataFrame(rows, columns=columns).to_json(json_path, index=False)
    else:
        payload = [dict(zip(columns, row)) for row in rows]
        json_path.write_text(json.dumps(payload), encoding="utf-8")

    return str(json_path)


class MockDbSQLAlchemy(DbSQLAlchemy):
    def save(self) -> None:
        pass


class MockRepository(inMemoryCSVRepository):
    def __init__(self) -> None:
        pass


@pytest.fixture
def database_loader() -> DatabaseLoader:
    return DatabaseLoader()
