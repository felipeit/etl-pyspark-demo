from typing import Any
import pytest
import pandas as pd
from pyspark.sql import SparkSession
from requests import session

from src.domain.loader import DatabaseLoader
from src.infra.orm.db_sqlalchemy import DbSQLAlchemy
from src.infra.repository.csv_file_repository import CSVData, inMemoryCSVRepository
# from pyspark.sql.functions import col


@pytest.fixture(scope='session')
async def spark_fixture() -> Any:
    spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()
    yield spark

@pytest.fixture
async def csv_test(tmp_path) -> str:
    list = []
    # It will create 100 records
    for i in range(0, 100):
        email = 'tester{i}@gmail.com'.replace("{i}",str(i))
        phone = "0000000000"
        phone = str(i) + phone[len(str(i)):] 
        fname = "test" + str(i)
        lname = "test" + str(i)
        dob = "199{a}-{a}-0{a}".replace("{a}",str(len(str(i))))
        list.append((fname, lname, email, phone, dob, str(i)))
    columns = ['First Name', 'Last Name', 'Email Address', 'Phone Number','Date Of Birth','Current Loyalty Point Total']

    df = pd.DataFrame(list, columns = columns)
    csv_path = tmp_path / "random.csv"
    df.to_csv(csv_path, index = False)
    return csv_path.__str__()

@pytest.fixture
async def json_test(tmp_path) -> str:
    list = []
    # It will create 100 records
    for i in range(0, 100):
        email = 'tester{i}@gmail.com'.replace("{i}",str(i))
        phone = "0000000000"
        phone = str(i) + phone[len(str(i)):] 
        fname = "test" + str(i)
        lname = "test" + str(i)
        dob = "199{a}-{a}-0{a}".replace("{a}",str(len(str(i))))
        list.append((fname, lname, email, phone, dob, str(i)))
    columns = ['First Name', 'Last Name', 'Email Address', 'Phone Number','Date Of Birth','Current Loyalty Point Total']

    df = pd.DataFrame(list, columns = columns)
    json_path = tmp_path / "random.json"
    df.to_json(json_path, index = False)
    return json_path.__str__()

class MockDbSQLAlchemy(DbSQLAlchemy):
    def save(self) -> None:
        pass 

class MockRepository(inMemoryCSVRepository):
    def __init__(self) -> None:
        pass

@pytest.fixture
def database_loader() -> DatabaseLoader:
    return DatabaseLoader()