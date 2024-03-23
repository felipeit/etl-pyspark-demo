from typing import Any
import pytest
import pandas as pd
from pyspark.sql import SparkSession
# from pyspark.sql.functions import col


@pytest.fixture
async def spark_fixture() -> Any:
    spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()
    yield spark

@pytest.fixture
async def csv_test() -> Any:
    list = []
    # It will create 100k records
    for i in range(0,100000):
        email = 'tester{i}@aeturnum.com'.replace("{i}",str(i))
        phone = "0000000000"
        phone = str(i) + phone[len(str(i)):] 
        fname = "test" + str(i)
        lname = "test" + str(i)
        dob = "199{a}-{a}-0{a}".replace("{a}",str(len(str(i))))
        list.append((fname, lname, email, phone, dob, str(i)))
    columns = ['First Name', 'Last Name', 'Email Address', 'Phone Number','Date Of Birth','Current Loyalty Point Total']

    df = pd.DataFrame(list, columns = columns)
    return df.to_csv("random.csv", index = False)
