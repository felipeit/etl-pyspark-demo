from src.domain.loader import DatabaseLoader, Loader
import pyspark.testing
from pyspark.testing.utils import assertDataFrameEqual

async def test_verify_if_instance_from_loader() -> None:
    output = DatabaseLoader()
    assert isinstance(output, Loader)

async def test_method_process_of_loader() -> None:
    # df1 = spark.createDataFrame(data=[("1", 1000), ("2", 3000)], schema=["id", "amount"])
    # df2 = spark.createDataFrame(data=[("1", 1000), ("2", 3000)], schema=["id", "amount"])
    pass