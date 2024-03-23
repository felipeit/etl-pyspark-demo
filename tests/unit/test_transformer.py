from src.domain.transformer import JSONTransformer, Transformer

async def test_verify_if_instance_from_loader() -> None:
    output = JSONTransformer()
    assert isinstance(output, Transformer)

async def test_verify_if_jsontransformer_process_a_dataframe_with_sucesss() -> None:
    # df1 = spark.createDataFrame(data=[("1", 1000), ("2", 3000)], schema=["id", "amount"])
    # df2 = spark.createDataFrame(data=[("1", 1000), ("2", 3000)], schema=["id", "amount"])
    pass