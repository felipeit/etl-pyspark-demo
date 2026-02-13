from src.infra.data.adapters.spark_conn import SparkAdapter, SparkAdapters


def test_spark_adapter_singleton() -> None:
    a = SparkAdapter()
    b = SparkAdapter()
    assert a is b
    # backward-compatible name
    assert SparkAdapters is SparkAdapter


def test_get_session_has_read_attribute() -> None:
    sess = SparkAdapter().get_session()
    assert hasattr(sess, "read")
