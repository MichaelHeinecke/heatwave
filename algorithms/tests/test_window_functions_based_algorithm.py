import datetime
import logging

import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, DateType, DoubleType, LongType

from algorithms.window_functions_based_algorithm import calculate_heatwaves


# Fixture code adapted from
# https://blog.cellenza.com/en/data/pyspark-unit-test-best-practices/
@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Configure and create the Spark session for all tests.

    :return: A SparkSession object
    """
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("unit-tests")
        .config("spark.driver.memory", "512m")
        # Changing default number of partitions
        .config("spark.sql.shuffle.partitions", 2)
        # Disabling rdd and map output compression as data is already small for tests
        .config("spark.shuffle.compress", False)
        # Disable Spark UI for tests
        .config("spark.ui.enabled", False)
        .config("spark.ui.showConsoleProgress", False)
        # Extra configs to optimize Delta internal operations on tests
        .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", 5)
        .getOrCreate()
    )

    logger = logging.getLogger("py4j")
    logger.setLevel(logging.WARN)

    # Use yield instead of return for a clean teardown. See:
    # https://docs.pytest.org/en/6.2.x/fixture.html#teardown-cleanup-aka-fixture-finalization
    yield spark
    spark.stop()


@pytest.fixture()
def input_schema():
    return (
        StructType()
        .add("date", DateType(), False)
        .add("temperature", DoubleType(), False)
    )


@pytest.fixture()
def result_schema():
    return (
        StructType()
        .add("from_date", DateType())
        .add("to_date", DateType())
        .add("duration_in_days", LongType())
        .add("number_tropical_days", LongType())
        .add("max_temperature", DoubleType())
    )


def test_when_heat_wave_present_in_data_then_calculated(
    spark, input_schema, result_schema
):
    data = [
        (datetime.date(2004, 6, 1), 21.0),
        (datetime.date(2004, 6, 2), 25.9),
        (datetime.date(2004, 6, 3), 26.1),
        (datetime.date(2004, 6, 4), 24.6),
        (datetime.date(2004, 6, 5), 30.0),
        (datetime.date(2004, 6, 6), 25.3),
        (datetime.date(2004, 6, 7), 31.8),
        (datetime.date(2004, 6, 8), 25.1),
        (datetime.date(2004, 6, 9), 22.1),
        (datetime.date(2004, 6, 10), 27.4),  # Heat wave start
        (datetime.date(2004, 6, 11), 29.3),
        (datetime.date(2004, 6, 12), 25.1),
        (datetime.date(2004, 6, 13), 32.4),
        (datetime.date(2004, 6, 14), 29.5),
        (datetime.date(2004, 6, 15), 30.9),
        (datetime.date(2004, 6, 16), 31.0),
        (datetime.date(2004, 6, 17), 27.8),
        (datetime.date(2004, 6, 18), 25.3),  # Heat wave end
        (datetime.date(2004, 6, 19), 22.4),
        (datetime.date(2004, 6, 20), 25.4),
        (datetime.date(2004, 6, 21), 27.4),
    ]

    input_df = spark.createDataFrame(data, schema=input_schema)
    actual_df = input_df.transform(calculate_heatwaves)

    expected_data = [
        (datetime.date(2004, 6, 10), datetime.date(2004, 6, 18), 9, 3, 32.4),
    ]
    expected_df = spark.createDataFrame(expected_data, schema=result_schema)

    assert_df_equality(actual_df, expected_df, ignore_nullable=True)


def test_when_heat_wave_start_on_first_day_with_at_least_25d_then_calculated(
    spark, input_schema, result_schema
):
    data = [
        (datetime.date(2004, 6, 1), 21.0),
        (datetime.date(2004, 6, 2), 25.9),  # Heat wave start
        (datetime.date(2004, 6, 3), 26.1),
        (datetime.date(2004, 6, 4), 25.6),
        (datetime.date(2004, 6, 5), 30.0),
        (datetime.date(2004, 6, 6), 25.3),
        (datetime.date(2004, 6, 7), 31.8),
        (datetime.date(2004, 6, 8), 30.1),  # Heat wave end
        (datetime.date(2004, 6, 9), 22.1),
    ]

    input_df = spark.createDataFrame(data, schema=input_schema)
    actual_df = input_df.transform(calculate_heatwaves)

    expected_data = [
        (datetime.date(2004, 6, 2), datetime.date(2004, 6, 8), 7, 3, 31.8),
    ]
    expected_df = spark.createDataFrame(expected_data, schema=result_schema)

    assert_df_equality(actual_df, expected_df, ignore_nullable=True)


def test_when_wave_start_first_day_with_at_least_25d_and_first_of_month_then_empty_df(
    spark, input_schema, result_schema
):
    data = [
        (datetime.date(2004, 6, 1), 25.0),  # Heat wave start - edge case to be excluded
        (datetime.date(2004, 6, 2), 25.9),
        (datetime.date(2004, 6, 3), 26.1),
        (datetime.date(2004, 6, 4), 25.6),
        (datetime.date(2004, 6, 5), 30.0),
        (datetime.date(2004, 6, 6), 25.3),
        (datetime.date(2004, 6, 7), 31.8),
        (datetime.date(2004, 6, 8), 30.1),  # Heat wave end
        (datetime.date(2004, 6, 9), 22.1),
    ]

    input_df = spark.createDataFrame(data, schema=input_schema)
    actual_df = input_df.transform(calculate_heatwaves)

    expected_df = spark.createDataFrame(
        spark.sparkContext.emptyRDD(), schema=result_schema
    )

    assert_df_equality(actual_df, expected_df, ignore_nullable=True)


def test_when_multiple_heat_waves_and_edge_cases_then_calculated(
        spark, input_schema, result_schema
):
    data = [
        (datetime.date(2004, 6, 1), 25.0),  # Heat wave start - edge case to be excluded
        (datetime.date(2004, 6, 2), 25.9),
        (datetime.date(2004, 6, 3), 30.1),
        (datetime.date(2004, 6, 4), 30.6),
        (datetime.date(2004, 6, 5), 30.0),  # Heat wave end
        (datetime.date(2004, 6, 6), 24.3),
        (datetime.date(2004, 6, 7), 31.8),
        (datetime.date(2004, 6, 8), 30.1),
        (datetime.date(2004, 6, 9), 22.1),
        (datetime.date(2004, 6, 10), 25.0),  # Heat wave start
        (datetime.date(2004, 6, 11), 25.9),
        (datetime.date(2004, 6, 12), 26.1),
        (datetime.date(2004, 6, 13), 25.6),
        (datetime.date(2004, 6, 14), 30.0),
        (datetime.date(2004, 6, 15), 25.3),
        (datetime.date(2004, 6, 16), 31.8),
        (datetime.date(2004, 6, 17), 30.1),  # Heat wave end
        (datetime.date(2004, 6, 18), 24.0),
        (datetime.date(2004, 6, 19), 25.9),  # Heat wave start
        (datetime.date(2004, 6, 20), 26.1),
        (datetime.date(2004, 6, 21), 25.6),
        (datetime.date(2004, 6, 22), 30.0),
        (datetime.date(2004, 6, 23), 25.3),
        (datetime.date(2004, 6, 24), 31.8),
        (datetime.date(2004, 6, 25), 30.1),
        (datetime.date(2004, 6, 26), 28.1),
        (datetime.date(2004, 6, 27), 34.1),
        (datetime.date(2004, 6, 28), 32.1),
        (datetime.date(2004, 6, 29), 30.3),
        (datetime.date(2004, 6, 30), 30.2),
        (datetime.date(2004, 7, 1), 30.0),  # Heat wave end
        (datetime.date(2004, 7, 2), 24.1),
    ]

    input_df = spark.createDataFrame(data, schema=input_schema)
    actual_df = input_df.transform(calculate_heatwaves)

    expected_data = [
        (datetime.date(2004, 6, 10), datetime.date(2004, 6, 17), 8, 3, 31.8),
        (datetime.date(2004, 6, 19), datetime.date(2004, 7, 1), 13, 8, 34.1),
    ]
    expected_df = spark.createDataFrame(expected_data, schema=result_schema)

    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
