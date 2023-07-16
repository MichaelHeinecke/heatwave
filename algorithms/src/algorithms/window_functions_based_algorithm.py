from pyspark.sql import DataFrame, Window, functions as f
from pyspark.sql.types import IntegerType


def calculate_heatwaves(df: DataFrame) -> DataFrame:
    """Calculate heat waves given a dataframe with a date column and
    a temperature column.

    A heat wave is defined by the KNMI definition:
    A heat wave is a succession of at least 5 summer days (maximum temperature
    25.0 °C or higher) in De Bilt, of which at least three are tropical days
    (maximum temperature 30.0 °C or higher).

    :param df: The dataframe to calculate the heatwaves on. Must include a column named
    "date" of type pyspark.sql.types.DateType and a column named "temperature" of type
    pyspark.sql.types.DoubleType
    :return: A dataframe containing calculated heat waves with 5 columns: 1. From date,
    2. To date (inc), 3. Duration (in days), 4. Number of tropical days,
    5. Max temperature.
    :raises ValueError: Thrown required columns are not present in dataframe, or if
    these columns are not of the required type.
    """
    col_names_types = dict(df.dtypes)
    for col in [("date", "date"), ("temperature", "double")]:
        if col[0] not in col_names_types:
            raise ValueError(f"The required column '{col[0]}' is not in the dataframe.")
        if col_names_types.get(col[0]) != col[1]:
            raise ValueError(
                f"Column '{col[0]}' is of type {col_names_types.get(col[0])}. "
                f"It must be of type '{col[0]}'."
            )

    # There's nothing to partition the window by in this case. All data will be moved
    # to a single partition and be processed on a single worker. This only works as
    # long as the dataframe is sufficiently small (which it is at this point).
    window_spec = Window.orderBy("date")

    is_start_of_sequence = f.col("number_of_days_to_preceding_rows_date") > 1
    is_tropical_day = df.temperature >= 30.0
    is_heat_wave = (f.col("duration_in_days") >= 5) & (
        f.col("number_tropical_days") >= 3
    )

    result_df = (
        df
        # Only keep days which might be part of a heat wave.
        .where(df.temperature >= 25.0)
        # Ordering by date, calculate the date difference between each row and
        # its preceding row.
        .withColumn(
            "number_of_days_to_preceding_rows_date",
            f.datediff("date", f.lag(df.date).over(window_spec)),
        )
        # If the number of days to the preceding row is greater than 1 in a row,
        # the date is the start of a new sequence.
        .withColumn(
            "sequence_start_date",
            f.when(is_start_of_sequence, df.date)
            # Handle edge case: First row is not assigned a sequence start date
            # as number_of_days_to_preceding_rows_date is null. Assign start date
            # to this row, unless it's the first day of the month. In that case
            # there might be a heat wave extending from the previous month on
            # which we don't have full data.
            .when(
                f.col("number_of_days_to_preceding_rows_date").isNull()
                & (f.dayofmonth(df.date) != 1),
                df.date,
            ),
        )
        # Find the last non-null value (going upwards in the
        # column/backwards in time).
        .withColumn(
            "sequence_start_date",
            f.last(f.col("sequence_start_date"), ignorenulls=True).over(window_spec),
        )
        .where(f.col("sequence_start_date").isNotNull())
        # Add helper column to calculate number of tropical days
        .withColumn("tropical_day", f.when(is_tropical_day, 1))
        # Aggregate by sequence_start_date
        .groupBy(f.col("sequence_start_date"))
        .agg(
            f.count("*").alias("duration_in_days"),
            f.sum("tropical_day").alias("number_tropical_days"),
            f.max("temperature").alias("max_temperature"),
        )
        # As the end date of our sequence needs to be the last day which is part
        # of the heat wave rather than the first day which is not, 1 day needs
        # to be subtracted.
        .withColumn(
            "to_date",
            f.col("sequence_start_date")
            + f.col("duration_in_days").cast(IntegerType())
            - 1,
        )
        # Filter out date sequences that don't qualify as heat wave
        .where(is_heat_wave)
        .select(
            f.col("sequence_start_date").alias("from_date"),
            f.col("to_date"),
            f.col("duration_in_days"),
            f.col("number_tropical_days"),
            f.col("max_temperature"),
        )
    )

    return result_df
