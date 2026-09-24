from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def count_nulls(df: DataFrame, columns: list[str]) -> dict[str, int]:
    """
    Return null counts for selected columns.
    """

    expressions = [
        F.sum(
            F.when(F.col(column).isNull(), 1).otherwise(0)
        ).alias(column)
        for column in columns
    ]

    row = df.select(*expressions).collect()[0]

    return {
        column: row[column]
        for column in columns
    }


def count_duplicates(
    df: DataFrame,
    key_columns: list[str],
) -> int:
    """
    Count duplicate groups based on the supplied key columns.
    """

    return (
        df
        .groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )


def count_rows_outside_date_range(
    df: DataFrame,
    timestamp_column: str,
    start_date: str,
    end_date: str,
) -> int:
    """
    Count rows outside an expected inclusive date range.
    """

    return (
        df
        .filter(
            (F.to_date(F.col(timestamp_column)) < F.lit(start_date))
            | (F.to_date(F.col(timestamp_column)) > F.lit(end_date))
        )
        .count()
    )
