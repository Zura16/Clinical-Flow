"""
Surrogate keys for the warehouse.

Keys are a deterministic hash of the natural key, not a counter. The same natural key produces the
same surrogate key on every run, on any machine, whatever order rows arrive in — so a rebuilt
dimension does not invalidate the facts that already point at it. A counter (`monotonically_
increasing_id`, an identity column, a sequence) would renumber on every rebuild and needs
coordination to stay unique across partitions.

The trade is collisions: xxhash64 gives 64 bits, so at a million rows the chance of any collision
is on the order of 1 in 10^8. That is the cost of not coordinating, and it is worth stating out
loud rather than pretending the risk is zero. A grain test on each dimension would catch one.

UNKNOWN_SK (-1) is the unknown member: every dimension has a real row with that key, so a fact
whose dimension is missing or late still joins to something instead of carrying a NULL.
"""

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

UNKNOWN_SK = -1
# If a hash ever lands on the sentinel, move it: a real member must never claim the unknown key.
_SENTINEL_REPLACEMENT = -2


def surrogate_key(*columns: Column) -> Column:
    parts = [F.coalesce(c.cast("string"), F.lit("~null~")) for c in columns]
    hashed = F.xxhash64(F.concat_ws("||", *parts))
    return F.when(hashed == F.lit(UNKNOWN_SK), F.lit(_SENTINEL_REPLACEMENT)).otherwise(hashed)


def date_key(column: Column) -> Column:
    """dim_date's key: YYYYMMDD as an integer, or the unknown member when there is no date."""
    return F.coalesce(F.date_format(column, "yyyyMMdd").cast("int"), F.lit(UNKNOWN_SK))


def with_unknown_member(df: DataFrame, spark: SparkSession, sk_column: str, overrides: dict | None = None) -> DataFrame:
    """Prepend the unknown member row, typed to match the dimension."""
    overrides = overrides or {}
    row = {}
    for field in df.schema.fields:
        if field.name == sk_column:
            row[field.name] = F.lit(UNKNOWN_SK).cast(field.dataType)
        elif field.name in overrides:
            row[field.name] = F.lit(overrides[field.name]).cast(field.dataType)
        elif isinstance(field.dataType, StringType):
            row[field.name] = F.lit("UNKNOWN").cast(field.dataType)
        else:
            row[field.name] = F.lit(None).cast(field.dataType)
    unknown = spark.range(1).select(*[expr.alias(name) for name, expr in row.items()])
    return unknown.unionByName(df.select(*row.keys()))
