
from chispa.dataframe_comparer import *
from pyspark.sql.types import StructType, StructField, StringType,MapType, IntegerType
from pyspark.sql import Row
from ..jobs.hosts_cumulated_job import do_hosts_cumulated_transformation

def test_hosts_cumulated(spark):
    # Prepare events data
    events_data = [
        Row(url="http://example.com", referrer=None, user_id=1, host="example.com", event_time="2023-01-02"),
        Row(url="http://example.com", referrer=None, user_id=1, host="example.com", event_time="2023-01-03"),
    ]
    
    # Prepare existing hosts_cumulated data (simulating previous day's state)
    hosts_cumulated_data = [
        Row(host="example.com", date="2023-01-02", host_activity_datelist={"2023-01-02": 1})
    ]

    # Create events DataFrame with schema
    events_schema = StructType([
        StructField("url", StringType(), True),
        StructField("referrer", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("host", StringType(), True),
        StructField("event_time", StringType(), True)
    ])
    events_df = spark.createDataFrame(events_data, schema=events_schema)

    # Create hosts_cumulated DataFrame with schema
    hosts_cumulated_schema = StructType([
        StructField("host", StringType(), True),
        StructField("date", StringType(), True),
        StructField("host_activity_datelist", MapType(StringType(), IntegerType()), True)
    ])
    hosts_cumulated_df = spark.createDataFrame(hosts_cumulated_data, schema=hosts_cumulated_schema)

    # Create temporary views needed for the SQL query
    events_df.createOrReplaceTempView("events")
    hosts_cumulated_df.createOrReplaceTempView("hosts_cumulated")

    # Run transformation
    actual_df = do_hosts_cumulated_transformation(spark, events_df)

    # Prepare expected result
    expected_data = [
        Row(host="example.com", date="2023-01-03", host_activity_datelist={"2023-01-02": 1, "2023-01-03": 1})
    ]

    expected_schema = StructType([
        StructField("host", StringType(), True),
        StructField("date", StringType(), True),
        StructField("host_activity_datelist", MapType(StringType(), IntegerType()), True)
    ])

    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    # Compare DataFrames
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)