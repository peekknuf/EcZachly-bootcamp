from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType

def test_actor_streaks(spark):
    input_data = [
        Row(actor="Al Pacino", quality_class="Good", current_year=2001, is_active=True),
        Row(actor="Al Pacino", quality_class="Good", current_year=2002, is_active=False),
        Row(actor="Al Pacino", quality_class="Bad", current_year=2003, is_active=False),
        Row(actor="Al Pacino", quality_class="Good", current_year=2020, is_active=True),
        Row(actor="Robert De Niro", quality_class="Bad", current_year=2005, is_active=True),
        Row(actor="Robert De Niro", quality_class="Bad", current_year=2006, is_active=False),
    ]

    input_schema = StructType([
        StructField("actor", StringType(), True),
        StructField("quality_class", StringType(), True),
        StructField("current_year", IntegerType(), True),
        StructField("is_active", BooleanType(), True),
    ])

    input_df = spark.createDataFrame(input_data, schema=input_schema)
    input_df.createOrReplaceTempView("actors")

    expected_data = [
        Row(actor="Al Pacino", quality_class="Good", is_active=True, first_year=2001, end_year=2001, current_year=2021),
        Row(actor="Al Pacino", quality_class="Good", is_active=False, first_year=2002, end_year=2002, current_year=2021),
        Row(actor="Al Pacino", quality_class="Bad", is_active=False, first_year=2003, end_year=2003, current_year=2021),
        Row(actor="Al Pacino", quality_class="Good", is_active=True, first_year=2020, end_year=2020, current_year=2021),
        Row(actor="Robert De Niro", quality_class="Bad", is_active=True, first_year=2005, end_year=2005, current_year=2021),
        Row(actor="Robert De Niro", quality_class="Bad", is_active=False, first_year=2006, end_year=2006, current_year=2021),
    ]

    expected_schema = StructType([
        StructField("actor", StringType(), True),
        StructField("quality_class", StringType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("first_year", IntegerType(), True),
        StructField("end_year", IntegerType(), True),
        StructField("current_year", IntegerType(), True),
    ])

    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)
