
from pyspark.sql import SparkSession

query = """with previous as (
select actor
                , quality_class
                , current_year
                , is_active
                , LAG("quality_class", 1) over (partition by actor order by current_year) as previous_quality_class
                , LAG(is_active , 1) over (partition by actor order by current_year) as previous_is_active
from actors
),
indicators as (
select *
        , case when "quality_class" <> previous_quality_class then 1
                        when is_active <> previous_is_active then 1
                else 0
        end as change_indicator
from previous
),
streaks as (

select *,
                sum(change_indicator)
                        over (partition by actor order by current_year) as streak_identifier
from indicators
),
final_prep as (
select actor
                , quality_class
                , is_active
                , streak_identifier
                , min(current_year) as first_year
                , max(current_year) as end_year
from streaks
group by actor, streak_identifier, is_active, quality_class
order by actor, streak_identifier
)

select actor
                , quality_class
                , is_active
                , first_year
                , end_year
                , 2021 as current_year
from final_prep"""

def do_actors_backfill_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("actors")
    return spark.sql(query)

def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("actors_backfill") \
      .getOrCreate()
    output_df = do_actors_backfill_transformation(spark, spark.table("actors"))
    output_df.write.mode("overwrite").insertInto("actors_history_scd")