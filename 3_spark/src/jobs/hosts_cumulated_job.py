
from pyspark.sql import SparkSession

query = """
with cte_dupes_identified as (
     select
            *,
            row_number()
                over (
                    partition by
                        url,
                        referrer,
                        e.user_id,
                        host,
                        event_time
                order by
                    event_time desc)
            as checking_dupes
        from events as e
    ),
    cte_prepared_events as (
        select
            host,
            cast(event_time as date) as event_time
        from cte_dupes_identified where checking_dupes = 1
    ),
    cte_yesterday as (
        select * from hosts_cumulated where date = date('2023-01-02')
    ),
    cte_today as (
        select
            host,
            event_time as days,
            count(1) as num_events
        from cte_prepared_events
        where
            event_time = '2023-01-03'
        group by 1,2
    ),
    cte_aggregated_json AS (
        SELECT
            coalesce(t.host, y.host) as host,
            COALESCE(t.days, y.date + INTERVAL '1 day') AS date,
            map_concat(
                COALESCE(y.host_activity_datelist, map()),
                CASE WHEN t.host IS NOT NULL 
                     THEN map(cast(t.days as string), t.num_events)
                     ELSE map()
                END
            ) AS host_activity_datelist
        FROM cte_yesterday y
        FULL OUTER JOIN cte_today t ON t.host = y.host
    )
    select * from cte_aggregated_json
"""


def do_hosts_cumulated_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("events")
    return spark.sql(query)

def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("hosts_cumulated") \
      .getOrCreate()
    output_df = do_hosts_cumulated_transformation(spark, spark.table("events"))
    output_df.write.mode("overwrite").insertInto("hosts_cumulated")
