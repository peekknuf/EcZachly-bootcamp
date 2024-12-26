import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.table.expressions import lit, col
from pyflink.table.window import Session


def create_session_sink_table(t_env):
    table_name = "session_events_sink"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            session_start TIMESTAMP,
            session_end TIMESTAMP,
            ip VARCHAR,
            host VARCHAR,
            event_count BIGINT,
            PRIMARY KEY (session_start, ip, host) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_session_metrics_sink(t_env):
    table_name = "session_metrics_sink"
    # Funny thing that Flink REQUIRES the PK for Session to work BUT CANT ENFORCE IT IN ANY WAY
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            host VARCHAR,
            avg_events_per_session NUMERIC(10,2),
            PRIMARY KEY (host) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_processed_events_source_kafka(t_env):
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "process_events_kafka"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_time VARCHAR,
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR,
            window_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR window_timestamp AS window_timestamp - INTERVAL '15' SECOND
        ) WITH (
             'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def log_aggregation():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    # not really important but for the sake of testing and trying things out bumped it up to 2
    env.set_parallelism(2)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create Kafka table
        source_table = create_processed_events_source_kafka(t_env)

        session_table = create_session_sink_table(t_env)
        session_metrics_table = create_session_metrics_sink(t_env)
        # Make all the necessary transformations with 5 minute window and ingest the data
        # Tested with different window sizes, for example 1 min window gave at most 14 events, lasted 3.5 minutes
        t_env.from_path(source_table).window(
            Session.with_gap(lit(5).minutes)
            .on(col("window_timestamp"))
            .alias("session_window")
        ).group_by(col("session_window"), col("ip"), col("host")).select(
            col("session_window").start.alias("session_start"),
            col("session_window").end.alias("session_end"),
            col("ip"),
            col("host"),
            lit(1).count.alias("event_count"),
        ).execute_insert(
            session_table
        )
        # Pretty much the same as above but more nuanced and rather gnarly.
        # www.techcreator.io is by far the most popular host
        t_env.from_path(source_table).window(
            Session.with_gap(lit(5).minutes)
            .on(col("window_timestamp"))
            .alias("session_window")
        ).group_by(col("session_window"), col("ip"), col("host")).select(
            col("session_window").start.alias("session_start"),
            col("session_window").end.alias("session_end"),
            col("ip"),
            col("host"),
            lit(1).count.alias("event_count"),
        ).group_by(
            col("host")
        ).select(
            col("host"), col("event_count").avg.alias("avg_events_per_session")
        ).execute_insert(
            session_metrics_table
        )
    # Oh how many times have I seen this while working on this thing
    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == "__main__":
    log_aggregation()
