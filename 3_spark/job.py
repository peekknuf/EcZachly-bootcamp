from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .appName("homework")
         .config("spark.sql.sources.v2.bucketing.enabled", "true")
         .config("spark.sql.iceberg.planning.preserve-data-grouping", "true")
         .config("spark.sql.autoBroadcastJoinThreshold", "-1")
         .getOrCreate())

# we start by importing our data from the csv files inferring the schema
base_path = "/home/iceberg/data/"

medals_csv = spark.read.option("inferSchema", "true").option("header", "true").csv(f"{base_path}medals.csv")
maps_csv = spark.read.option("inferSchema", "true").option("header", "true").csv(f"{base_path}maps.csv")

matches_csv = spark.read.option("inferSchema", "true").option("header", "true").csv(f"{base_path}matches.csv")
match_details_csv = spark.read.option("inferSchema", "true").option("header", "true").csv(f"{base_path}match_details.csv")
medals_matches_players_csv = spark.read.option("inferSchema", "true").option("header", "true").csv(f"{base_path}medals_matches_players.csv")

#we create the DB and the tables
spark.sql("""CREATE DATABASE IF NOT EXISTS bootcamp""")

spark.sql("""CREATE TABLE IF NOT EXISTS bootcamp.medals (
    medal_id STRING,
    sprite_uri STRING,
    sprite_left INT,
    sprite_top INT,
    sprite_sheet_width INT,
    sprite_sheet_height INT,
    sprite_width INT,
    sprite_height INT,
    classification STRING,
    description STRING,
    name STRING,
    difficulty STRING
)
USING iceberg""")


spark.sql("""CREATE TABLE IF NOT EXISTS bootcamp.maps (
    map_id STRING,
    name STRING,
    description STRING
)
USING iceberg""")


spark.sql("""CREATE TABLE IF NOT EXISTS bootcamp.matches_bucketed (
    match_id STRING,
    map_id STRING,
    playlist_id STRING,
    game_variant_id STRING,
    completion_date TIMESTAMP
)
USING iceberg
PARTITIONED BY (completion_date, BUCKET(match_id, 16))""")

spark.sql("""CREATE TABLE IF NOT EXISTS bootcamp.match_details_bucketed (
    match_id STRING,
    player_gamertag STRING,
    player_total_kills INT,
    player_total_deaths INT,
    did_win BOOLEAN,
    team_id STRING
)
USING iceberg
PARTITIONED BY (player_gamertag, BUCKET(match_id, 16))""")

spark.sql("""CREATE TABLE IF NOT EXISTS bootcamp.medals_matches_players_bucketed (
    match_id STRING,
    player_gamertag STRING,
    medal_id STRING,
    count INT
)
USING iceberg
PARTITIONED BY (player_gamertag, BUCKET(match_id, 16))""")

# after that we save the data in the tables adjusting some of the types and column names
# partitioning and bucketing the data
medals_csv.write.mode("overwrite").saveAsTable("bootcamp.medals")
maps_csv.write.mode("overwrite").saveAsTable("bootcamp.maps")

matches_csv.select("match_id", "mapid", "playlist_id", "game_variant_id", "completion_date") \
        .withColumnRenamed("mapid", "map_id") \
        .sortWithinPartitions(col("match_id")) \
        .write.mode("overwrite") \
        .bucketBy(16, "match_id") \
        .saveAsTable("bootcamp.matches_bucketed")



match_details_csv.select("match_id", "player_gamertag", "player_total_kills", "player_total_deaths","did_win","team_id") \
            .withColumn("did_win", col("did_win").cast("boolean")) \
            .withColumn("team_id", col("team_id").cast("string")) \
            .sortWithinPartitions(col("match_id")) \
            .write.mode("overwrite") \
            .bucketBy(16, "match_id") \
            .saveAsTable("bootcamp.match_details_bucketed")

medals_matches_players_csv.sortWithinPartitions(col('match_id')) \
                        .write.mode("overwrite") \
                        .bucketBy(16, "match_id") \
                        .saveAsTable("bootcamp.medals_matches_players_bucketed")


# now we read the data from the tables instead of csv
medals = spark.read.format("iceberg").table("bootcamp.medals")
maps = spark.read.format("iceberg").table("bootcamp.maps")
match_details = spark.read.format("iceberg").table("bootcamp.match_details_bucketed")
matches = spark.read.format("iceberg").table("bootcamp.matches_bucketed")
medals_matches_players = spark.read.format("iceberg").table("bootcamp.medals_matches_players_bucketed")


# actually performing the join with multiple steps
result = match_details.join(
    matches,
    match_details.match_id == matches.match_id,
    "inner"
).join(
    medals_matches_players,
    (medals_matches_players.match_id == match_details.match_id) &
    (medals_matches_players.player_gamertag == match_details.player_gamertag),
    "inner"
).join(
    broadcast(maps),
    matches.map_id == maps.mapid,
    "inner"
).join(
    broadcast(medals),
    medals_matches_players.medal_id == medals.medal_id,
    "inner"
)

# we want to save the final result in an iceberg table because it's output is highly valuable
# for future possible analysis
spark.sql("""CREATE TABLE IF NOT EXISTS bootcamp.final_result (
    match_id STRING,
    player_gamertag STRING,
    player_total_kills INT,
    player_total_deaths INT,
    did_win BOOLEAN,
    playlist_id STRING,
    medal_id STRING,
    map_id STRING,
    completion_date TIMESTAMP
)
USING iceberg
PARTITIONED BY (map_id)""")

final_result = result.select(
    "match_details_bucketed.match_id",
    "match_details_bucketed.player_gamertag",
    "match_details_bucketed.player_total_kills",
    "match_details_bucketed.player_total_deaths",
    "match_details_bucketed.did_win",
    "matches_bucketed.playlist_id",
    "medals_matches_players_bucketed.medal_id",
    "matches_bucketed.map_id",
    "matches_bucketed.completion_date"
).sortWithinPartitions(
    col("matches_bucketed.map_id")
).write.mode("overwrite") \
  .partitionBy("map_id") \
  .saveAsTable("bootcamp.final_result")


outcome = spark.read.format("iceberg").table("bootcamp.final_result")
# we perform some analysis and output our results
avg_kills_per_game = outcome.groupBy("player_gamertag") \
    .agg(
        (F.avg(outcome["player_total_kills"]).cast("double")).alias("avg_kills_per_game")
    ) \
    .orderBy("avg_kills_per_game", ascending=False)

most_played_playlist = outcome.groupBy("playlist_id") \
    .agg(
        F.count(outcome["match_id"]).alias("playlist_count")
    ) \
    .orderBy("playlist_count", ascending=False)

most_played_map = outcome.groupBy("map_id") \
    .agg(
        F.count(outcome["match_id"]).alias("map_count")
    ) \
    .orderBy("map_count", ascending=False)

killing_spree_map = outcome.join(
    broadcast(medals),
    outcome["medal_id"] == medals["medal_id"],
    "inner"
).filter(medals["name"] == "Killing Spree") \
    .groupBy("map_id") \
    .agg(
        F.count(outcome["medal_id"]).alias("killing_spree_count")
    ) \
    .orderBy("killing_spree_count", ascending=False)




avg_kills_per_game.show(1)
most_played_playlist.show(1)
most_played_map.show(1)
killing_spree_map.show(1)
