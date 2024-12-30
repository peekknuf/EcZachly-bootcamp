## Dimensional Data Modeling

### Key Accomplishments
- Designed and implemented robust SQL scripts for managing and tracking historical actors' data.  
- Developed custom types, effective use of CTEs, and logical primary keys to ensure data integrity and clarity.  
- Applied advanced SQL techniques like window functions, change indicators, and `UNNEST` to handle complex data requirements.  

### Highlights
- **Actors Table**:  
  Well-structured schema with thoughtful type definitions, although array-based primary keys could lead to potential duplication.  
- **Cumulative Updates**:  
  Effective full outer join and quality class calculations, with attention to null handling in `COALESCE`.  
- **SCD Management**:  
  Temporal consistency through logical separation of unchanged, changed, and new records.  
- **Backfill Logic**:  
  Scalable and transformation-optimized approach using window functions for historical data reconstruction.  

### Possible suggestions for future improvements
1. Enhance commenting on complex scripts for maintainability and clarity. A must. 
2. Revisit array-based keys for uniqueness assurance. (Already made the checks prior to writing the queries.)
3. Optimize performance with appropriate indexing for large datasets.(Not really applicable for this project, but would be helpful for future projects.)


## Fact Data Modeling

### Key Accomplishments
- Designed and implemented efficient SQL scripts for deduplication and fact table creation.
- Utilized advanced SQL techniques like `row_number()`, full outer joins, and array manipulation to handle complex data transformations.
- Ensured primary key definitions aligned with data integrity requirements across different tables.

### Highlights
- **De-duplication in Game Details** (`1-dedupegamedetails.sql`):
  Successfully implemented deduplication using `row_number()` and partitioning, with a well-defined primary key (`dim_game_date`, `dim_team_id`, `dim_player_id`).
- **User Devices Activity DDL** (`2-userdevicescumulated.sql`):
  Designed a clear schema capturing activity dates with an array type and ensured proper primary key definition (`user_id`, `device_id`, `date`).
- **Device Activity Datelist Implementation** (`3-deviceactivitydatelist.sql`):
  Handled deduplication and missing dates using full outer joins, with well-applied partitioning logic.
- **Date List to Integer Transformation** (`4-datelist_int.sql`):
  Efficiently converted date lists into base-2 integers using bit manipulation, showcasing advanced transformation skills.
- **Host Activity Datelist Implementation** (`6-hostscumulatedincremental.sql`):
  Aggregated deduplicated events into a `JSONB` object, effectively managing sparse or dynamic data sets.
- **Reduced Host Fact Array DDL** (`7-hostactivityreduced.sql`):
  Created a concise schema using `integer[]` types for storing hit arrays and unique visitors.
- **Reduced Host Fact Array Implementation** (`8-hostactivityreducedincrementalloading.sql`):
  Properly managed deduplication and array computation for incremental data loading.

### Possible suggestions for future improvements
1. Enhance commenting in complex sections to clarify assumptions and improve maintainability (e.g., `cte_bits`, JSON aggregation logic). 
2. Provide inline examples or documentation illustrating incremental approaches, aiding clarity for reviewers.
3. Clarify data type choices in the documentation to help others understand design decisions.


## Spark and Iceberg Implementation

### Key Accomplishments
- Successfully implemented advanced PySpark configurations and operations to meet task requirements.
- Demonstrated a solid understanding of join strategies, bucketing, aggregation, and data size optimization.
- Integrated modern table formats (Iceberg) to enhance distributed processing efficiency.

### Highlights
- **Disabling Broadcast Joins**:
  Correctly set `spark.sql.autoBroadcastJoinThreshold = "-1"` to disable automatic broadcast joins.
- **Explicit Broadcast Join**:
  Properly used the `broadcast` function for the maps and medals tables, ensuring explicit control over join behavior.
- **Bucket Joins**:
  Applied bucketing with 16 buckets on `match_id` for `matches`, `match_details`, and `medals_matches_players`.
- **Aggregation Queries**:
  - **4a (Highest Average Kills per Game)**: Used `avg` and `orderBy` for accurate aggregation.
  - **4b (Playlist with Most Plays)**: Implemented `groupBy` and `count` effectively.
  - **4c (Most Played Map)**: Correctly identified the map with the highest plays using `groupBy` and `count`.
  - **4d (Highest Killing Spree Medals on Map)**: Combined joins and filtering to determine the map with the highest "Killing Spree" medals.
- **Data Size Optimization**:
  - Used `.sortWithinPartitions` and partitioned results by `map_id`, showcasing understanding of partitioning and sorting.

### Suggestions
1. Experiment with different partitioning strategies and sort orders on Iceberg to optimize data size and query performance further.
2. Include more detailed comments or examples explaining the rationale behind specific partitioning and sorting choices.
3. Explore alternative approaches to sorting within partitions to enhance query patterns, especially for high-cardinality fields.


## SparkSQL and PySpark Job Implementation Feedback

### Key Accomplishments
- Successfully converted Postgres SCD transformation logic to SparkSQL with efficient use of window functions and partitioning.
- Developed well-structured PySpark jobs for backfill and cumulative data transformations, integrating SQL logic seamlessly.
- Demonstrated robust testing practices with thorough validations using `chispa` and SQL assertions.

### Highlights
- **Backfill Query Conversion**:
  - Correctly translated SCD logic for `actors_history_scd` into SparkSQL.
  - Effective use of window functions like `LAG` and partitioned `SUM` for generating streak identifiers.
- **PySpark Jobs**:
  - **Actors Backfill Job** (`actors_backfill_job.py`):
    - Integrated SparkSQL transformations into PySpark effectively.
    - Accurately implemented window functions for detecting changes.
    - Demonstrated clear code readability and logical organization.
  - **Hosts Cumulated Job** (`hosts_cumulated_job.py`):
    - Utilized CTEs, `FULL OUTER JOIN`, and `map_concat` to handle deduplication and cumulative activity aggregation.
    - Strong SparkSQL integration for managing complex transformations.
- **Tests**:
  - **Actors Backfill Test** (`test_actors_backfill.py`):
    - Validated transformation logic using `chispa.dataframe_comparer`.
    - Clear schema definitions for input and expected output enhance test accuracy.
  - **Hosts Cumulated Test** (`test_hosts_cumulated.py`):
    - Verified cumulative logic with example data and temporary views.
    - Effective use of SQL assertions ensures consistency.

### Suggestions
1. Add more inline comments to explain complex SQL queries for better maintainability.
2. Incorporate dynamic date handling in SQL scripts to enhance flexibility and reduce hardcoding.
3. Explore further optimization opportunities in SparkSQL transformations for large datasets.

### General Observations
- **Best Practices**: Demonstrated strong understanding of PySpark and SparkSQL, including SQL expressions, window functions, and cumulative data handling.
- **Code Structure**: Organized, readable codebase with clear variable names and modular SQL logic.
- **Comprehensive Testing**: Thorough test coverage ensures accurate data processing and transformation logic.


# Flink Sessionization Job

## Overview
This Flink job processes web traffic data to sessionize events by IP address and host, using a 5-minute session gap. The job integrates Kafka as the data source and writes sessionized data and metrics to Postgres sinks. Additionally, it computes the average number of web events per session for each host, enabling host-specific insights.

## Key Features

- **Sessionization Logic:**
  - Implements session windows with a 5-minute gap using Flink's `Session` API.
  - Captures session start and end times, IP address, host, and event counts.

- **Kafka Integration:**
  - Configures Kafka as the source with proper watermarking for event-time processing.
  - Handles out-of-order events efficiently using watermarks.

- **Postgres Sink:**
  - Writes sessionized data to a `session_events_sink` table.
  - Aggregates metrics for average events per session and writes to a `session_metrics_sink` table.

- **Scalability and Reliability:**
  - Enables checkpointing for fault tolerance.
  - Configurable parallelism for efficient resource utilization.

## Data Flow

1. **Source:** Kafka topic containing web traffic data in JSON format.
2. **Transformations:**
   - Sessionize events using a 5-minute gap.
   - Compute metrics for average events per session by host.
3. **Sink:** Write sessionized data and metrics to Postgres tables.

## Suggestions for Future Improvements

1. Add more inline comments to improve code maintainability and explain complex transformations.
2. Use dynamic parameterization for session gap time and other configurations.
3. Optimize sessionization logic for larger datasets using partition pruning and stateful processing.
4. Enhance metrics by including session duration, unique URLs per session, or geodata-based insights.
5. Implement retry logic and enhanced logging for better error monitoring and handling.
