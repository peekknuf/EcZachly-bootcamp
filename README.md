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

