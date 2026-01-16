# ceu-modern-data-platforms
Data Engineering 2 - Modern Data Platforms. dbt, Snowflake, Databricks, Apache Spark

## Installation

Databricks Setup:
1) Sign up for Databricks Free Edition: https://www.databricks.com/learn/free-edition

Snowflake Setup:

2) Register to Snowflake: https://signup.snowflake.com/?trial=student&cloud=aws&region=us-west-2
3) Set up Snowflake tables: https://dbtsetup.nordquant.com/

dbt Setup

4) Fork this repo and clone it to your PC: https://github.com/nordquant/dbt-student-repo
5) Ensure you have a compatible Python Version: https://docs.getdbt.com/faqs/Core/install-python-compatibility (if you don't, install Python 3.13) 
6) Install uv: https://docs.astral.sh/uv/getting-started/installation/
7) Install packages: `uv sync`
8) Activate the virtualenv
- Windows (PowerShell): `.\.venv\Scripts\Activate.ps1`
- Windows (CMD): `.venv\Scripts\activate.bat`
- macOS / Linux: `. .venv/bin/activate`

## Starting a dbt Project
Create a dbt project (all platforms):
```sh
dbt init --skip-profile-setup airbnb
```
Once done, drag and drop the `profiles.yml` file you downloaded to the `airbnb` folder.

Try if dbt works:
```sh
dbt debug
```

### Clean Up Example Files
From within the `airbnb` folder, remove the example models that dbt created by default:
```sh
rm -rf models/example
```
Also remove the example model configuration from `dbt_project.yml`. Delete these lines at the end of the file:
```yaml
models:
  airbnb:
    # Config indicated by + and applies to all files under models/example/
    example:
      +materialized: view
```

# Data Exploration
Solutions:
```sql
USE AIRBNB.RAW;

SELECT * FROM RAW_LISTINGS LIMIT 10;
SELECT * FROM RAW_HOSTS LIMIT 10;
SELECT * FROM RAW_REVIEWS LIMIT 10;

SELECT ROOM_TYPE, COUNT(*) as NUM_RECORDS FROM RAW_LISTINGS GROUP BY ROOM_TYPE ORDER BY ROOM_TYPE;

SELECT MIN(MINIMUM_NIGHTS), MAX(MINIMUM_NIGHTS) FROM RAW_LISTINGS;
SELECT COUNT(*) FROM RAW_LISTINGS WHERE MINIMUM_NIGHTS = 0;

SELECT MIN(PRICE), MAX(PRICE) FROM RAW_LISTINGS;

SELECT sentiment, COUNT(*) as NUM_RECORDS FROM RAW_REVIEWS WHERE sentiment IS NOT NULL GROUP BY sentiment;

SELECT SUM(CASE WHEN IS_SUPERHOST='t' THEN 1 ELSE 0 END)/SUM(1)* 100 as SUPERHOST_PERCENT FROM RAW_HOSTS;

SELECT r.* FROM RAW_REVIEWS r LEFT JOIN RAW_LISTINGS l ON (r.listing_id = l.id) WHERE l.id IS NULL;
```

# Models
## Code used in the lesson

### SRC Listings
`models/src/src_listings.sql`:

```sql
WITH raw_listings AS (
    SELECT
        *
    FROM
        AIRBNB.RAW.RAW_LISTINGS
)
SELECT
    id AS listing_id,
    name AS listing_name,
    listing_url,
    room_type,
    minimum_nights,
    host_id,
    price AS price_str,
    created_at,
    updated_at
FROM
    raw_listings

```

## Exercise: SRC Reviews

Create a model which builds on top of our `raw_reviews` table.

1) Call the model `models/src/src_reviews.sql`
2) Use a CTE (common table expression) to define an alias called `raw_reviews`. This CTE selects every column from the raw reviews table `AIRBNB.RAW.RAW_REVIEWS`
3) In your final `SELECT`, select every column and record from `raw_reviews` and rename the following columns:
   * `date` to `review_date`
   * `comments` to `review_text`
   * `sentiment` to `review_sentiment`

### Solution

```sql
WITH raw_reviews AS (
    SELECT
        *
    FROM
        AIRBNB.RAW.RAW_REVIEWS
)
SELECT
    listing_id,
    date AS review_date,
    reviewer_name,
    comments AS review_text,
    sentiment AS review_sentiment
FROM
    raw_reviews
```


## Exercise: SRC Hosts

Create a model which builds on top of our `raw_hosts` table.

1) Call the model `models/src/src_hosts.sql`
2) Use a CTE (common table expression) to define an alias called `raw_hosts`. This CTE selects every column from the raw hosts table `AIRBNB.RAW.RAW_HOSTS`
3) In your final `SELECT`, select every column and record from `raw_hosts` and rename the following columns:
   * `id` to `host_id`
   * `name` to `host_name`

### Solution

```sql
WITH raw_hosts AS (
    SELECT
        *
    FROM
        AIRBNB.RAW.RAW_HOSTS
)
SELECT
    id AS host_id,
    NAME AS host_name,
    is_superhost,
    created_at,
    updated_at
FROM
    raw_hosts
```


# Sources

Create a new file called `models/sources.yml`.
Add the `listings` source that points to the `raw_listings` table in the `raw` schema:

```yaml
sources:
  - name: airbnb
    schema: raw
    tables:
      - name: listings
        identifier: raw_listings
```

## Exercise: Add Hosts and Reviews Sources

Add the `hosts` and `reviews` sources to your `models/sources.yml` file.
Both should point to their respective raw tables (`raw_hosts` and `raw_reviews`) in the `raw` schema.

### Solution

```yaml
sources:
  - name: airbnb
    schema: raw
    tables:
      - name: listings
        identifier: raw_listings

      - name: hosts
        identifier: raw_hosts

      - name: reviews
        identifier: raw_reviews
```

# Incremental Models

The `models/fct/fct_reviews.sql` model:
```sql
{{
  config(
    materialized = 'incremental',
    on_schema_change='fail'
    )
}}
WITH src_reviews AS (
  SELECT * FROM {{ ref('src_reviews') }}
)
SELECT * FROM src_reviews
WHERE review_text is not null

{% if is_incremental() %}
  AND review_date > (select max(review_date) from {{ this }})
{% endif %}
```

Run the model:
```sh
dbt run --select fct_reviews
```


Get every review for listing _3176_ (In Snowflake):
```sql
SELECT * FROM "AIRBNB"."DEV"."FCT_REVIEWS" WHERE listing_id=3176;
```

Add a new record to the table (In Snowflake):
```sql
INSERT INTO "AIRBNB"."RAW"."RAW_REVIEWS"
VALUES (3176, CURRENT_TIMESTAMP(), 'Zoltan', 'excellent stay!', 'positive');
```

Only add the new record:
```sh
dbt run
```

Or make a full-refresh:
```sh
dbt run --full-refresh
```

# Source Freshness Testing

Add freshness configuration to the `reviews` source in `models/sources.yml`:

```yaml
      - name: reviews
        identifier: raw_reviews
        config:
          loaded_at_field: date
          freshness:
            warn_after: {count: 1, period: day}
```

Check source freshness:
```sh
dbt source freshness
```

# Cleansed Models

### DIM Listings Cleansed
`models/dim/dim_listings_cleansed.sql`:

```sql
WITH src_listings AS (
    SELECT * FROM {{ ref('src_listings') }}
)
SELECT
  listing_id,
  listing_name,
  room_type,
  CASE
    WHEN minimum_nights = 0 THEN 1
    ELSE minimum_nights
  END AS minimum_nights,
  host_id,
  REPLACE(
    price_str,
    '$'
  ) :: NUMBER(
    10,
    2
  ) AS price,
  created_at,
  updated_at
FROM
  src_listings
```

## Exercise: DIM Hosts Cleansed

Create a new model in the `models/dim/` folder called `dim_hosts_cleansed.sql`.
Use a CTE to reference the `src_hosts` model.
SELECT every column and every record, and add a cleansing step to `host_name`:
- If `host_name` is not null, keep the original value
- If `host_name` is null, replace it with the value `'Anonymous'`
- Use the `NVL(column_name, default_null_value)` function

### Solution

```sql
WITH src_hosts AS (
    SELECT
        *
    FROM
        {{ ref('src_hosts') }}
)
SELECT
    host_id,
    NVL(
        host_name,
        'Anonymous'
    ) AS host_name,
    is_superhost,
    created_at,
    updated_at
FROM
    src_hosts
```

## Exercise: DIM Listings with Hosts

Create a new model in the `models/dim/` folder called `dim_listings_w_hosts.sql`.
Join `dim_listings_cleansed` with `dim_hosts_cleansed` to create a denormalized view that includes host information alongside listing data.
- Use a LEFT JOIN on `host_id`
- Include all listing fields plus `host_name` and `is_superhost` (renamed to `host_is_superhost`)
- For `updated_at`, use the `GREATEST()` function to get the most recent update from either table

### Solution

```sql
WITH
l AS (
    SELECT
        *
    FROM
        {{ ref('dim_listings_cleansed') }}
),
h AS (
    SELECT *
    FROM {{ ref('dim_hosts_cleansed') }}
)

SELECT
    l.listing_id,
    l.listing_name,
    l.room_type,
    l.minimum_nights,
    l.price,
    l.host_id,
    h.host_name,
    h.is_superhost as host_is_superhost,
    l.created_at,
    GREATEST(l.updated_at, h.updated_at) as updated_at
FROM l
LEFT JOIN h ON (h.host_id = l.host_id)
```

# Materializations

## Project-level Materialization

Set `src` models to `ephemeral` and `dim` models to `view` in `dbt_project.yml`:

```yaml
models:
  airbnb:
    src:
      +materialized: ephemeral
    dim:
      +materialized: view # This is default, but let's make it explicit 
```

After setting ephemeral materialization, drop the existing src views in Snowflake:
```sql
DROP VIEW AIRBNB.DEV.SRC_HOSTS;
DROP VIEW AIRBNB.DEV.SRC_LISTINGS;
DROP VIEW AIRBNB.DEV.SRC_REVIEWS;
```

## Model-level Materialization

Set `dim_listings_w_hosts` to `table` materialization by adding a config block to the model:

`models/dim/dim_listings_w_hosts.sql`:
```sql
{{
  config(
    materialized = 'table'
  )
}}
WITH
l AS (
...
```

