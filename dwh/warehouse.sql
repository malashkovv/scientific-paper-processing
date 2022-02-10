CREATE DATABASE warehouse;

CREATE TABLE warehouse.paper_details (
  title STRING,
  source_id STRING,
  abstract STRING,
  category STRING,
  doi STRING,
  venue STRING,
  posted DATE,
  created_at TIMESTAMP,
  authors ARRAY<STRING>,
  created_date STRING,
  topic STRING
)
USING DELTA
-- PARTITIONED BY (topic, created_date)
LOCATION 's3a://dwh/data/papers_details';