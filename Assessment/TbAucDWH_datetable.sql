-- Databricks notebook source
-- MAGIC  %md
-- MAGIC ## Lakehouse Calendar Dimension for Delta Live Tables (DLT)
-- MAGIC
-- MAGIC This notebook creates a calendar dimension (Also known as date dimension) for the lakehouse. It is intended to be executed within a Delta Live Tables (DLT) pipeline, and defaults to loading data using a rolling 5 year period.
-- MAGIC  
-- MAGIC ### Directions
-- MAGIC
-- MAGIC  - Modify the date range as necessary by updating the dates CTE.
-- MAGIC  - Add/modify/remove columns as necessary.
-- MAGIC  - Add to Delta Live Tables (DLT) Pipeline
-- MAGIC  
-- MAGIC ### References
-- MAGIC - [Five Simple Steps for Implementing a Star Schema in Databricks With Delta Lake](https://www.databricks.com/blog/2022/05/20/five-simple-steps-for-implementing-a-star-schema-in-databricks-with-delta-lake.html)
-- MAGIC  - [Datetime Patterns for Formatting and Parsing](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html)
-- MAGIC

-- COMMAND ----------

use default

-- COMMAND ----------

create or replace table dim_calendar
using delta
comment "Calendar dimension for the gold layer"
tblproperties ( "TbAucPipeline.quality" = "gold")
location "/mnt/lifeline-ukraine/dgold/calendar"
as
--Set the date range in the dates CTE below
with dates as (
  SELECT explode(sequence(
    date_trunc('year', current_date() - interval '2' year), -- Start of the year, two years ago
    date_trunc('year', current_date() + interval '2' year) - interval '1' day, -- Last day of next year
    interval '1' day
  )) AS calendar_date
)
select
  year(calendar_date) * 10000 + month(calendar_date) * 100 + day(calendar_date) as date_int,
  cast(calendar_date as date),
  year(calendar_date) AS calendar_year,
  date_format(calendar_date, 'MMMM') as calendar_month,
  month(calendar_date) as month_of_year,
  date_format(calendar_date, 'EEEE') as calendar_day,
  dayofweek(calendar_date) AS day_of_week,
  weekday(calendar_date) + 1 as day_of_week_start_monday,
  case
    when weekday(calendar_date) < 5 then 'Y'
    else 'N'
  end as is_week_day,
  dayofmonth(calendar_date) as day_of_month,
  case
    when calendar_date = last_day(calendar_date) then 'Y'
    else 'N'
  end as is_last_day_of_month,
  dayofyear(calendar_date) as day_of_year,
  weekofyear(calendar_date) as week_of_year_iso,
  quarter(calendar_date) as quarter_of_year
from
  dates
order by
  calendar_date

-- COMMAND ----------

optimize dim_calendar zorder by (calendar_date)

-- COMMAND ----------

vacuum dim_calendar
