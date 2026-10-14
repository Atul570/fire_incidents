-- Databricks notebook source
CREATE OR REPLACE TABLE fire_incident.gold.response_time_by_neighborhood
USING DELTA
COMMENT 'Monthly response time analysis by neighborhood - Gold layer aggregated table'
AS
with t1 as (
  SELECT
    incident_number,
    neighborhood_district,
    incident_date,
    alarm_dttm,
    arrival_dttm,
    round(
      (unix_timestamp(arrival_dttm) - unix_timestamp(alarm_dttm)) / 60,
      2
    ) as response_time_in_minutes
  FROM
    fire_incident.silver.fire_incidents
  where
    arrival_dttm is not null and
    alarm_dttm is not null and
    neighborhood_district is not null and
    arrival_dttm >= alarm_dttm
)
select
  neighborhood_district,
  year(incident_date) as incident_year,
  month(incident_date) as incident_month,
  max(response_time_in_minutes) as max_response_time,
  min(response_time_in_minutes) as min_response_time,
  round(avg(response_time_in_minutes),2) as average_response_time,
  round(percentile(response_time_in_minutes,0.5),2) as median_response_time,
  DATE_TRUNC('MONTH', incident_date) AS month_start_date,
  sum(case when response_time_in_minutes > 10 then 1 else 0 end) as incident_over_10_min,
  round((100 * sum(case when response_time_in_minutes > 10 then 1 else 0 end))/count(*),2) as pct_over_10_min,
  count(*) as total_record
from
  t1
group by neighborhood_district,year(incident_date),month(incident_date),DATE_TRUNC('MONTH', incident_date)
ORDER BY incident_year DESC, incident_month DESC, average_response_time DESC ;

-- COMMAND ----------

-- Top 10 neighborhoods with slowest average response time (last 12 months)
select
  neighborhood_district,
  round(avg(average_response_time),2) as avg_res_time,
  ROUND(AVG(median_response_time), 2) AS median_response_time,
  SUM(total_record) AS total_incidents,
  ROUND(AVG(pct_over_10_min), 2) AS avg_pct_over_sla
from
  fire_incident.gold.response_time_by_neighborhood
where 
    month_start_date >= add_months(current_date(),-12)
group by neighborhood_district 
    having sum(total_record) > 100
order by avg_res_time desc
limit 10;

-- COMMAND ----------

--Query 2: Is response time getting BETTER or WORSE over time?
-- Year-over-year trend (last 5 years)
WITH YEARLY_DATA AS (
  SELECT
    neighborhood_district,
    incident_year,
    ROUND(AVG(average_response_time),2) as avg_response_min,
    lag(ROUND(AVG(average_response_time),2),1)  over (partition by neighborhood_district order by incident_year) as prev_year_avg_response_min
  FROM
    fire_incident.gold.response_time_by_neighborhood
  GROUP BY
    neighborhood_district,
    incident_year
)
select 
    neighborhood_district,
    incident_year,
    avg_response_min,
    prev_year_avg_response_min,
    round(avg_response_min - prev_year_avg_response_min,2) as yoy,
    round((100*(avg_response_min - prev_year_avg_response_min ))/prev_year_avg_response_min,2) as yoy_pct,
    case
    when round(avg_response_min - prev_year_avg_response_min,2) > 0 THEN '📈 Slower (Worse)'
    when round(avg_response_min - prev_year_avg_response_min,2) < 0  THEN '📉 Faster (Better)'  
    ELSE '➡️ No Change'
    end as trend  
from yearly_data;

-- COMMAND ----------

WITH xyz AS (
  SELECT
    neighborhood_district,
    incident_year,
    ROUND(AVG(average_response_time),2) as avg
  FROM
    fire_incident.gold.response_time_by_neighborhood
  GROUP BY
    neighborhood_district,
    incident_year
)
SELECT * FROM XYZ
        PIVOT (
        MAX(AVG)
        FOR incident_year IN (2019,2020,2021,2022,2023,2024,2025,2026)
    );

-- COMMAND ----------

