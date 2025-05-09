#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from google.cloud import bigquery

# Initialize the BigQuery client with your project
client = bigquery.Client(project='neat-calculus-453319-f5')  # Replace with your project ID

# Define your SQL query
sql_query = """
-- Create table if it doesn't exist
CREATE TABLE IF NOT EXISTS `neat-calculus-453319-f5.reporting_db.rep_revenue_per_period` (
  reporting_period STRING,
  reporting_date DATE,
  total_revenue NUMERIC
);

-- Insert aggregated revenue per period from 2015 to current
INSERT INTO `neat-calculus-453319-f5.reporting_db.rep_revenue_per_period`
(reporting_period, reporting_date, total_revenue)

WITH payments AS (
  SELECT *
  FROM `neat-calculus-453319-f5.staging_db.stg_payment`
),

rentals AS (
  SELECT *
  FROM `neat-calculus-453319-f5.staging_db.stg_rental`
),

inventory AS (
  SELECT *
  FROM `neat-calculus-453319-f5.staging_db.stg_inventory`
),

films AS (
  SELECT *
  FROM `neat-calculus-453319-f5.staging_db.stg_film`
),

-- Filter valid payments excluding GOODFELLAS SALUTE
valid_payments AS (
  SELECT
    p.payment_amount,
    p.payment_date
  FROM payments p
  JOIN rentals r ON p.rental_id = r.rental_id
  JOIN inventory i ON r.inventory_id = i.inventory_id
  JOIN films f ON i.film_id = f.film_id
  WHERE f.film_title != 'GOODFELLAS SALUTE'
),

-- Revenue aggregates
revenue_by_period AS (
  SELECT 'Day' AS reporting_period, DATE_TRUNC(payment_date, DAY) AS reporting_date, SUM(payment_amount) AS total_revenue
  FROM valid_payments
  GROUP BY 1, 2

  UNION ALL

  SELECT 'Month', DATE_TRUNC(payment_date, MONTH), SUM(payment_amount)
  FROM valid_payments
  GROUP BY 1, 2

  UNION ALL

  SELECT 'Year', DATE_TRUNC(payment_date, YEAR), SUM(payment_amount)
  FROM valid_payments
  GROUP BY 1, 2
),

-- Join with full period calendar to include all from 2015
final AS (
  SELECT
    rpt.reporting_period,
    rpt.reporting_date,
    COALESCE(rbp.total_revenue, 0) AS total_revenue
  FROM `neat-calculus-453319-f5.reporting_db.reporting_periods_table` rpt
  LEFT JOIN revenue_by_period rbp
    ON rpt.reporting_period = rbp.reporting_period
   AND rpt.reporting_date = rbp.reporting_date
  WHERE rpt.reporting_date >= '2015-01-01'
)

-- Final insert
SELECT * 
FROM neat-calculus-453319-f5.reporting_db.rep_revenue_per_period
"""

# Run the query
query_job = client.query(sql_query)

# Wait for the query to finish
query_job.result()

# Print success message
print("✅ Table 'rep_revenue_per_period' created and data inserted successfully!")

