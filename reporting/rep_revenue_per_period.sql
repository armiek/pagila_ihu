## CREATE rep_revenue_per_period - reporting table 1

CREATE TABLE IF NOT EXISTS `neat-calculus-453319-f5.reporting_db.rep_revenue_per_period` (
  reporting_period STRING,     -- 'Day', 'Month', 'Year'
  reporting_date DATE,         -- The specific date of the period
  total_revenue NUMERIC        -- Revenue for the period (0 if no revenue)
);

-- Step 2: Populate the table

INSERT INTO `neat-calculus-453319-f5.reporting_db.rep_revenue_per_period`
(reporting_period, reporting_date, total_revenue)

WITH processed_dates AS (
  SELECT 'Day' AS reporting_period, DATE_TRUNC(date_column, DAY) AS reporting_date
  ,FROM `neat-calculus-453319-f5.reporting_db.all_dates`
  GROUP BY 1, 2

  UNION ALL

  SELECT 'Week' AS reporting_period, DATE_TRUNC(date_column, WEEK) AS reporting_date
  ,FROM `neat-calculus-453319-f5.reporting_db.all_dates`
  GROUP BY 1, 2

  UNION ALL

  SELECT 'Month' AS reporting_period, DATE_TRUNC(date_column, MONTH) AS reporting_date
  ,FROM `neat-calculus-453319-f5.reporting_db.all_dates`
  GROUP BY 1, 2

  UNION ALL

  SELECT 'Quarter' AS reporting_period, DATE_TRUNC(date_column, QUARTER) AS reporting_date
  ,FROM `neat-calculus-453319-f5.reporting_db.all_dates`
  GROUP BY 1, 2

  UNION ALL

  SELECT 'Year' AS reporting_period, DATE_TRUNC(date_column, YEAR) AS reporting_date
  ,FROM `neat-calculus-453319-f5.reporting_db.all_dates`
  GROUP BY 1, 2
),

payments_data AS (
  SELECT
    p.payment_date,
    p.payment_amount
  FROM `neat-calculus-453319-f5.staging_db.stg_payment` p
  JOIN `neat-calculus-453319-f5.staging_db.stg_rental` r ON p.rental_id = r.rental_id
  JOIN `neat-calculus-453319-f5.staging_db.stg_inventory` i ON r.inventory_id = i.inventory_id
  JOIN `neat-calculus-453319-f5.staging_db.stg_film` f ON i.film_id = f.film_id
  WHERE f.film_title != 'GOODFELLAS SALUTE'
),

revenue_per_period AS (
  SELECT
    'Day' AS reporting_period,
    DATE_TRUNC(payment_date, DAY) AS reporting_date,
    SUM(payment_amount) AS total_revenue
  FROM payments_data
  GROUP BY 1, 2

  UNION ALL

  SELECT
    'Week' AS reporting_period,
    DATE_TRUNC(payment_date, WEEK) AS reporting_date,
    SUM(payment_amount) AS total_revenue
  FROM payments_data
  GROUP BY 1, 2

  UNION ALL

  SELECT
    'Month' AS reporting_period,
    DATE_TRUNC(payment_date, MONTH) AS reporting_date,
    SUM(payment_amount) AS total_revenue
  FROM payments_data
  GROUP BY 1, 2

  UNION ALL

  SELECT
    'Quarter' AS reporting_period,
    DATE_TRUNC(payment_date, QUARTER) AS reporting_date,
    SUM(payment_amount) AS total_revenue
  FROM payments_data
  GROUP BY 1, 2

  UNION ALL

  SELECT
    'Year' AS reporting_period,
    DATE_TRUNC(payment_date, YEAR) AS reporting_date,
    SUM(payment_amount) AS total_revenue
  FROM payments_data
  GROUP BY 1, 2
)

SELECT
  d.reporting_period,
  d.reporting_date,
  IFNULL(r.total_revenue, 0) AS total_revenue
FROM processed_dates d
LEFT JOIN revenue_per_period r
  ON d.reporting_period = r.reporting_period
  AND d.reporting_date = r.reporting_date
WHERE d.reporting_date <= CURRENT_DATE

