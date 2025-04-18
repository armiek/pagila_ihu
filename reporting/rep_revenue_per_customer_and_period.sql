## CREATE rep_revenue_per_customer_and_period - reporting table 2


-- Step 1: Create the reporting table if not exists

CREATE TABLE IF NOT EXISTS `neat-calculus-453319-f5.reporting_db.rep_revenue_per_customer_and_period` (
  reporting_period STRING,
  reporting_date DATE,
  customer_id INT64,
  total_revenue NUMERIC
);

-- Step 2: Insert revenue by customer and period

INSERT INTO `neat-calculus-453319-f5.reporting_db.rep_revenue_per_customer_and_period`
(reporting_period, reporting_date, customer_id, total_revenue)

WITH rentals AS (
  SELECT *
  FROM `neat-calculus-453319-f5.staging_db.stg_rental`
),

customers AS (
  SELECT *
  FROM `neat-calculus-453319-f5.staging_db.stg_customer`
),

payments AS (
  SELECT *
  FROM `neat-calculus-453319-f5.staging_db.stg_payment`
),

films AS (
  SELECT *
  FROM `neat-calculus-453319-f5.staging_db.stg_film`
),

inventory AS (
  SELECT *
  FROM `neat-calculus-453319-f5.staging_db.stg_inventory`
),

reporting_dates AS (
  SELECT *
  FROM `neat-calculus-453319-f5.reporting_db.reporting_periods_table`
  WHERE reporting_period IN ('Day', 'Month', 'Year')
),

-- Join all relevant payment info (exclude GOODFELLAS SALUTE)
payments_joined AS (
  SELECT
    r.customer_id,
    p.payment_amount,
    p.payment_date
  FROM payments p
  JOIN rentals r ON p.rental_id = r.rental_id
  JOIN inventory i ON r.inventory_id = i.inventory_id
  JOIN films f ON i.film_id = f.film_id
  WHERE f.film_title != 'GOODFELLAS SALUTE'
),

-- Aggregate revenue per customer per period
revenue_per_period AS (
  SELECT 'Day' AS reporting_period,
         DATE_TRUNC(payment_date, DAY) AS reporting_date,
         customer_id,
         SUM(payment_amount) AS total_revenue
  FROM payments_joined
  GROUP BY 1, 2, 3

  UNION ALL

  SELECT 'Month',
         DATE_TRUNC(payment_date, MONTH),
         customer_id,
         SUM(payment_amount)
  FROM payments_joined
  GROUP BY 1, 2, 3

  UNION ALL

  SELECT 'Year',
         DATE_TRUNC(payment_date, YEAR),
         customer_id,
         SUM(payment_amount)
  FROM payments_joined
  GROUP BY 1, 2, 3
),

-- Final join with available reporting dates (optional but matches your style)
final AS (
  SELECT
    rd.reporting_period,
    rd.reporting_date,
    rp.customer_id,
    rp.total_revenue
  FROM reporting_dates rd
  INNER JOIN revenue_per_period rp
    ON rd.reporting_period = rp.reporting_period
   AND rd.reporting_date = rp.reporting_date
)

-- Only include where there was actual revenue
SELECT *
FROM final
WHERE total_revenue > 0;
