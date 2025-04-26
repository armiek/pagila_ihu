#!/usr/bin/env python
# coding: utf-8

# In[17]:


from google.cloud import bigquery


# In[20]:


# Import libraries

from google.cloud import bigquery
import pandas as pd
from pandas_gbq import to_gbq
import os
#print('Libraries imported successfully')


# In[34]:


# Initialize BigQuery client
client = bigquery.Client()


# In[1]:


#!pip3 install --upgrade pip
# !pip install google-cloud-bigquery
# !pip install pandas-gbq -U
# !pip install db-dtypes
# !pip install packaging --upgrade


# Set the environment variable for Google Cloud credentials
# Place the path in which the .json file is located.

# Example (if .json is located in the same directory with the notebook)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "at-arch-416714-6f9900ec7.json"

# -- YOUR CODE GOES BELOW THIS LINE
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/armela/Desktop/neat-calculus-453319-f5-08fadb59b569.json" # Edit path
# -- YOUR CODE GOES ABOVE THIS LINE


# In[35]:


## Define your project and dataset

project_id = 'neat-calculus-453319-f5'
dataset_id = 'reporting_db'
table_id = 'rep_revenue_per_customer_and_period'


# In[41]:


from google.cloud import bigquery

# Initialize BigQuery client
client = bigquery.Client()

# Write the query

create_or_replace_query = """
CREATE OR REPLACE TABLE `neat-calculus-453319-f5.reporting_db.rep_revenue_per_customer_and_period` AS

WITH valid_payments AS (
  SELECT
    p.payment_amount,
    p.payment_date,
    r.customer_id
  FROM `neat-calculus-453319-f5.staging_db.stg_payment` p
  JOIN `neat-calculus-453319-f5.staging_db.stg_rental` r
    ON p.rental_id = r.rental_id
  JOIN `neat-calculus-453319-f5.staging_db.stg_inventory` i
    ON r.inventory_id = i.inventory_id
  JOIN `neat-calculus-453319-f5.staging_db.stg_film` f
    ON i.film_id = f.film_id
  WHERE f.film_title != 'GOODFELLAS SALUTE'
)

SELECT 
  customer_id,
  'Day' AS reporting_period,
  DATE_TRUNC(payment_date, DAY) AS reporting_date,
  SUM(payment_amount) AS total_revenue
FROM valid_payments
WHERE payment_date >= '2015-01-01'
GROUP BY customer_id, DATE_TRUNC(payment_date, DAY)

UNION ALL

SELECT 
  customer_id,
  'Month' AS reporting_period,
  DATE_TRUNC(payment_date, MONTH) AS reporting_date,
  SUM(payment_amount) AS total_revenue
FROM valid_payments
WHERE payment_date >= '2015-01-01'
GROUP BY customer_id, DATE_TRUNC(payment_date, MONTH)

UNION ALL

SELECT 
  customer_id,
  'Year' AS reporting_period,
  DATE_TRUNC(payment_date, YEAR) AS reporting_date,
  SUM(payment_amount) AS total_revenue
FROM valid_payments
WHERE payment_date >= '2015-01-01'
GROUP BY customer_id, DATE_TRUNC(payment_date, YEAR)
"""

# Execute the query

query_job = client.query(create_or_replace_query)  
query_job.result()  




# In[ ]:




