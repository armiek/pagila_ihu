#!/usr/bin/env python
# coding: utf-8

# # Setup - Install Libraries

# # Import libraries

# In[1]:


# Import libraries

from google.cloud import bigquery
import pandas as pd
from pandas_gbq import to_gbq
import os
print('Libraries imported successfully')


# In[2]:


# Run the following commands once, in order to install libraries - DO NOT Uncomment this line.

# Uncomment below lines

#!pip3 install --upgrade pip
# !pip install google-cloud-bigquery
# !pip install pandas-gbq -U
# !pip install db-dtypes
# !pip install packaging --upgrade


# In[3]:


get_ipython().system('pip3 install --upgrade pip')


# In[4]:


get_ipython().system('pip3 install google-cloud-bigquery')


# In[5]:


get_ipython().system('pip3 install pandas-gbq -U')


# In[6]:


get_ipython().system('pip3 install db-dtypes')


# In[7]:


get_ipython().system('pip3 install packaging --upgrade')


# In[ ]:





# In[8]:


# Set the environment variable for Google Cloud credentials
# Place the path in which the .json file is located.

# Example (if .json is located in the same directory with the notebook)
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "at-arch-416714-6f9900ec7.json"

# -- YOUR CODE GOES BELOW THIS LINE
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/armela/Desktop/neat-calculus-453319-f5-08fadb59b569.json" # Edit path
# -- YOUR CODE GOES ABOVE THIS LINE


# In[9]:


# Set your Google Cloud project ID and BigQuery dataset details

# -- YOUR CODE GOES BELOW THIS

project_id = 'neat-calculus-453319-f5' # Edit with your project id
dataset_id = 'staging_db' # Modify the necessary schema name: staging_db, reporting_db etc.
table_id = 'stg_payment' # Modify the necessary table name: stg_customer, stg_city etc.

# -- YOUR CODE GOES ABOVE THIS LINE


# # SQL Query

# In[10]:


# Create a BigQuery client
client = bigquery.Client(project=project_id)

# -- YOUR CODE GOES BELOW THIS LINE

# Define your SQL query here
query = """
with base as (
  select *
  from `neat-calculus-453319-f5.pagila_productionpublic.payment` --Your table path
  )

  , final as (
    select
        payment_id
        , customer_id
        , staff_id 
        , rental_id
        , amount as payment_amount
        , payment_date
   FROM base
  )

  select * from final
"""

# -- YOUR CODE GOES ABOVE THIS LINE

# Execute the query and store the result in a dataframe
df = client.query(query).to_dataframe()

# Explore some records
df.head()


# # Write to BigQuery

# In[11]:


# Define the full table ID
full_table_id = f"{project_id}.{dataset_id}.{table_id}"

# -- YOUR CODE GOES BELOW THIS LINE
# Define table schema based on the project description

schema = [
    bigquery.SchemaField('payment_id', 'INTEGER'),
    bigquery.SchemaField('customer_id', 'INTEGER'),
    bigquery.SchemaField('staff_id', 'INTEGER'),
    bigquery.SchemaField('rental_id', 'INTEGER'),
    bigquery.SchemaField('payment_amount', 'NUMERIC'),
    bigquery.SchemaField('payment_date', 'DATETIME'),
    ]

# -- YOUR CODE GOES ABOVE THIS LINE


# In[12]:


# Create a BigQuery client
client = bigquery.Client(project=project_id)

# Check if the table exists
def table_exists(client, full_table_id):
    try:
        client.get_table(full_table_id)
        return True
    except Exception:
        return False

# Write the dataframe to the table (overwrite if it exists, create if it doesn't)

if table_exists(client, full_table_id):
    # If the table exists, overwrite it
    destination_table = f"{dataset_id}.{table_id}"
    # Write the dataframe to the table (overwrite if it exists)
    to_gbq(df, destination_table, project_id=project_id, if_exists='replace')
    print(f"Table {full_table_id} exists. Overwritten.")
else:
    # If the table does not exist, create it
    job_config = bigquery.LoadJobConfig(schema=schema)
    job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
    job.result()  # Wait for the job to complete
    print(f"Table {full_table_id} did not exist. Created and data loaded.")


# In[13]:


# Below line converts your i.pynb file to .py python executable file. Modify the input and output names based
# on the table you are processing.
# Example:
# ! jupyter nbconvert stg_customer.ipynb --to python

# -- YOUR CODE GOES BELOW THIS LINE

get_ipython().system('python3 -m jupyter nbconvert stg_actor.ipynb --to python')

# -- YOUR CODE GOES ABOVE THIS LINE


# In[14]:


get_ipython().system('python3 -m pip install nbconvert')


# In[15]:


get_ipython().system('python3 -m pip install nbconvert -U')


# In[ ]:





# In[ ]:




