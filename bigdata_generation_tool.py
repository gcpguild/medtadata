#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Bigdata framework for generation of metadata in Cloudera into AWS, Azure, GCP and other clound platform.
Date : May 12, 2023
"""
_author__ = "Ramamurthy Valavandan"
__copyright__ = "Copyright (c) 2023 Nature Labs, Namakkal. All Rights Reserved."
__credits__ = ["Balakrishnan Gothandapani", "Kanagalakshmi", "Valavandan Pillai", "Savitha Ramamuthy", "Bharani",  "Dharani" ]
__license__ = "Apache"
__version__ = "1.0"
__maintainer__ = "Balakrishnan Gothandapani"
__email__ = "find@ngosys.com"
__status__ = "Production"

"""
Copyright [2023] [NatureLabs]

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for specific language governing permissions and
   limitations under the License.

"""
from pyspark.sql import SparkSession
import pandas as pd

# Create Spark session
spark = SparkSession.builder \
    .appName("Metadata Auto Generation") \
    .master("local[2]") \
    .config("spark.executor.memory", "20g") \
    .getOrCreate()

# Define file paths
df1_job_master_lst = "job_master_data.csv"
df2_job_column_lst = "jobs_cloumn_list.csv"
df3_job_linked_lst = "linked_job_name_data.csv"

# Read CSV files as dataframes
df1 = spark.read.format("csv").option("header", "true").load(df1_job_master_lst)
df2 = spark.read.format("csv").option("header", "true").load(df2_job_column_lst)
df3 = spark.read.format("csv").option("header", "true").load(df3_job_linked_lst)

# Join dataframes
df_merged = df1.join(df2, "job_name", "left").join(df3, "job_name", "left")

# Select columns of interest
selected_columns = ['job_name', 'is_transformation', 'where_filter_apply', 'column_name', 'job_link_name', 'load_date', 'partition_id']0 

df_selected = df_merged.select(*selected_columns)

# Order by job_name in ascending order
df_ordered = df_selected.orderBy('job_name')

# Convert to Pandas dataframe
df_pd = df_ordered.toPandas()

# Save dataframe to CSV
df_pd.to_csv("metadata_cdsc.csv", index=False)

# Stop the Spark session
spark.stop()
