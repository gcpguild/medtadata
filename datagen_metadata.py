import pandas as pd
import numpy as np
import hashlib
import random
import datetime

N_ROWS = 100  # Define the number of rows to generate

def generate_random_data(col_name):
    if col_name == 'job_name':
        return [f'{col_name}_{i}' for i in range(N_ROWS)]
    elif col_name == 'is_transformation':
        return random.choices(['True', 'False'], k=N_ROWS)
    elif col_name == 'where_filter_apply':
        return [f'filter_{i}' for i in range(N_ROWS)]
    elif col_name == 'column_name':
        return [[f'column_{j}' for j in range(3)] for _ in range(N_ROWS)]
    elif col_name == 'job_link_name':
        return [[f'linked_job_{j}' for j in range(2)] for _ in range(N_ROWS)]
    elif col_name == 'load_date':
        return [datetime.date.today() - datetime.timedelta(days=random.randint(1, 100)) for _ in range(N_ROWS)]
    else:
        return []  # Return an empty list for unknown column types


def generate_data_frame():
    column_lst = ['job_name', 'is_transformation', 'where_filter_apply', 'column_name', 'job_link_name', 'load_date']
    data = {}
    for col_name in column_lst:
        data[col_name] = generate_random_data(col_name)
    data['partition_id'] = [datetime.datetime.now() for _ in range(N_ROWS)]
    df = pd.DataFrame(data)
    return df

# Generate the data frame
df = generate_data_frame()

# Save the data frame as a CSV file
df.to_csv('metadata_generated.csv', index=False)
