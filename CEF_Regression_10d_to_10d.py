import sqlite3
import numpy as np
import pandas as pd

#dependent variable df in this dicitonary of dfs 'dfs'
from import_and_clean_db_to_df import dfs
for name, df in dfs.items():
    print(f"Table: {name}")
    print(df.columns.tolist())
    print("-" * 20)

#ETL independent variable
conn_ALT = sqlite3.connect('DATA_database.db')

# 1. Get all table names
query = "SELECT name FROM sqlite_master WHERE type='table';"
table_names = pd.read_sql_query(query, conn_ALT)['name'].tolist()

# 2. Store in a dictionary {name: dataframe}
all_tables = {name: pd.read_sql_query(f"SELECT * FROM {name} LIMIT 1", conn_ALT) for name in table_names}

# 3. Print the headers using the loop above
for name, df in all_tables.items():
    print(f"Header for {name}: {df.columns.values}")

## Bring in independent variables
dfs_x = {table: pd.read_sql_query(f"SELECT * FROM {table}", conn_ALT, parse_dates=['observation_date']) for table in all_tables}
#print(dfs_x)

# 1. Get the key of the first DataFrame
first_key = next(iter(dfs))

# 2. Extract the date column
# We use .copy() to avoid SettingWithCopy warnings later
date_col = dfs[first_key]['date'].copy()
date_col = pd.to_datetime(date_col)

# 3. Loop through the dictionary and assign the column to all DataFrames
#for key in dfs:
#    dfs[key]['date'] = date_col

X_vars = pd.DataFrame(date_col)
print(X_vars)

for name, df in dfs_x.items():
    if name == 'treasury_rate_1yr_table':
        ##df['column'].diff() equates to the forward term minus current term --> positive = rising rate
        df['1yr_10d_difference'] = df['DGS1'].diff(periods=10)
        df.rename(columns={'observation_date': 'date'}, inplace=True)
        df.dropna(inplace=True)
        df['date'] = pd.to_datetime(df['date'])

        #append to X_vars
        X_vars = pd.merge(X_vars, df[['date','1yr_10d_difference']],on='date', how= 'left')


    #create and clean additional x_variables

print(X_vars)





conn_ALT.close()