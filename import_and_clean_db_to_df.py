import sqlite3
import numpy as np
import pandas as pd

conn_CEF = sqlite3.connect('CEF_database.db')
conn_ALT = sqlite3.connect('DATA_database.db')
conn_ETF = sqlite3.connect('ETF_database.db')


# 2. Get a list of all tables in the database
query_tables = "SELECT name FROM sqlite_master WHERE type='table';"
tables = pd.read_sql_query(query_tables, conn_CEF)['name'].tolist()

results = []

# 3. Iterate through each table to find the earliest date
for table in tables:
    try:

        query_min = f"SELECT date as earliest_date FROM {table}"
        min_date = pd.read_sql_query(query_min, conn_CEF, parse_dates = ['date']).iloc[0]['earliest_date']
        max_date = pd.read_sql_query(query_min, conn_CEF, parse_dates = ['date']).iloc[-1]['earliest_date']
        min_date = pd.to_datetime(min_date, format='%m/%d/%Y')
        max_date = pd.to_datetime(max_date, format='%m/%d/%Y')

        results.append({'table_name': table, 'earliest_date': min_date, 'latest_date' : max_date})
       
    except Exception as e:
        # If a table doesn't have the date column, skip it or log the error
        results.append({'table_name': table, 'earliest_date': 'Column not found', 'latest_date' : 'Column not found'})

# 4. Display results as a new DataFrame
earliest_dates_df = pd.DataFrame(results)
#print(earliest_dates_df)

########################################################
# Select Tables based on a desired date and create df of relevant data for selected tables
########################################################

target_date_0 = pd.to_datetime('2006-03-01')
target_date_1 = pd.to_datetime('2026-03-01')

viable_CEFs_0 = earliest_dates_df.loc[earliest_dates_df['earliest_date']< target_date_0, 'table_name']
viable_CEFs_1 = earliest_dates_df.loc[earliest_dates_df['latest_date']> target_date_1, 'table_name']

#print(viable_CEFs_0)
#print(viable_CEFs_1)

viable_CEFs_2 = pd.merge(viable_CEFs_0, viable_CEFs_1, on = 'table_name', how = 'inner')
viable_CEFs = pd.merge(viable_CEFs_2, earliest_dates_df, on='table_name', how = 'inner')
#print(viable_CEFs)

print('\nDesired Start of Data is: ', target_date_0)
print('Desired End of Data is: ', target_date_1)
print('\nCEFs with data available are:\n', viable_CEFs)

#########################################################################
dfs = {table: pd.read_sql_query(f"SELECT * FROM {table}", conn_CEF, parse_dates=['date']) for table in viable_CEFs['table_name']}


##Check Headers in first df of dictionary##
# 1. Get the first key in the dictionary
#first_key = next(iter(dfs))
# 2. Access the DataFrame and print its column headers
#print(dfs[first_key].columns.tolist())

#Only keep certain columns for all dfs in a dictionary python
req_columns = ['DATE','TICKER','NAV_0','PrevClose', 'Discount_0', 'NAV_Cumulative', 'Distribution_Cumulative', 'DISTRIBUTION', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME', 'VWAP']
req_columns = [title.lower() for title in req_columns]
#print(req_columns)

for name, df in dfs.items():
    
    current_columns = [title.lower() for title in df.columns]
    current_columns = set(current_columns)
    match_columns = set(req_columns)
    
    # Check if columns match exactly
    if current_columns == match_columns:
        print(f"Validation Passed for {name}")
        # Save to SQLite
        #df.to_sql(name, conn_CEF, if_exists='replace', index=False)
    else:
        # Find missing/extra columns for feedback
        missing = match_columns - current_columns
        extra = current_columns - match_columns
        print(f"Validation Failed for {name}. Missing: {missing}, Extra: {extra}")
        
cols_to_remove = list(extra)

for df in dfs.values():
        df.columns = df.columns.str.lower()
        df.drop(columns=cols_to_remove, inplace=True, errors='ignore')

#print(dfs)

def get_master_dfs():
    return dfs



# Close connection when finished
conn_CEF.close()
conn_ALT.close()
conn_ETF.close()