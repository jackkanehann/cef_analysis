import pandas as pd
import sqlite3

# Define file and table names
csv_file = "MEDCPIM158SFRBCLE.csv"
db_file = 'DATA_database.db'
table_name = 'CPI_ann_median_table'

try:
    # 1. Read the CSV file into a pandas DataFrame
    df = pd.read_csv(csv_file)
    
    # Optional: Strip whitespace from column headers for clean SQL column names
    df.columns = df.columns.str.strip()

    # 2. Connect to the SQLite database (it will be created if it doesn't exist)
    conn = sqlite3.connect(db_file)

    # 3. Write the DataFrame to the SQL database table
    # 'if_exists="replace"' will drop the table if it already exists and create a new one
    # 'index=False' prevents pandas from writing the DataFrame index as a column in the DB
    df.to_sql(table_name, conn, if_exists='replace', index=False)

    # 4. Commit changes and close the connection
    conn.commit()
    conn.close()

    print(f"Successfully imported {csv_file} into table {table_name} in {db_file}")

except FileNotFoundError:
    print(f"Error: The file {csv_file} was not found.")
except Exception as e:
    print(f"An error occurred: {e}")

