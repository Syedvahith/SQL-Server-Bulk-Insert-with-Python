import os
import time
import pypyodbc as odbc  # pip install pypyodbc

def bulk_insert(data_file, target_table):
    sql = f"""
        BULK INSERT {target_table}
        FROM '{data_file}'
        WITH
        (   
            FORMAT='CSV',
            FIRSTROW = 2, 
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\\n'
        )    
    """.strip()
    return sql

# Record start time
start_time = time.time()

# Step 1. Establish SQL Server Connection
SERVICE_NAME = '<server name>'
DATABASE_NAME = '<database name>'

conn = odbc.connect(f"""
    Driver={{SQL Server}};
    Server={SERVICE_NAME};
    Database={DATABASE_NAME};
    UID=<user id>;
    PWD=<password>;
""".strip())

print("Connected successfully to the database!")

# Step 2. Define data files and target tables
data_file_folder = os.path.join(os.getcwd(), 'Data Files')
data_files = os.listdir(data_file_folder)

# Define target tables for each CSV file
target_tables = {
    'new.csv': 'Bal',
    'sampledata.csv': 'Bal3'
}

# Step 3. Iterate over data files and insert into respective target tables
cursor = conn.cursor()
try:
    with cursor:
        for data_file in data_files:
            target_table = target_tables.get(data_file)
            if target_table:
                try:
                    cursor.execute(bulk_insert(os.path.join(data_file_folder, data_file), target_table))
                    print(f"{data_file} inserted into {target_table}.")
                except Exception as e:
                    print(f"Error inserting {data_file}:", e)
                    continue
    conn.commit()
    print("All data files inserted successfully!")
except Exception as e:
    print("Error:", e)
    conn.rollback()

# Close the database connection
conn.close()
print("Database connection closed.")

# Record end time
end_time = time.time()

# Calculate and print compile time
compile_time = end_time - start_time
print(f"Compile time: {compile_time} seconds")


