import pyodbc
import pandas as pd

# Step 1: Connect to QuickBooks using QODBC
conn = pyodbc.connect('DSN=DSN3')

# Step 2: Execute the SP_COLUMNS stored procedure to get column metadata for SalesReceiptLine
query = "SELECT TOP 1 * FROM Customer"
df = pd.read_sql(query, conn)

# Step 3: Export the result to an Excel file with headers
df.to_excel('Customer_data.xlsx', index=False, header=True)

# Close the connection
conn.close()

print("Data exported to Excel successfully.")

