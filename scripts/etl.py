import pandas as pd
import mysql.connector

# Load CSV
df = pd.read_csv("sql/fake_data.csv")

# Setup DB connection
conn = mysql.connector.connect(
    host="localhost",
    user="db_user",
    password="6equj5_db_user",
    database="home_db",
    port=3306
)
cursor = conn.cursor()

# Insert into properties
property_cols = ['property_id', 'address', 'year_built', 'city', 'state', 'zip_code']
for _, row in df[property_cols].drop_duplicates().iterrows():
    cursor.execute("""
        INSERT INTO properties (property_id, address, year_built, city, state, zip_code)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE address=VALUES(address)
    """, tuple(row))

# Insert HOA
for _, row in df[['property_id', 'hoa_dues', 'hoa_frequency']].dropna().iterrows():
    cursor.execute("""
        INSERT INTO hoa_details (property_id, dues, frequency)
        VALUES (%s, %s, %s)
    """, tuple(row))

# Insert Rehab
for _, row in df[['property_id', 'rehab_estimate', 'rehab_scope']].dropna().iterrows():
    cursor.execute("""
        INSERT INTO rehab_estimates (property_id, estimate_amount, scope)
        VALUES (%s, %s, %s)
    """, tuple(row))

# Insert Valuations
for _, row in df[['property_id', 'valuation_amount', 'valuation_source', 'valuation_date']].dropna().iterrows():
    cursor.execute("""
        INSERT INTO valuations (property_id, estimated_value, source, valuation_date)
        VALUES (%s, %s, %s, %s)
    """, tuple(row))

conn.commit()
cursor.close()
conn.close()

print("✅ ETL complete.")
