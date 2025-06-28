import pandas as pd
import mysql.connector
from mysql.connector import Error
import logging
import sys
import os
from datetime import datetime
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def connect_to_database():
    """Connect to MySQL database"""
    try:
        connection = mysql.connector.connect(
            host='localhost',
            port=3306,
            database='home_db',
            user='db_user',
            password='6equj5_db_user',
            charset='utf8mb4',
            autocommit=True
        )
        logger.info("Successfully connected to MySQL database")
        return connection
    except Error as e:
        logger.error(f"Error connecting to MySQL: {e}")
        return None

def create_schema(cursor):
    """Create database schema"""
    logger.info("Creating database schema...")
    
    try:
        # Drop existing tables in correct order
        tables_to_drop = ['valuations', 'rehab_estimates', 'hoa_details', 'properties']
        for table in tables_to_drop:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
        
        # Create properties table
        cursor.execute("""
            CREATE TABLE properties (
                id INT AUTO_INCREMENT PRIMARY KEY,
                property_id VARCHAR(255) NOT NULL UNIQUE,
                address TEXT,
                year_built INT,
                city VARCHAR(255),
                state VARCHAR(255),
                zip_code VARCHAR(10),
                property_type VARCHAR(100),
                bedrooms INT,
                bathrooms INT,
                sqft_total INT,
                sqft_basement INT,
                parking VARCHAR(100),
                pool VARCHAR(50),
                commercial VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_property_id (property_id),
                INDEX idx_city_state (city, state)
            )
        """)
        
        # Create hoa_details table
        cursor.execute("""
            CREATE TABLE hoa_details (
                id INT AUTO_INCREMENT PRIMARY KEY,
                property_id VARCHAR(255),
                dues DECIMAL(10, 2),
                frequency VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (property_id) REFERENCES properties(property_id) ON DELETE CASCADE
            )
        """)
        
        # Create rehab_estimates table
        cursor.execute("""
            CREATE TABLE rehab_estimates (
                id INT AUTO_INCREMENT PRIMARY KEY,
                property_id VARCHAR(255),
                estimate_amount DECIMAL(12, 2),
                calculation_amount DECIMAL(12, 2),
                scope TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (property_id) REFERENCES properties(property_id) ON DELETE CASCADE
            )
        """)
        
        # Create valuations table
        cursor.execute("""
            CREATE TABLE valuations (
                id INT AUTO_INCREMENT PRIMARY KEY,
                property_id VARCHAR(255),
                valuation_type VARCHAR(100),
                estimated_value DECIMAL(12, 2),
                source VARCHAR(100),
                valuation_date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (property_id) REFERENCES properties(property_id) ON DELETE CASCADE
            )
        """)
        
        logger.info("Database schema created successfully")
        return True
        
    except Error as e:
        logger.error(f"Error creating schema: {e}")
        return False

def load_and_clean_data():
    """Load and clean CSV data"""
    logger.info("Loading CSV data...")
    
    # Try different possible locations for the CSV file
    possible_paths = [
        'sql/fake_data.csv',
        'fake_data.csv',
        'data/fake_data.csv'
    ]
    
    df = None
    for path in possible_paths:
        try:
            df = pd.read_csv(path)
            logger.info(f"Successfully loaded {len(df)} records from {path}")
            logger.info(f"Data shape: {df.shape}")
            break
        except FileNotFoundError:
            continue
    
    if df is None:
        logger.error("Could not find fake_data.csv in any expected location")
        return None
    
    # Clean data
    logger.info("🧹 Cleaning data...")
    
    # Ensure Property_Title is string
    if 'Property_Title' in df.columns:
        df['Property_Title'] = df['Property_Title'].astype(str)
        df = df[df['Property_Title'].notna() & (df['Property_Title'] != 'nan')]
    
    # Clean numeric columns
    numeric_cols = ['Year_Built', 'Bed', 'Bath', 'SQFT_Total', 'SQFT_Basement', 
                   'List_Price', 'Zestimate', 'ARV', 'Expected_Rent', 'HOA',
                   'Underwriting_Rehab', 'Rehab_Calculation', 'Redfin_Value']
    
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Clean string columns
    string_cols = ['Address', 'City', 'State', 'Zip', 'Property_Type', 'Parking']
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace(['nan', 'NaN', 'None', ''], None)
    
    logger.info("Data cleaning completed")
    return df

def load_properties(cursor, df):
    """Load properties table"""
    logger.info("Loading properties...")
    
    cursor.execute("DELETE FROM properties")
    
    # Select property columns that exist in the dataframe
    property_cols = ['Property_Title', 'Address', 'Year_Built', 'City', 'State', 'Zip',
                    'Property_Type', 'Bed', 'Bath', 'SQFT_Total', 'SQFT_Basement', 'Parking']
    
    available_cols = [col for col in property_cols if col in df.columns]
    properties_df = df[available_cols].drop_duplicates(subset=['Property_Title'])
    
    insert_query = """
        INSERT INTO properties (property_id, address, year_built, city, state, zip_code,
                              property_type, bedrooms, bathrooms, sqft_total, sqft_basement, parking)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    data_tuples = []
    for _, row in properties_df.iterrows():
        data_tuples.append((
            str(row.get('Property_Title', '')),
            str(row.get('Address', '')) if pd.notna(row.get('Address')) else None,
            int(row.get('Year_Built', 0)) if pd.notna(row.get('Year_Built')) and row.get('Year_Built', 0) > 1800 else None,
            str(row.get('City', '')) if pd.notna(row.get('City')) else None,
            str(row.get('State', '')) if pd.notna(row.get('State')) else None,
            str(row.get('Zip', '')) if pd.notna(row.get('Zip')) else None,
            str(row.get('Property_Type', '')) if pd.notna(row.get('Property_Type')) else None,
            int(row.get('Bed', 0)) if pd.notna(row.get('Bed')) and row.get('Bed', 0) > 0 else None,
            int(row.get('Bath', 0)) if pd.notna(row.get('Bath')) and row.get('Bath', 0) > 0 else None,
            int(row.get('SQFT_Total', 0)) if pd.notna(row.get('SQFT_Total')) and row.get('SQFT_Total', 0) > 0 else None,
            int(row.get('SQFT_Basement', 0)) if pd.notna(row.get('SQFT_Basement')) and row.get('SQFT_Basement', 0) > 0 else None,
            str(row.get('Parking', '')) if pd.notna(row.get('Parking')) else None
        ))
    
    cursor.executemany(insert_query, data_tuples)
    logger.info(f"Loaded {len(data_tuples)} properties")

def load_hoa_details(cursor, df):
    """Load HOA details"""
    logger.info("Loading HOA details...")
    
    cursor.execute("DELETE FROM hoa_details")
    
    hoa_cols = ['Property_Title', 'HOA', 'HOA_Flag']
    available_cols = [col for col in hoa_cols if col in df.columns]
    
    if len(available_cols) >= 2:
        hoa_df = df[available_cols].dropna(subset=['HOA'])
        
        insert_query = "INSERT INTO hoa_details (property_id, dues, frequency) VALUES (%s, %s, %s)"
        
        data_tuples = []
        for _, row in hoa_df.iterrows():
            if pd.notna(row.get('HOA')) and row.get('HOA', 0) > 0:
                data_tuples.append((
                    str(row.get('Property_Title', '')),
                    float(row.get('HOA', 0)),
                    str(row.get('HOA_Flag', '')) if pd.notna(row.get('HOA_Flag')) else 'Monthly'
                ))
        
        if data_tuples:
            cursor.executemany(insert_query, data_tuples)
            logger.info(f"Loaded {len(data_tuples)} HOA records")

def load_rehab_estimates(cursor, df):
    """Load rehab estimates"""
    logger.info("Loading rehab estimates...")
    
    cursor.execute("DELETE FROM rehab_estimates")
    
    rehab_cols = ['Property_Title', 'Underwriting_Rehab', 'Rehab_Calculation']
    available_cols = [col for col in rehab_cols if col in df.columns]
    
    if len(available_cols) >= 2:
        rehab_df = df[available_cols]
        
        insert_query = """
            INSERT INTO rehab_estimates (property_id, estimate_amount, calculation_amount, scope)
            VALUES (%s, %s, %s, %s)
        """
        
        data_tuples = []
        for _, row in rehab_df.iterrows():
            underwriting = row.get('Underwriting_Rehab', 0)
            calculation = row.get('Rehab_Calculation', 0)
            
            if (pd.notna(underwriting) and underwriting > 0) or (pd.notna(calculation) and calculation > 0):
                scope = f"Underwriting: ${underwriting:,.2f}, Calculation: ${calculation:,.2f}"
                
                data_tuples.append((
                    str(row.get('Property_Title', '')),
                    float(underwriting) if pd.notna(underwriting) and underwriting > 0 else None,
                    float(calculation) if pd.notna(calculation) and calculation > 0 else None,
                    scope
                ))
        
        if data_tuples:
            cursor.executemany(insert_query, data_tuples)
            logger.info(f"Loaded {len(data_tuples)} rehab records")

def load_valuations(cursor, df):
    """Load valuations from multiple sources"""
    logger.info("Loading valuations...")
    
    cursor.execute("DELETE FROM valuations")
    
    current_date = datetime.now().date()
    
    # Define valuation sources
    valuation_sources = [
        ('Redfin_Value', 'Redfin Estimate', 'Redfin'),
        ('List_Price', 'List Price', 'Market'),
        ('Zestimate', 'Zestimate', 'Zillow'),
        ('ARV', 'After Repair Value', 'Analysis'),
        ('Expected_Rent', 'Monthly Rent', 'Rental Market')
    ]
    
    insert_query = """
        INSERT INTO valuations (property_id, valuation_type, estimated_value, source, valuation_date)
        VALUES (%s, %s, %s, %s, %s)
    """
    
    data_tuples = []
    for col_name, val_type, source in valuation_sources:
        if col_name in df.columns:
            for _, row in df.iterrows():
                value = row.get(col_name, 0)
                if pd.notna(value) and value > 0:
                    data_tuples.append((
                        str(row.get('Property_Title', '')),
                        val_type,
                        float(value),
                        source,
                        current_date
                    ))
    
    if data_tuples:
        cursor.executemany(insert_query, data_tuples)
        logger.info(f"Loaded {len(data_tuples)} valuation records")

def verify_data(cursor):
    """Verify loaded data"""
    logger.info("🔍 Verifying data...")
    
    tables = ['properties', 'hoa_details', 'rehab_estimates', 'valuations']
    total_records = 0
    
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        logger.info(f"📊 {table}: {count:,} records")
        total_records += count
    
    logger.info(f"📈 Total records across all tables: {total_records:,}")
    
    # Sample data
    cursor.execute("SELECT property_id, address, city, state FROM properties LIMIT 3")
    sample = cursor.fetchall()
    logger.info("🔍 Sample properties:")
    for row in sample:
        logger.info(f"  - {row[0]}: {row[1]}, {row[2]}, {row[3]}")

def main():
    """Main ETL function"""
    print("\n" + "🏠" * 20)
    print("DATA ENGINEERING ASSESSMENT")
    print("Property Data ETL Pipeline")
    print("🏠" * 20 + "\n")
    
    logger.info("=" * 60)
    logger.info("🚀 STARTING ETL PIPELINE")
    logger.info("=" * 60)
    
    start_time = datetime.now()
    
    try:
        # 1: Connect to database
        connection = connect_to_database()
        if not connection:
            return False
        
        cursor = connection.cursor()
        
        # 2: Create schema
        if not create_schema(cursor):
            return False
        
        # 3: Load and clean data
        df = load_and_clean_data()
        if df is None:
            return False
        
        # 4: Load data into tables
        load_properties(cursor, df)
        load_hoa_details(cursor, df)
        load_rehab_estimates(cursor, df)
        load_valuations(cursor, df)
        
        # 5: Verify data
        verify_data(cursor)
        
        # Commit and close
        connection.commit()
        cursor.close()
        connection.close()
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("=" * 60)
        logger.info(f"🎉 ETL PIPELINE COMPLETED in {duration}")
        logger.info("=" * 60)
        
        print("\n SUCCESS! ETL Pipeline completed!")
        print("Database is ready for analysis")
        print("Connect: mysql -h localhost -P 3306 -u db_user -p home_db")
        print("Password: 6equj5_db_user")
        print("\n Quick test query:")
        print("SELECT COUNT(*) FROM properties;")
        
        return True
        
    except Exception as e:
        logger.error(f"ETL Pipeline failed: {e}")
        print(f"\nETL Pipeline failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)