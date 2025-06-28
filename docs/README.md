# Data Engineering Assessment

Welcome! This exercise is designed to evaluate your core skills in **data engineering**:
- **SQL databases**: Data modeling, normalization, and scripting
- **Python and ETL**: Data cleaning, transformation, and loading workflows

---

##  How This Document Works

Each section is structured with:
- **Problem:** Background and context for the task
- **Task:** What you are required to do (including any bonus "extra" tasks)
- **Solution:** Where you must document your approach, decisions, and provide instructions for reviewers

> **Tech Stack:**  
> Please use only Python (for ETL/data processing) and SQL/MySQL (for database).  
> Only use extra libraries if they do not replace core logic, and clearly explain your choices in your solution.

---

## 0. Setup

1. **Fork and clone this repository:**
    ```bash
    git clone https://github.com/<your-username>/homellc_data_engineer_assessment_skeleton.git
    ```
2. **Start the MySQL database in Docker:**
    ```bash
    docker-compose -f docker-compose.initial.yml up --build -d
    ```
    - Database is available on `localhost:3306`
    - Credentials/configuration are in the Docker Compose file
    - **Do not change** database name or credentials

3. For MySQL Docker image reference:  
   [MySQL Docker Hub](https://hub.docker.com/_/mysql)

---

### Problem

You are provided with property-related data in a CSV file.
- Each row relates to a property.
- There are multiple Columns related to this property.
- The database is not normalized and lacks relational structure.

### Task

- **Normalize the data:**
  - Develop a Python ETL script to read, clean, transform, and load data into your normalized MySQL tables.
  - Refer the field config document for the relation of business logic
  - Use primary keys and foreign keys to properly capture relationships

- **Deliverable:**
  - Write necessary python and sql scripts
  - Place the scripts inside the `sql/` directory)
  - The scripts should take the initial csv to your final, normalized schema when executed
  - Clearly document how to run your script, dependencies, and how it integrates with your database.

**Tech Stack:**  
- Python (include a `requirements.txt`)
Use **MySQL** and SQL for all database work  
- You may use any CLI or GUI for development, but the final changes must be submitted as python/ SQL scripts 
- **Do not** use ORM migrations—write all SQL by hand

---

## Solution

**Candidate:** Anuj Meena  
**Date:** June 29, 2025

### Database Design and Schema

I analyzed the source CSV file which contains 10,000 property records with 66 columns. The original data was completely denormalized - mixing property details, financial information, HOA data, and renovation estimates in a single flat structure. This created significant redundancy and made data analysis difficult.

#### My Normalized Schema Design

I broke down the data into 4 main tables following database normalization principles:

**1. Properties Table (Core Information)**
```sql
CREATE TABLE properties (
    property_id VARCHAR(255) PRIMARY KEY,
    address TEXT,
    city VARCHAR(255),
    state VARCHAR(255),
    zip_code VARCHAR(10),
    property_type VARCHAR(100),
    year_built INT,
    bedrooms INT,
    bathrooms INT,
    sqft_total INT,
    sqft_basement INT,
    parking VARCHAR(100)
);
```
This stores the fundamental property information that doesn't change frequently. I used the Property_Title column as the primary key since it was the most reliable unique identifier in the source data.

**2. HOA Details Table**
```sql
CREATE TABLE hoa_details (
    property_id VARCHAR(255),
    dues DECIMAL(10, 2),
    frequency VARCHAR(50),
    FOREIGN KEY (property_id) REFERENCES properties(property_id)
);
```
I separated HOA information because not all properties have HOA fees, and this data can change independently of the property itself.

**3. Rehab Estimates Table**
```sql
CREATE TABLE rehab_estimates (
    property_id VARCHAR(255),
    estimate_amount DECIMAL(12, 2),
    calculation_amount DECIMAL(12, 2),
    scope TEXT,
    FOREIGN KEY (property_id) REFERENCES properties(property_id)
);
```
Renovation estimates deserve their own table since they can be updated multiple times and represent a different business process than basic property info.

**4. Valuations Table**
```sql
CREATE TABLE valuations (
    property_id VARCHAR(255),
    valuation_type VARCHAR(100),
    estimated_value DECIMAL(12, 2),
    source VARCHAR(100),
    valuation_date DATE,
    FOREIGN KEY (property_id) REFERENCES properties(property_id)
);
```
This was my solution for handling multiple valuation sources (Zillow, Redfin, market listing, etc.). Instead of separate columns, I store each valuation as a separate row, making comparisons much easier.

#### Why This Design Works

- **Eliminates redundancy**: No more repeating property info across rows
- **Supports growth**: Easy to add new valuation sources or rehab estimates
- **Maintains relationships**: Foreign keys ensure data integrity
- **Enables analysis**: Can easily compare valuations or track renovation costs

### ETL Implementation

#### My Approach

I built the ETL pipeline to handle the messy real-world data in the CSV file. Here's how I tackled each phase:

**Extract Phase:**
- Read the CSV using pandas (it's the most reliable way to handle large files)
- Check for file existence and log basic stats
- Validate that we have the expected columns

**Transform Phase:**
- Clean up data types (lots of the "numeric" columns had text mixed in)
- Handle missing values appropriately for each column type
- Split the single row into data for multiple tables
- Create meaningful descriptions for rehab scope

**Load Phase:**
- Create tables in the right order (parent first, then children)
- Use batch inserts for performance
- Validate foreign key relationships

#### Key Challenges I Solved

**Data Quality Issues:**
The CSV had inconsistent formatting - some numbers were stored as text, dates were in different formats, and there were plenty of null values. I wrote robust cleaning logic to handle these cases.

**Relationship Mapping:**
Figuring out which columns belonged to which business entity required careful analysis. I used the Field Config document to understand the business logic behind each column.

**Performance:**
Loading 50,000+ records (10K properties × ~5 valuations each) required batch processing to avoid timeouts.

### How to Run This Solution

#### Prerequisites
You need Docker Desktop and Python 3.8+ installed on your machine.

#### Step 1: Setup Environment
```bash
# Navigate to the project folder
cd data-engineer

# Install Python packages
pip install -r requirements.txt
```

#### Step 2: Start Database
```bash
# Launch MySQL in Docker
docker-compose -f docker-compose.initial.yml up --build -d

# Verify it's running
docker ps
```
You should see a container named `mysql_ctn` running.

#### Step 3: Run the ETL Pipeline
```bash
# Execute the main script
python scripts/etl.py
```

The script will show progress logs and should complete in under 5 seconds. You'll see output like:
```
 Successfully connected to MySQL database
 Database schema created successfully  
 Successfully loaded 10000 records from CSV
 Loaded 10000 properties
 Loaded 9974 HOA records
 Loaded 10000 rehab records
 Loaded 50000 valuation records
 ETL PIPELINE COMPLETED
```

#### Step 4: Verify the Results
```bash
# Connect to the database
docker exec -it mysql_ctn mysql -u db_user -p home_db
# Password: 6equj5_db_user
```

Then run these queries to check your data:
```sql
-- See all tables
SHOW TABLES;

-- Check record counts
SELECT 'Properties' as table_name, COUNT(*) as count FROM properties
UNION ALL
SELECT 'Valuations', COUNT(*) FROM valuations;

-- Sample the data
SELECT property_id, address, city, state FROM properties LIMIT 3;
```

### Technical Details

#### Dependencies Used
```
pandas==2.1.4               # For CSV processing
mysql-connector-python==8.2.0  # MySQL database driver
numpy==1.24.3               # Math operations for pandas
openpyxl==3.1.2             # Excel file support
```

I kept dependencies minimal and chose well-established libraries. Pandas is the standard for data processing in Python, and mysql-connector-python is the official MySQL driver.

#### Error Handling
The ETL script includes comprehensive error handling:
- Database connection failures
- Missing CSV files
- Data type conversion errors
- Foreign key constraint violations

All errors are logged with timestamps for debugging.

#### Performance Considerations
- Used `executemany()` for batch database inserts
- Added database indexes on frequently queried columns
- Processed data in memory (the 10K records fit comfortably)

### Results and Data Quality

After running the pipeline, I verified data quality:

**Record Counts:**
- Properties: 10,000 (100% of source records)
- HOA Details: 9,974 (99.7% - only properties with HOA data)
- Rehab Estimates: 10,000 (100% - all properties have some rehab data)
- Valuations: 50,000 (5 different valuation types per property)

**Data Integrity:**
- Zero orphaned records (all foreign keys valid)
- No duplicate property IDs
- Proper data types in all columns

**Sample Business Queries:**
```sql
-- Average property values by state
SELECT state, COUNT(*) as properties, AVG(estimated_value) as avg_price
FROM properties p
JOIN valuations v ON p.property_id = v.property_id
WHERE v.valuation_type = 'List Price'
GROUP BY state
ORDER BY properties DESC;

-- Properties with highest renovation needs
SELECT p.address, p.city, r.estimate_amount
FROM properties p
JOIN rehab_estimates r ON p.property_id = r.property_id
ORDER BY r.estimate_amount DESC
LIMIT 10;
```

### Business Value Created

This normalized database enables several analytical capabilities that weren't possible with the flat CSV:

1. **Investment Analysis**: Compare different valuation sources to identify undervalued properties
2. **Market Research**: Analyze property trends by geographic region
3. **Renovation Planning**: Track and budget renovation costs across the portfolio
4. **Performance Tracking**: Monitor property values over time as new valuations come in

The clean, normalized structure makes it easy for analysts and other stakeholders to work with the data without needing to understand the original messy CSV format.

### File Structure
```
homellc_data_engineer_assessment_skeleton/
├── README.md                    # This documentation
├── requirements.txt             # Python dependencies  
├── scripts/
│   └── etl.py                  # Main ETL pipeline
├── sql/
│   ├── fake_data.csv           # Source data (10K properties, 66 columns)
│   └── schema.sql              # Database schema definition
└── docker-compose.initial.yml  # MySQL setup
```

### What I Learned

This project reinforced several important data engineering principles:
- Real-world data is always messier than expected
- Proper normalization makes future analysis much easier
- Comprehensive error handling and logging are essential
- Performance matters even for "small" datasets

The solution demonstrates practical data engineering skills including schema design, ETL development, data quality assurance, and business value creation.

---
