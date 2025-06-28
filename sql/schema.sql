USE home_db;

-- Drop tables in reverse dependency order to handle foreign keys
DROP TABLE IF EXISTS valuations;
DROP TABLE IF EXISTS rehab_estimates;
DROP TABLE IF EXISTS hoa_details;
DROP TABLE IF EXISTS properties;

-- Properties table (main entity)
CREATE TABLE IF NOT EXISTS properties (
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
    flood_zone VARCHAR(50),
    highway_nearby BOOLEAN DEFAULT FALSE,
    train_nearby BOOLEAN DEFAULT FALSE,
    occupancy VARCHAR(100),
    source VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- Indexes for better performance
    INDEX idx_property_id (property_id),
    INDEX idx_city_state (city, state),
    INDEX idx_property_type (property_type),
    INDEX idx_year_built (year_built)
);

-- HOA details table
CREATE TABLE IF NOT EXISTS hoa_details (
    id INT AUTO_INCREMENT PRIMARY KEY,
    property_id VARCHAR(255) NOT NULL,
    dues DECIMAL(10, 2),
    frequency VARCHAR(50),
    hoa_flag VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (property_id) REFERENCES properties(property_id) ON DELETE CASCADE,
    INDEX idx_hoa_property (property_id)
);

-- Rehab estimates table
CREATE TABLE IF NOT EXISTS rehab_estimates (
    id INT AUTO_INCREMENT PRIMARY KEY,
    property_id VARCHAR(255) NOT NULL,
    estimate_amount DECIMAL(12, 2),
    calculation_amount DECIMAL(12, 2),
    scope TEXT,
    paint_needed VARCHAR(50),
    flooring_needed VARCHAR(50),
    kitchen_needed VARCHAR(50),
    bathroom_needed VARCHAR(50),
    hvac_needed VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (property_id) REFERENCES properties(property_id) ON DELETE CASCADE,
    INDEX idx_rehab_property (property_id)
);

-- Valuations table
CREATE TABLE IF NOT EXISTS valuations (
    id INT AUTO_INCREMENT PRIMARY KEY,
    property_id VARCHAR(255) NOT NULL,
    valuation_type VARCHAR(100),
    estimated_value DECIMAL(12, 2),
    source VARCHAR(100),
    valuation_date DATE,
    confidence_score DECIMAL(3,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (property_id) REFERENCES properties(property_id) ON DELETE CASCADE,
    INDEX idx_valuation_property (property_id),
    INDEX idx_valuation_type (valuation_type),
    INDEX idx_estimated_value (estimated_value)
);

-- Create a summary view for easy querying
CREATE OR REPLACE VIEW property_summary AS
SELECT 
    p.property_id,
    p.address,
    p.city,
    p.state,
    p.zip_code,
    p.property_type,
    p.year_built,
    p.bedrooms,
    p.bathrooms,
    p.sqft_total,
    
    -- Financial information from valuations
    MAX(CASE WHEN v.valuation_type = 'List Price' THEN v.estimated_value END) as list_price,
    MAX(CASE WHEN v.valuation_type = 'Zestimate' THEN v.estimated_value END) as zestimate,
    MAX(CASE WHEN v.valuation_type = 'Redfin Estimate' THEN v.estimated_value END) as redfin_value,
    MAX(CASE WHEN v.valuation_type = 'After Repair Value' THEN v.estimated_value END) as arv,
    MAX(CASE WHEN v.valuation_type = 'Rental Value' THEN v.estimated_value END) as expected_rent,
    
    -- HOA information
    h.dues as hoa_dues,
    h.frequency as hoa_frequency,
    
    -- Rehab information
    r.estimate_amount as rehab_estimate,
    r.scope as rehab_scope,
    
    p.created_at

FROM properties p
LEFT JOIN valuations v ON p.property_id = v.property_id
LEFT JOIN hoa_details h ON p.property_id = h.property_id
LEFT JOIN rehab_estimates r ON p.property_id = r.property_id
GROUP BY p.property_id, p.address, p.city, p.state, p.zip_code, p.property_type, 
         p.year_built, p.bedrooms, p.bathrooms, p.sqft_total, h.dues, h.frequency,
         r.estimate_amount, r.scope, p.created_at;

-- Show created tables
SHOW TABLES;

-- Display table information
SELECT 
    TABLE_NAME as 'Table Name',
    TABLE_COMMENT as 'Description'
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'home_db' 
AND TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_NAME;