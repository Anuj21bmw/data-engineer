-- Properties table
CREATE TABLE IF NOT EXISTS properties (
    id INT AUTO_INCREMENT PRIMARY KEY,
    property_id VARCHAR(255) NOT NULL UNIQUE,
    address TEXT,
    year_built INT,
    city VARCHAR(255),
    state VARCHAR(255),
    zip_code VARCHAR(10)
);

-- HOA details
CREATE TABLE IF NOT EXISTS hoa_details (
    id INT AUTO_INCREMENT PRIMARY KEY,
    property_id VARCHAR(255),
    dues DECIMAL(10, 2),
    frequency VARCHAR(50),
    FOREIGN KEY (property_id) REFERENCES properties(property_id)
);

-- Rehab estimates
CREATE TABLE IF NOT EXISTS rehab_estimates (
    id INT AUTO_INCREMENT PRIMARY KEY,
    property_id VARCHAR(255),
    estimate_amount DECIMAL(12, 2),
    scope TEXT,
    FOREIGN KEY (property_id) REFERENCES properties(property_id)
);

-- Valuations
CREATE TABLE IF NOT EXISTS valuations (
    id INT AUTO_INCREMENT PRIMARY KEY,
    property_id VARCHAR(255),
    estimated_value DECIMAL(12, 2),
    source VARCHAR(100),
    valuation_date DATE,
    FOREIGN KEY (property_id) REFERENCES properties(property_id)
);
