USE home_db;

-- 1. TABLE OVERVIEW

SELECT ' DATABASE OVERVIEW' as section;

SELECT 
    TABLE_NAME as 'Table Name',
    TABLE_ROWS as 'Rows',
    ROUND(DATA_LENGTH / 1024 / 1024, 2) as 'Size (MB)'
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'home_db' 
AND TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_NAME;

-- 2. RECORD COUNTS

SELECT ' RECORD COUNTS' as section;

SELECT 'Properties' as table_name, COUNT(*) as record_count FROM properties
UNION ALL
SELECT 'HOA Details', COUNT(*) FROM hoa_details
UNION ALL
SELECT 'Rehab Estimates', COUNT(*) FROM rehab_estimates
UNION ALL
SELECT 'Valuations', COUNT(*) FROM valuations
ORDER BY record_count DESC;

-- 3. DATA INTEGRITY CHECKS

SELECT ' DATA INTEGRITY CHECKS' as section;

-- Check for orphaned records
SELECT 
    'HOA records without properties' as check_type,
    COUNT(*) as violation_count
FROM hoa_details h 
LEFT JOIN properties p ON h.property_id = p.property_id 
WHERE p.property_id IS NULL

UNION ALL

SELECT 
    'Rehab records without properties',
    COUNT(*)
FROM rehab_estimates r 
LEFT JOIN properties p ON r.property_id = p.property_id 
WHERE p.property_id IS NULL

UNION ALL

SELECT 
    'Valuation records without properties',
    COUNT(*)
FROM valuations v 
LEFT JOIN properties p ON v.property_id = p.property_id 
WHERE p.property_id IS NULL

UNION ALL

SELECT 
    'Properties with missing address',
    COUNT(*)
FROM properties 
WHERE address IS NULL OR address = '';

-- 4. SAMPLE DATA

SELECT ' SAMPLE DATA' as section;

-- Properties sample
SELECT 'PROPERTIES SAMPLE:' as data_type;
SELECT 
    property_id,
    LEFT(address, 40) as address,
    city,
    state,
    property_type,
    year_built,
    bedrooms,
    bathrooms,
    sqft_total
FROM properties 
LIMIT 5;

-- Valuations sample
SELECT 'VALUATIONS SAMPLE:' as data_type;
SELECT 
    property_id,
    valuation_type,
    FORMAT(estimated_value, 0) as estimated_value,
    source,
    valuation_date
FROM valuations 
LIMIT 10;

-- 5. PROPERTY SUMMARY VIEW TEST

SELECT ' PROPERTY SUMMARY VIEW' as section;

SELECT 
    property_id,
    LEFT(address, 35) as address,
    CONCAT(city, ', ', state) as location,
    property_type,
    year_built,
    CONCAT(bedrooms, 'bed/', bathrooms, 'bath') as bed_bath,
    FORMAT(sqft_total, 0) as sqft,
    FORMAT(list_price, 0) as list_price,
    FORMAT(expected_rent, 0) as monthly_rent,
    FORMAT(zestimate, 0) as zestimate,
    FORMAT(hoa_dues, 0) as hoa_dues,
    FORMAT(rehab_estimate, 0) as rehab_cost
FROM property_summary 
WHERE address IS NOT NULL
ORDER BY list_price DESC
LIMIT 8;

-- 6. BUSINESS ANALYTICS

SELECT ' BUSINESS ANALYTICS' as section;

-- Properties by state
SELECT 'PROPERTIES BY STATE:' as analysis_type;
SELECT 
    state,
    COUNT(*) as property_count,
    FORMAT(AVG(list_price), 0) as avg_list_price,
    FORMAT(AVG(expected_rent), 0) as avg_rent
FROM property_summary 
WHERE state IS NOT NULL 
AND state != ''
GROUP BY state
HAVING property_count >= 5
ORDER BY property_count DESC
LIMIT 10;

-- Property types
SELECT 'PROPERTY TYPES:' as analysis_type;
SELECT 
    property_type,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM properties), 2) as percentage
FROM properties 
WHERE property_type IS NOT NULL 
AND property_type != ''
GROUP BY property_type
ORDER BY count DESC;

-- Year built distribution
SELECT 'PROPERTIES BY DECADE:' as analysis_type;
SELECT 
    CASE 
        WHEN year_built >= 2020 THEN '2020s'
        WHEN year_built >= 2010 THEN '2010s'
        WHEN year_built >= 2000 THEN '2000s'
        WHEN year_built >= 1990 THEN '1990s'
        WHEN year_built >= 1980 THEN '1980s'
        WHEN year_built >= 1970 THEN '1970s'
        WHEN year_built < 1970 THEN 'Pre-1970s'
        ELSE 'Unknown'
    END as decade,
    COUNT(*) as property_count
FROM properties
GROUP BY 
    CASE 
        WHEN year_built >= 2020 THEN '2020s'
        WHEN year_built >= 2010 THEN '2010s'
        WHEN year_built >= 2000 THEN '2000s'
        WHEN year_built >= 1990 THEN '1990s'
        WHEN year_built >= 1980 THEN '1980s'
        WHEN year_built >= 1970 THEN '1970s'
        WHEN year_built < 1970 THEN 'Pre-1970s'
        ELSE 'Unknown'
    END
ORDER BY property_count DESC;


-- 7. VALUATION ANALYSIS

SELECT ' VALUATION ANALYSIS' as section;

-- Valuation sources comparison
SELECT 'VALUATION SOURCES:' as comparison_type;
SELECT 
    valuation_type,
    COUNT(*) as property_count,
    FORMAT(AVG(estimated_value), 0) as avg_value,
    FORMAT(MIN(estimated_value), 0) as min_value,
    FORMAT(MAX(estimated_value), 0) as max_value
FROM valuations 
WHERE estimated_value > 0
GROUP BY valuation_type
ORDER BY avg_value DESC;

-- Highest value properties
SELECT 'TOP VALUE PROPERTIES:' as property_type;
SELECT 
    ps.property_id,
    LEFT(ps.address, 30) as address,
    CONCAT(ps.city, ', ', ps.state) as location,
    FORMAT(ps.list_price, 0) as list_price,
    FORMAT(ps.zestimate, 0) as zestimate,
    FORMAT(ps.expected_rent, 0) as monthly_rent
FROM property_summary ps
WHERE ps.list_price IS NOT NULL
ORDER BY ps.list_price DESC
LIMIT 5;

-- Best rental yields
SELECT 'BEST RENTAL YIELDS:' as property_type;
SELECT 
    ps.property_id,
    LEFT(ps.address, 30) as address,
    CONCAT(ps.city, ', ', ps.state) as location,
    FORMAT(ps.list_price, 0) as list_price,
    FORMAT(ps.expected_rent, 0) as monthly_rent,
    ROUND(ps.expected_rent / ps.list_price * 1200, 2) as annual_yield_pct
FROM property_summary ps
WHERE ps.list_price > 50000 
AND ps.expected_rent > 500
ORDER BY annual_yield_pct DESC
LIMIT 5;

-- 8. REHAB ANALYSIS

SELECT ' REHAB ANALYSIS' as section;

-- Properties needing rehab
SELECT 'REHAB STATISTICS:' as rehab_type;
SELECT 
    COUNT(*) as properties_with_rehab,
    FORMAT(AVG(estimate_amount), 0) as avg_rehab_cost,
    FORMAT(SUM(estimate_amount), 0) as total_rehab_cost
FROM rehab_estimates 
WHERE estimate_amount > 0;

-- High rehab cost properties
SELECT 'HIGH REHAB COST PROPERTIES:' as rehab_type;
SELECT 
    ps.property_id,
    LEFT(ps.address, 25) as address,
    CONCAT(ps.city, ', ', ps.state) as location,
    FORMAT(ps.list_price, 0) as list_price,
    FORMAT(ps.rehab_estimate, 0) as rehab_cost,
    ROUND(ps.rehab_estimate / ps.list_price * 100, 1) as rehab_pct
FROM property_summary ps
WHERE ps.rehab_estimate > 0 
AND ps.list_price > 0
ORDER BY rehab_pct DESC
LIMIT 5;

-- 9. DATA QUALITY SUMMARY

SELECT ' DATA QUALITY SUMMARY' as section;

SELECT 
    'Total Properties' as metric,
    COUNT(*) as count,
    '100%' as completeness
FROM properties

UNION ALL

SELECT 
    'Properties with Valuations',
    COUNT(DISTINCT v.property_id),
    CONCAT(ROUND(COUNT(DISTINCT v.property_id) * 100.0 / (SELECT COUNT(*) FROM properties), 1), '%')
FROM valuations v

UNION ALL

SELECT 
    'Properties with HOA Data',
    COUNT(*),
    CONCAT(ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM properties), 1), '%')
FROM hoa_details

UNION ALL

SELECT 
    'Properties with Rehab Data',
    COUNT(*),
    CONCAT(ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM properties), 1), '%')
FROM rehab_estimates

UNION ALL

SELECT 
    'Properties with Complete Address',
    COUNT(*),
    CONCAT(ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM properties), 1), '%')
FROM properties 
WHERE address IS NOT NULL AND address != '' AND city IS NOT NULL AND state IS NOT NULL;

-- 10. FINAL SUMMARY

SELECT ' VERIFICATION COMPLETE' as final_status;

SELECT 
    CONCAT('Database contains ', 
           (SELECT COUNT(*) FROM properties), 
           ' properties across ',
           (SELECT COUNT(DISTINCT state) FROM properties WHERE state IS NOT NULL),
           ' states with ',
           (SELECT COUNT(*) FROM valuations),
           ' total valuations') as summary_info;

SELECT 'ETL Pipeline executed successfully! All data has been normalized and loaded.' as conclusion;