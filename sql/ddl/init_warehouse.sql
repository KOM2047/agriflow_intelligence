-- =============================================
-- Project: AgriFlow Intelligence
-- Description: Star Schema Setup (SCD Type 2 Included)
-- Author: Kabelo Modimoeng
-- =============================================
-- 1. CLEANUP (For development iterations)
DROP TABLE IF EXISTS fact_harvest_yield CASCADE;
DROP TABLE IF EXISTS dim_market CASCADE;
DROP TABLE IF EXISTS dim_weather CASCADE;
DROP TABLE IF EXISTS dim_farm CASCADE;
DROP TABLE IF EXISTS dim_crop CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
-- =============================================
-- 2. DIMENSION TABLES
-- =============================================
-- DATE DIMENSION (Static reference for time analysis)
CREATE TABLE dim_date (
    date_id INT PRIMARY KEY,
    -- Format YYYYMMDD (e.g. 20260217)
    full_date DATE NOT NULL,
    year INT,
    quarter INT,
    month INT,
    week_of_year INT,
    day_of_week INT,
    season VARCHAR(10),
    -- 'Summer', 'Winter' (Crucial for Agri)
    is_weekend BOOLEAN
);
-- CROP DIMENSION (SCD Type 1 - Overwrite)
CREATE TABLE dim_crop (
    crop_id SERIAL PRIMARY KEY,
    crop_code VARCHAR(20) UNIQUE,
    -- Business Key (e.g. 'MZ-YEL')
    crop_name VARCHAR(50),
    -- 'Maize'
    variety VARCHAR(50),
    -- 'Yellow Dent'
    category VARCHAR(50),
    -- 'Grain'
    ideal_growing_days INT
);
-- FARM DIMENSION (SCD Type 2 - History Tracking)
-- Tracks changes in Farm Management or Size over time
CREATE TABLE dim_farm (
    farm_key SERIAL PRIMARY KEY,
    -- Surrogate Key (Internal ID)
    farm_id VARCHAR(20) NOT NULL,
    -- Business Key (External System ID)
    farm_name VARCHAR(100),
    region VARCHAR(50),
    -- 'Free State', 'Limpopo'
    manager_name VARCHAR(100),
    total_hectares DECIMAL(10, 2),
    -- SCD Type 2 Columns
    is_current BOOLEAN DEFAULT TRUE,
    valid_from DATE NOT NULL,
    valid_to DATE DEFAULT '9999-12-31' -- Future date for active records
);
-- WEATHER DIMENSION (Context)
CREATE TABLE dim_weather (
    weather_id SERIAL PRIMARY KEY,
    date_id INT REFERENCES dim_date(date_id),
    region VARCHAR(50),
    avg_temp_c DECIMAL(5, 2),
    precipitation_mm DECIMAL(5, 2),
    humidity_percent DECIMAL(5, 2),
    condition VARCHAR(50) -- 'Sunny', 'Rain', 'Drought'
);
-- MARKET DIMENSION (Context)
CREATE TABLE dim_market (
    market_id SERIAL PRIMARY KEY,
    date_id INT REFERENCES dim_date(date_id),
    crop_name VARCHAR(50),
    -- Links to Crop generally
    price_per_ton_zar DECIMAL(10, 2),
    currency VARCHAR(3) DEFAULT 'ZAR'
);
-- =============================================
-- 3. FACT TABLE (Transactions)
-- =============================================
CREATE TABLE fact_harvest_yield (
    harvest_id SERIAL PRIMARY KEY,
    -- Foreign Keys
    date_id INT REFERENCES dim_date(date_id),
    crop_id INT REFERENCES dim_crop(crop_id),
    farm_key INT REFERENCES dim_farm(farm_key),
    -- Links to specific SCD version
    weather_id INT REFERENCES dim_weather(weather_id),
    market_id INT REFERENCES dim_market(market_id),
    -- Business Metrics
    quantity_harvested_kg DECIMAL(10, 2),
    spoilage_kg DECIMAL(10, 2),
    labor_cost_zar DECIMAL(10, 2),
    logistics_cost_zar DECIMAL(10, 2),
    -- Calculated Metrics
    revenue_zar DECIMAL(12, 2),
    -- (Qty - Spoilage) * Market Price
    profit_zar DECIMAL(12, 2),
    -- Revenue - (Labor + Logistics)
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Indexes for performance
CREATE INDEX idx_fact_date ON fact_harvest_yield(date_id);
CREATE INDEX idx_fact_farm ON fact_harvest_yield(farm_key);
CREATE INDEX idx_fact_crop ON fact_harvest_yield(crop_id);