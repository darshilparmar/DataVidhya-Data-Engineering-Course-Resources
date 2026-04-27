create schema business;
CREATE TABLE business.dim_customers (
    customer_key BIGINT,
    customer_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    full_name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    address VARCHAR(255),
    city VARCHAR(50),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    region VARCHAR(50),

    -- Demographic Attributes
    age_group VARCHAR(20),
    customer_segment VARCHAR(50),
    signup_channel VARCHAR(50),
    loyalty_tier VARCHAR(20),

    -- Temporal Attributes
    registration_date DATE,
    first_order_date DATE,
    last_order_date DATE,
    customer_tenure_days INT,

    -- SCD Type 2 Management
    start_date DATE,
    end_date DATE,
    current_flag INT,
    version_number INT
);

CREATE TABLE business.dim_restaurants (
    restaurant_key BIGINT,
    restaurant_id INT,
    restaurant_name VARCHAR(100),
    cuisine_type VARCHAR(50),
    city VARCHAR(50),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    phone VARCHAR(20),
    email VARCHAR(100),
    is_active INT
);
GO


CREATE TABLE business.dim_date (
    date_key BIGINT, -- YYYYMMDD
    full_date DATE,
    day INT,
    month INT,
    year INT,
    week INT,
    day_name VARCHAR(10)
);
GO



CREATE TABLE business.fact_order_patterns (
    pattern_key BIGINT,

    -- Foreign Keys
    customer_key INT NOT NULL,
    restaurant_key INT NOT NULL,
    date_key INT NOT NULL,

    -- Metrics
    order_count INT,
    total_spent DECIMAL(10,2),
    avg_basket_size DECIMAL(10,2),

    -- Behavioral Patterns
    preferred_cuisine VARCHAR(50),
    meal_period VARCHAR(20),          -- Breakfast, Lunch, Dinner, Late Night
    preferred_order_hour INT,
    preferred_order_day VARCHAR(10),  -- Monday..Sunday

    -- Delivery Insights
    preferred_delivery_address VARCHAR(255),
    avg_delivery_distance DECIMAL(8,2),

    -- Audit
    load_timestamp DATETIME2(6)
);
GO


