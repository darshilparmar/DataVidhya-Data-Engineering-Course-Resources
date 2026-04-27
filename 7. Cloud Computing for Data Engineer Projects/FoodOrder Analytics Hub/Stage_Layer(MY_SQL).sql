create schema Stage;

CREATE TABLE Stage.stg_customers (
    customer_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(20),
    address VARCHAR(255),
    city VARCHAR(50),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    registration_date DATE,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



CREATE TABLE Stage.stg_restaurants (
    restaurant_id INT,
    restaurant_name VARCHAR(100),
    cuisine_type VARCHAR(50),
    address VARCHAR(255),
    city VARCHAR(50),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    phone VARCHAR(20),
    email VARCHAR(100),
    is_active BOOLEAN,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



CREATE TABLE Stage.stg_menu_items (
    menu_item_id INT,
    restaurant_id INT,
    item_name VARCHAR(100),
    item_description TEXT,
    category VARCHAR(50),
    price DECIMAL(10,2),
    is_available BOOLEAN,
    preparation_time INT,
    calories INT,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



CREATE TABLE Stage.stg_orders (
    order_id INT,
    customer_id INT,
    restaurant_id INT,
    order_date TIMESTAMP,
    order_status VARCHAR(20),
    total_amount DECIMAL(10,2),
    delivery_address VARCHAR(255),
    payment_method VARCHAR(50),
    payment_status VARCHAR(20),
    delivery_instructions TEXT,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);