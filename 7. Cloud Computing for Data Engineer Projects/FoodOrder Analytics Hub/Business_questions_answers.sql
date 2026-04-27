show tables;


select * from dim_customers;
select * from dim_restaurants;
select * from fact_order_patterns;

-- Firstly let's check which is the preferred Cuisine by Customers.
select preferred_cuisine,count(*) from fact_order_patterns group by preferred_cuisine; 

-- We will use this data to analyze Name of the customers who like preferred_cuisine as 'Italian'.
select * from   
fact_order_patterns fact
inner join
dim_customers dim on fact.customer_key=dim.customer_key
where preferred_cuisine='Italian';
; 


-- Let's check which day of the week has most orders.
select preferred_order_day,sum(total_spent) from   
fact_order_patterns fact
group by preferred_order_day
order by sum(preferred_order_day) desc
;

--  So on Monday People tend to order more food.

-- Let's check which of the restraunt is performing well.
select max(total_spent),restaurant_name from   
fact_order_patterns fact
inner join
dim_restaurants dim on fact.restaurant_key=dim.restaurant_key
group by restaurant_name;


-- Checking which Cuisine from which restraunt people are liking. 
select max(total_spent),restaurant_name,preferred_cuisine from   
fact_order_patterns fact
inner join
dim_restaurants dim on fact.restaurant_key=dim.restaurant_key
group by restaurant_name,preferred_cuisine
;
