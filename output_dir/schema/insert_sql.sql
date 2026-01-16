-- 导入 customer_deliveries 扩充后 (SF=10) 的 CSV 到 MySQL
-- 前置条件：将本目录下 *_SF_10.csv 复制到 MySQL 服务器可读目录，例如：/var/lib/mysql-files/output_dir/
-- 可选：指定数据库
-- USE your_database_name;

SET FOREIGN_KEY_CHECKS = 0;

-- 可选：清空相关表（已关闭外键检查，顺序不限）
TRUNCATE TABLE `actual_order_products`;
TRUNCATE TABLE `order_deliveries`;
TRUNCATE TABLE `regular_order_products`;
TRUNCATE TABLE `actual_orders`;
TRUNCATE TABLE `delivery_route_locations`;
TRUNCATE TABLE `customer_addresses`;
TRUNCATE TABLE `regular_orders`;
TRUNCATE TABLE `employees`;
TRUNCATE TABLE `trucks`;
TRUNCATE TABLE `delivery_routes`;
TRUNCATE TABLE `customers`;
TRUNCATE TABLE `addresses`;
TRUNCATE TABLE `products`;

-- 加载基础维表/被引用表
LOAD DATA INFILE '/var/lib/mysql-files/output_dir/Products.csv'
INTO TABLE `products`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`product_id`, @product_name, `product_price`, @product_description)
SET `product_name` = LEFT(@product_name, 20),
    `product_description` = LEFT(@product_description, 255);

LOAD DATA INFILE '/var/lib/mysql-files/output_dir/Addresses.csv'
INTO TABLE `addresses`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`address_id`, @address_details, @city, @zip_postcode, @state_province_county, @country)
SET `address_details`=LEFT(@address_details,80),
    `city`=LEFT(@city,50),
    `zip_postcode`=LEFT(@zip_postcode,20),
    `state_province_county`=LEFT(@state_province_county,50),
    `country`=LEFT(@country,50);

LOAD DATA INFILE '/var/lib/mysql-files/output_dir/Customers.csv'
INTO TABLE `customers`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`customer_id`, @payment_method, @customer_name, @customer_phone, @customer_email, `date_became_customer`)
SET `payment_method`=LEFT(@payment_method,10),
    `customer_name`=LEFT(@customer_name,80),
    `customer_phone`=LEFT(@customer_phone,80),
    `customer_email`=LEFT(@customer_email,80);

LOAD DATA INFILE '/var/lib/mysql-files/output_dir/Delivery_Routes.csv'
INTO TABLE `delivery_routes`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`route_id`, @route_name, @other_route_details)
SET `route_name`=LEFT(@route_name,50),
    `other_route_details`=LEFT(@other_route_details,255);

LOAD DATA INFILE '/var/lib/mysql-files/output_dir/Trucks.csv'
INTO TABLE `trucks`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`truck_id`, @truck_licence_number, @truck_details)
SET `truck_licence_number`=LEFT(@truck_licence_number,20),
    `truck_details`=LEFT(@truck_details,255);

LOAD DATA INFILE '/var/lib/mysql-files/output_dir/Employees.csv'
INTO TABLE `employees`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`employee_id`,`employee_address_id`, @employee_name, @employee_phone)
SET `employee_name`=LEFT(@employee_name,80),
    `employee_phone`=LEFT(@employee_phone,80);

-- 加载依赖这些表的从表/关系表（按外键顺序）
LOAD DATA INFILE '/var/lib/mysql-files/output_dir/Regular_Orders.csv'
INTO TABLE `regular_orders`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`regular_order_id`,`distributer_id`);

LOAD DATA INFILE '/var/lib/mysql-files/output_dir/Customer_Addresses.csv'
INTO TABLE `customer_addresses`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`customer_id`,`address_id`,`date_from`, @address_type, `date_to`)
SET `address_type`=LEFT(@address_type,10);

LOAD DATA INFILE '/var/lib/mysql-files/output_dir/Delivery_Route_Locations.csv'
INTO TABLE `delivery_route_locations`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(@location_code,`route_id`,`location_address_id`, @location_name)
SET `location_code`=LEFT(@location_code,10),
    `location_name`=LEFT(@location_name,50);

LOAD DATA INFILE '/var/lib/mysql-files/output_dir/Actual_Orders.csv'
INTO TABLE `actual_orders`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`actual_order_id`, @order_status_code, `regular_order_id`,`actual_order_date`)
SET `order_status_code`=LEFT(@order_status_code,10);

LOAD DATA INFILE '/var/lib/mysql-files/output_dir/Regular_Order_Products.csv'
INTO TABLE `regular_order_products`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`regular_order_id`,`product_id`);

LOAD DATA INFILE '/var/lib/mysql-files/output_dir/Order_Deliveries.csv'
INTO TABLE `order_deliveries`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(@location_code,`actual_order_id`, @delivery_status_code,`driver_employee_id`,`truck_id`,`delivery_date`)
SET `location_code`=LEFT(@location_code,10),
    `delivery_status_code`=LEFT(@delivery_status_code,10);

LOAD DATA INFILE '/var/lib/mysql-files/output_dir/Actual_Order_Products.csv'
INTO TABLE `actual_order_products`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`actual_order_id`,`product_id`);

SET FOREIGN_KEY_CHECKS = 1;
