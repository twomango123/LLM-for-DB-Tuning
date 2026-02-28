DROP DATABASE customer_deliveries;
CREATE DATABASE customer_deliveries; 
-- mysql -u root -p --default-character-set=utf8mb4 -D customer_deliveries < output_dir/schema/rollback.sql
-- CREATE DATABASE customer_deliveries_1; 
USE customer_deliveries;

CREATE TABLE `products` (
`product_id` INT PRIMARY KEY AUTO_INCREMENT,
`product_name` VARCHAR(20),
`product_price` DECIMAL(19,4),
`product_description` VARCHAR(255)
);

CREATE TABLE `addresses` (
`address_id` INT PRIMARY KEY AUTO_INCREMENT,
`address_details` VARCHAR(80),
`city` VARCHAR(50),
`zip_postcode` VARCHAR(20),
`state_province_county` VARCHAR(50),
`country` VARCHAR(50)
);

CREATE TABLE `customers` (
`customer_id` INT PRIMARY KEY AUTO_INCREMENT,
`payment_method` VARCHAR(10) NOT NULL,
`customer_name` VARCHAR(80),
`customer_phone` VARCHAR(80),
`customer_email` VARCHAR(80),
`date_became_customer` DATETIME
);

CREATE TABLE `regular_orders` (
`regular_order_id` INT PRIMARY KEY AUTO_INCREMENT,
`distributer_id` INT NOT NULL,
FOREIGN KEY (`distributer_id` ) REFERENCES `customers`(`customer_id` )
);

CREATE TABLE `regular_order_products` (
`regular_order_id` INT NOT NULL,
`product_id` INT NOT NULL,
FOREIGN KEY (`product_id` ) REFERENCES `products`(`product_id` ),
FOREIGN KEY (`regular_order_id` ) REFERENCES `regular_orders`(`regular_order_id` )
);

CREATE TABLE `actual_orders` (
`actual_order_id` INT PRIMARY KEY AUTO_INCREMENT,
`order_status_code` VARCHAR(10) NOT NULL,
`regular_order_id` INT NOT NULL,
`actual_order_date` DATETIME,
FOREIGN KEY (`regular_order_id` ) REFERENCES `regular_orders`(`regular_order_id` )
);

CREATE TABLE `actual_order_products` (
`actual_order_id` INT NOT NULL,
`product_id` INT NOT NULL,
FOREIGN KEY (`product_id` ) REFERENCES `products`(`product_id` ),
FOREIGN KEY (`actual_order_id` ) REFERENCES `actual_orders`(`actual_order_id` )
);

CREATE TABLE `customer_addresses` (
`customer_id` INT NOT NULL,
`address_id` INT NOT NULL,
`date_from` DATETIME NOT NULL,
`address_type` VARCHAR(10) NOT NULL,
`date_to` DATETIME,
FOREIGN KEY (`customer_id` ) REFERENCES `customers`(`customer_id` ),
FOREIGN KEY (`address_id` ) REFERENCES `addresses`(`address_id` )
);

CREATE TABLE `delivery_routes` (
`route_id` INT PRIMARY KEY AUTO_INCREMENT,
`route_name` VARCHAR(50),
`other_route_details` VARCHAR(255)
);

CREATE TABLE `delivery_route_locations` (
`location_code` VARCHAR(10) PRIMARY KEY,
`route_id` INT NOT NULL,
`location_address_id` INT NOT NULL,
`location_name` VARCHAR(50),
FOREIGN KEY (`location_address_id` ) REFERENCES `addresses`(`address_id` ),
FOREIGN KEY (`route_id` ) REFERENCES `delivery_routes`(`route_id` )
);

CREATE TABLE `trucks` (
`truck_id` INT PRIMARY KEY AUTO_INCREMENT,
`truck_licence_number` VARCHAR(20),
`truck_details` VARCHAR(255)
);

CREATE TABLE `employees` (
`employee_id` INT PRIMARY KEY AUTO_INCREMENT,
`employee_address_id` INT NOT NULL,
`employee_name` VARCHAR(80),
`employee_phone` VARCHAR(80),
FOREIGN KEY (`employee_address_id` ) REFERENCES `addresses`(`address_id` )
);

CREATE TABLE `order_deliveries` (
`location_code` VARCHAR(10) NOT NULL,
`actual_order_id` INT NOT NULL,
`delivery_status_code` VARCHAR(10) NOT NULL,
`driver_employee_id` INT NOT NULL,
`truck_id` INT NOT NULL,
`delivery_date` DATETIME,
FOREIGN KEY (`truck_id` ) REFERENCES `trucks`(`truck_id` ),
FOREIGN KEY (`actual_order_id` ) REFERENCES `actual_orders`(`actual_order_id` ),
FOREIGN KEY (`location_code` ) REFERENCES `delivery_route_locations`(`location_code` ),
FOREIGN KEY (`driver_employee_id` ) REFERENCES `employees`(`employee_id` )
);

-- 导入 customer_deliveries 扩充后 (SF=10) 的 CSV 到 MySQL
-- 前置条件：将本目录下 *_SF_10.csv 复制到 MySQL 服务器可读目录，例如：/var/lib/mysql-files/output_dir/
-- 可选：指定数据库
-- USE your_database_name;



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


-- 1. 禁用外键检查
SET FOREIGN_KEY_CHECKS = 0;
-- 2. 禁用唯一/主键检查（减少索引验证）
SET UNIQUE_CHECKS = 0;
-- 3. 关闭自动提交（避免逐行刷磁盘）
SET AUTOCOMMIT = 0;
-- 4. 临时关闭二进制日志（如果不需要主从同步/数据恢复，必开！提速最明显）
SET SQL_LOG_BIN = 0;
-- 加载基础维表/被引用表

LOAD DATA INFILE '/var/lib/mysql-files/output_dir/products.csv'
IGNORE INTO TABLE `products`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`product_id`,`product_name`,`product_price`,`product_description`);
COMMIT;

LOAD DATA INFILE '/var/lib/mysql-files/output_dir/addresses.csv'
IGNORE INTO TABLE `addresses`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`address_id`, `address_details`, `city`, `zip_postcode`, `state_province_county`, `country`);
COMMIT;


LOAD DATA INFILE '/var/lib/mysql-files/output_dir/customers.csv'
IGNORE INTO TABLE `customers`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`customer_id`, @payment_method, @customer_name, @customer_phone, @customer_email, @d);
COMMIT;


LOAD DATA INFILE '/var/lib/mysql-files/output_dir/regular_orders.csv'
IGNORE INTO TABLE `regular_orders`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`regular_order_id`,`distributer_id`);
COMMIT;

LOAD DATA INFILE '/var/lib/mysql-files/output_dir/regular_order_products.csv'
IGNORE INTO TABLE `regular_order_products`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`regular_order_id`,`product_id`);
COMMIT;


LOAD DATA INFILE '/var/lib/mysql-files/output_dir/actual_orders.csv'
IGNORE INTO TABLE `actual_orders`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`actual_order_id`, @order_status_code, `regular_order_id`, @aod);
COMMIT;


LOAD DATA INFILE '/var/lib/mysql-files/output_dir/actual_order_products.csv'
IGNORE INTO TABLE `actual_order_products`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`actual_order_id`,`product_id`);
COMMIT;


LOAD DATA INFILE '/var/lib/mysql-files/output_dir/customer_addresses.csv'
IGNORE INTO TABLE `customer_addresses`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`customer_id`,`address_id`, @df, @address_type, @dt);
COMMIT;


LOAD DATA INFILE '/var/lib/mysql-files/output_dir/delivery_routes.csv'
IGNORE INTO TABLE `delivery_routes`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`route_id`, @route_name, @other_route_details);
COMMIT;

LOAD DATA INFILE '/var/lib/mysql-files/output_dir/delivery_route_locations.csv'
IGNORE INTO TABLE `delivery_route_locations`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(@location_code,`route_id`,`location_address_id`, @location_name);
COMMIT;


LOAD DATA INFILE '/var/lib/mysql-files/output_dir/trucks.csv'
IGNORE INTO TABLE `trucks`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`truck_id`, @truck_licence_number, @truck_details);
COMMIT;

LOAD DATA INFILE '/var/lib/mysql-files/output_dir/employees.csv'
IGNORE INTO TABLE `employees`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`employee_id`,`employee_address_id`, @employee_name, @employee_phone);
COMMIT;



LOAD DATA INFILE '/var/lib/mysql-files/output_dir/order_deliveries.csv'
IGNORE INTO TABLE `order_deliveries`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(@location_code,`actual_order_id`, @delivery_status_code,`driver_employee_id`,`truck_id`, @dd);
COMMIT;


SET SQL_LOG_BIN = 1;
SET AUTOCOMMIT = 1;
SET UNIQUE_CHECKS = 1;
SET FOREIGN_KEY_CHECKS = 1;


UPDATE customers SET date_became_customer = NULL WHERE date_became_customer + 0 = 0;
UPDATE actual_orders SET actual_order_date = NULL WHERE actual_order_date + 0 = 0;
UPDATE order_deliveries SET delivery_date = NULL WHERE delivery_date + 0 = 0;
UPDATE customer_addresses SET date_from = '1970-01-01 00:00:00' WHERE date_from + 0 = 0;


UPDATE customer_addresses SET date_to = NULL WHERE date_to + 0 = 0;