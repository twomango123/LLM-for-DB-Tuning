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
(`customer_id`, @payment_method, @customer_name, @customer_phone, @customer_email, `date_became_customer`);
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
(`actual_order_id`, @order_status_code, `regular_order_id`,`actual_order_date`);
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
(`customer_id`,`address_id`,`date_from`, @address_type, `date_to`);
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
(@location_code,`actual_order_id`, @delivery_status_code,`driver_employee_id`,`truck_id`,`delivery_date`);
COMMIT;


SET SQL_LOG_BIN = 1;
SET AUTOCOMMIT = 1;
SET UNIQUE_CHECKS = 1;
SET FOREIGN_KEY_CHECKS = 1;
