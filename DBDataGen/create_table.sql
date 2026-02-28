-- New DDL with generator annotations (HISTOGRAM / DISTRI / SKEW / RANGE / SET / RULER)

CREATE TABLE `products` (
`product_id` INT AUTO_INCREMENT,
`product_name` VARCHAR(20) RULER("Product_$"),
`product_price` DECIMAL(19,4) RANGE(0.50, 9999.99) DISTRI(NORMAL(50, 15)),
`product_description` VARCHAR(255) RULER("Description_$"),
-- 末尾显式构建主键约束
PRIMARY KEY (`product_id`)
)SIZE=1000000;

CREATE TABLE `addresses` (
`address_id` INT AUTO_INCREMENT,
`address_details` VARCHAR(80) RULER("$ Main St"),
`city` VARCHAR(50) HISTOGRAM({'Metropols': 0.01, 'Metropolis': 0.09, 'Gotham': 0.15, 'Springfield': 0.15, 'Star City': 0.10, 'Central City': 0.10}),
`zip_postcode` VARCHAR(20) HISTOGRAM({'9021O': 0.01}),
`state_province_county` VARCHAR(50) HISTOGRAM({'California': 0.30, 'New York': 0.12, 'Texas': 0.12, 'Washington': 0.09, 'Illinois': 0.08, 'Nevada': 0.07, 'Oregon': 0.07, 'Arizona': 0.06, 'Other': 0.09}),
`country` VARCHAR(50) HISTOGRAM({'USA': 0.95, 'Canada': 0.05}),
-- 末尾显式构建主键约束
PRIMARY KEY (`address_id`)
)SIZE=1000000;

CREATE TABLE `customers` (
`customer_id` INT AUTO_INCREMENT,
`payment_method` VARCHAR(10) NOT NULL SET('Visa','CARD','PayPal','Amex') HISTOGRAM({0: 0.50, 1: 0.25, 2: 0.15, 3: 0.10}),
`customer_name` VARCHAR(80) RULER("Customer_$"),
`customer_phone` VARCHAR(80) RULER("+1-555-$"),
`customer_email` VARCHAR(80) RULER("$@example.com"),
`date_became_customer` DATETIME,
-- 末尾显式构建主键约束
PRIMARY KEY (`customer_id`)
)SIZE=1000000;

CREATE TABLE `regular_orders` (
`regular_order_id` INT AUTO_INCREMENT,
`distributer_id` INT NOT NULL SKEW(0.25),
FOREIGN KEY (`distributer_id` ) REFERENCES `customers`(`customer_id` ),
-- 末尾显式构建主键约束
PRIMARY KEY (`regular_order_id`)
)SIZE=1000000;

CREATE TABLE `regular_order_products` (
`regular_order_id` INT NOT NULL SKEW(0.20),
`product_id` INT NOT NULL SKEW(0.30),
FOREIGN KEY (`product_id` ) REFERENCES `products`(`product_id` ),
FOREIGN KEY (`regular_order_id` ) REFERENCES `regular_orders`(`regular_order_id` ),
)SIZE=1000000;

CREATE TABLE `actual_orders` (
`actual_order_id` INT AUTO_INCREMENT,
`order_status_code` VARCHAR(10) NOT NULL SET('Success','NEW','PACKED','CANCELLED') HISTOGRAM({0: 0.35, 1: 0.20, 2: 0.25, 3: 0.20}),
`regular_order_id` INT NOT NULL SKEW(0.20),
`actual_order_date` DATETIME,
FOREIGN KEY (`regular_order_id` ) REFERENCES `regular_orders`(`regular_order_id` ),
-- 末尾显式构建主键约束
PRIMARY KEY (`actual_order_id`)
)SIZE=1000000;

CREATE TABLE `actual_order_products` (
`actual_order_id` INT NOT NULL SKEW(0.20),
`product_id` INT NOT NULL SKEW(0.25),
FOREIGN KEY (`product_id` ) REFERENCES `products`(`product_id` ),
FOREIGN KEY (`actual_order_id` ) REFERENCES `actual_orders`(`actual_order_id` ),
)SIZE=1000000;

CREATE TABLE `customer_addresses` (
`customer_id` INT NOT NULL SKEW(0.10),
`address_id` INT NOT NULL SKEW(0.10),
`date_from` DATETIME NOT NULL,
`address_type` VARCHAR(10) NOT NULL SET('BILLING','SHIPPING') HISTOGRAM({0: 0.50, 1: 0.50}),
`date_to` DATETIME,
FOREIGN KEY (`customer_id` ) REFERENCES `customers`(`customer_id` ),
FOREIGN KEY (`address_id` ) REFERENCES `addresses`(`address_id` ),
-- 补充：原表无显式主键，此处保持与原结构一致
)SIZE=1000000;

CREATE TABLE `delivery_routes` (
`route_id` INT AUTO_INCREMENT,
`route_name` VARCHAR(50) RULER("Route_$"),
`other_route_details` VARCHAR(255) RULER("Details_$"),
-- 末尾显式构建主键约束
PRIMARY KEY (`route_id`)
)SIZE=1000000;

CREATE TABLE `delivery_route_locations` (
`location_code` VARCHAR(10),
`route_id` INT NOT NULL SKEW(0.50),
`location_address_id` INT NOT NULL SKEW(0.10),
`location_name` VARCHAR(50) RULER("Loc_$"),
FOREIGN KEY (`location_address_id` ) REFERENCES `addresses`(`address_id` ),
FOREIGN KEY (`route_id` ) REFERENCES `delivery_routes`(`route_id` ),
-- 末尾显式构建主键约束
PRIMARY KEY (`location_code`)
)SIZE=1000000;

CREATE TABLE `trucks` (
`truck_id` INT AUTO_INCREMENT,
`truck_licence_number` VARCHAR(20) RULER("TRK-$"),
`truck_details` VARCHAR(255) RULER("Truck_$"),
-- 末尾显式构建主键约束
PRIMARY KEY (`truck_id`)
)SIZE=1000000;

CREATE TABLE `employees` (
`employee_id` INT AUTO_INCREMENT,
`employee_address_id` INT NOT NULL SKEW(0.05),
`employee_name` VARCHAR(80) RULER("Employee_$"),
`employee_phone` VARCHAR(80) RULER("+1-555-$"),
FOREIGN KEY (`employee_address_id` ) REFERENCES `addresses`(`address_id` ),
-- 末尾显式构建主键约束
PRIMARY KEY (`employee_id`)
)SIZE=1000000;

CREATE TABLE `order_deliveries` (
`location_code` VARCHAR(10) NOT NULL SKEW(0.20),
`actual_order_id` INT NOT NULL SKEW(0.20),
`delivery_status_code` VARCHAR(10) NOT NULL SET('SCHEDULED','IN_TRANSIT','DELIVERED') HISTOGRAM({0: 0.30, 1: 0.40, 2: 0.30}),
`driver_employee_id` INT NOT NULL SKEW(0.10),
`truck_id` INT NOT NULL SKEW(0.30),
`delivery_date` DATETIME,
FOREIGN KEY (`truck_id` ) REFERENCES `trucks`(`truck_id` ),
FOREIGN KEY (`actual_order_id` ) REFERENCES `actual_orders`(`actual_order_id` ),
FOREIGN KEY (`location_code` ) REFERENCES `delivery_route_locations`(`location_code` ),
FOREIGN KEY (`driver_employee_id` ) REFERENCES `employees`(`employee_id` ),
-- 补充：原表无显式主键，此处保持与原结构一致
)SIZE=1000000;
