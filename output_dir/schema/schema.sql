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
