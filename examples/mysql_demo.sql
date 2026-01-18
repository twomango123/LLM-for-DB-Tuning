-- Minimal demo schema and data for cardinality/storage tests
DROP DATABASE IF EXISTS codex_demo;
CREATE DATABASE codex_demo;
USE codex_demo;

-- Orders table
DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
  o_orderkey INT PRIMARY KEY,
  o_orderstatus CHAR(1)
);

INSERT INTO orders (o_orderkey, o_orderstatus) VALUES
  (1, 'F'),
  (2, 'O'),
  (3, 'F'),
  (4, 'P');

-- Lineitem table
DROP TABLE IF EXISTS lineitem;
CREATE TABLE lineitem (
  l_orderkey INT,
  l_linenumber INT,
  l_partkey INT,
  l_shipdate DATE,
  l_comment VARCHAR(32),
  PRIMARY KEY (l_orderkey, l_linenumber)
);

INSERT INTO lineitem (l_orderkey, l_linenumber, l_partkey, l_shipdate, l_comment) VALUES
  (1, 1, 10, '1995-01-01', 'first line'),
  (1, 2, 11, '1995-01-03', 'second line'),
  (2, 1, 12, '1995-02-01', 'something'),
  (3, 1, 13, '1995-03-01', 'hello'),
  (4, 1, 14, '1995-04-01', 'world');

-- Simple index to help optimizer (optional)
CREATE INDEX idx_orders_status ON orders(o_orderstatus);
CREATE INDEX idx_lineitem_okey ON lineitem(l_orderkey);

