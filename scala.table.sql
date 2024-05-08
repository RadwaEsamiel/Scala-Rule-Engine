CREATE TABLE orders (
  order_date DATE,
  expiry_date DATE,
  product_name VARCHAR2(255),
  quantity INT,
  unit_price NUMBER(10, 2),
  channel VARCHAR2(50),
  payment_method VARCHAR2(50),
  discount NUMBER(10, 2),
  original_price NUMBER(10, 2),
  final_price NUMBER(10, 2)
);
