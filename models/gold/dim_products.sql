CREATE TABLE IF NOT EXISTS dev_catalog.gold.dim_products (
    product_id INT,
    product_name STRING,
    category STRING,
    price DECIMAL(10, 2)
);

INSERT INTO dev_catalog.gold.dim_products (product_id, product_name, category, price)
VALUES
    (1, 'Laptop', 'Electronics', 1200.00),
    (2, 'Mouse', 'Electronics', 25.00);