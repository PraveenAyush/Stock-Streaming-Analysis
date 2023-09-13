CREATE TABLE IF NOT EXISTS stock_data (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    price DECIMAL,
    volume FLOAT,
    price_datetime TIMESTAMP(3)
);

