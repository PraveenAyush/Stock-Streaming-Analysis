CREATE TABLE IF NOT EXISTS stock_data (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    price DECIMAL,
    symbol VARCHAR(20),
    data_datetime TIMESTAMP(3),
    volume FLOAT
);

