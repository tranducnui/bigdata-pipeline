  -- Create tables for BigDataDB

CREATE TABLE customer_spending (
    customer   VARCHAR(100),
    total_spent FLOAT,
    created_at  DATETIME DEFAULT DATEADD(HOUR, 7, GETUTCDATE())
);

CREATE TABLE sales_realtime (
    id         INT IDENTITY(1,1) PRIMARY KEY,
    customer   VARCHAR(100),
    product    VARCHAR(100),
    amount     FLOAT,
    created_at DATETIME DEFAULT DATEADD(HOUR, 7, GETUTCDATE())
);