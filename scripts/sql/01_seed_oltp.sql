-- ============================================================
-- RetailNova On-Prem SQL Server Source Database
-- Simulates legacy OLTP system being migrated to Azure
-- ============================================================

SET QUOTED_IDENTIFIER ON;
GO

USE master;
GO

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'RetailNova_OLTP')
BEGIN
    CREATE DATABASE RetailNova_OLTP;
END
GO

USE RetailNova_OLTP;
GO

-- ─── DROP TABLES IN DEPENDENCY ORDER (child → parent) ─────────────────────
IF OBJECT_ID('dbo.sales_order_lines', 'U') IS NOT NULL DROP TABLE dbo.sales_order_lines;
IF OBJECT_ID('dbo.sales_orders', 'U') IS NOT NULL DROP TABLE dbo.sales_orders;
IF OBJECT_ID('dbo.customers', 'U') IS NOT NULL DROP TABLE dbo.customers;
IF OBJECT_ID('dbo.products', 'U') IS NOT NULL DROP TABLE dbo.products;
IF OBJECT_ID('dbo.stores', 'U') IS NOT NULL DROP TABLE dbo.stores;
GO

-- ─── CUSTOMERS ────────────────────────────────────────────────────────────
CREATE TABLE dbo.customers (
    customer_id     INT            NOT NULL IDENTITY(1,1) PRIMARY KEY,
    first_name      NVARCHAR(100)  NOT NULL,
    last_name       NVARCHAR(100)  NOT NULL,
    email           NVARCHAR(200)  NOT NULL UNIQUE,
    phone           NVARCHAR(30),
    address_line1   NVARCHAR(200),
    address_line2   NVARCHAR(200),
    city            NVARCHAR(100),
    country         NVARCHAR(100)  NOT NULL DEFAULT 'Greece',
    postal_code     NVARCHAR(20),
    date_of_birth   DATE,
    gender          CHAR(1)        CHECK (gender IN ('M','F','O')),
    loyalty_tier    NVARCHAR(20)   DEFAULT 'Bronze' CHECK (loyalty_tier IN ('Bronze','Silver','Gold','Platinum')),
    created_at      DATETIME       NOT NULL DEFAULT GETDATE(),
    last_modified   DATETIME       NOT NULL DEFAULT GETDATE(),
    is_active       BIT            NOT NULL DEFAULT 1
);
GO

-- ─── PRODUCTS ─────────────────────────────────────────────────────────────
CREATE TABLE dbo.products (
    product_id      INT            NOT NULL IDENTITY(1,1) PRIMARY KEY,
    product_code    NVARCHAR(50)   NOT NULL UNIQUE,
    product_name    NVARCHAR(200)  NOT NULL,
    category        NVARCHAR(100)  NOT NULL,
    subcategory     NVARCHAR(100),
    brand           NVARCHAR(100),
    unit_price      DECIMAL(10,2)  NOT NULL,
    cost_price      DECIMAL(10,2),
    weight_kg       DECIMAL(8,3),
    is_active       BIT            NOT NULL DEFAULT 1,
    created_at      DATETIME       NOT NULL DEFAULT GETDATE(),
    last_modified   DATETIME       NOT NULL DEFAULT GETDATE()
);
GO

-- ─── STORES ───────────────────────────────────────────────────────────────
CREATE TABLE dbo.stores (
    store_id        INT            NOT NULL IDENTITY(1,1) PRIMARY KEY,
    store_code      NVARCHAR(20)   NOT NULL UNIQUE,
    store_name      NVARCHAR(200)  NOT NULL,
    store_type      NVARCHAR(50)   NOT NULL DEFAULT 'Physical' CHECK (store_type IN ('Physical','Online','Popup')),
    city            NVARCHAR(100),
    region          NVARCHAR(100),
    country         NVARCHAR(100)  NOT NULL DEFAULT 'Greece',
    manager_name    NVARCHAR(200),
    open_date       DATE,
    is_active       BIT            NOT NULL DEFAULT 1,
    created_at      DATETIME       NOT NULL DEFAULT GETDATE()
);
GO

-- ─── SALES ORDERS ─────────────────────────────────────────────────────────
CREATE TABLE dbo.sales_orders (
    order_id        INT            NOT NULL IDENTITY(1,1) PRIMARY KEY,
    order_number    NVARCHAR(50)   NOT NULL UNIQUE,
    customer_id     INT            NOT NULL REFERENCES dbo.customers(customer_id),
    store_id        INT            NOT NULL REFERENCES dbo.stores(store_id),
    order_date      DATETIME       NOT NULL,
    required_date   DATETIME,
    shipped_date    DATETIME,
    status          NVARCHAR(30)   NOT NULL DEFAULT 'Pending' CHECK (status IN ('Pending','Processing','Shipped','Delivered','Cancelled','Returned')),
    shipping_cost   DECIMAL(8,2)   DEFAULT 0,
    discount_pct    DECIMAL(5,2)   DEFAULT 0,
    tax_amount      DECIMAL(10,2)  DEFAULT 0,
    total_amount    DECIMAL(12,2),
    payment_method  NVARCHAR(50),
    notes           NVARCHAR(500),
    created_at      DATETIME       NOT NULL DEFAULT GETDATE(),
    last_modified   DATETIME       NOT NULL DEFAULT GETDATE()
);
GO

-- ─── SALES ORDER LINES ────────────────────────────────────────────────────
CREATE TABLE dbo.sales_order_lines (
    line_id         INT            NOT NULL IDENTITY(1,1) PRIMARY KEY,
    order_id        INT            NOT NULL REFERENCES dbo.sales_orders(order_id),
    product_id      INT            NOT NULL REFERENCES dbo.products(product_id),
    quantity        INT            NOT NULL,
    unit_price      DECIMAL(10,2)  NOT NULL,
    discount_amt    DECIMAL(10,2)  DEFAULT 0,
    line_total      AS (quantity * unit_price - discount_amt) PERSISTED,
    created_at      DATETIME       NOT NULL DEFAULT GETDATE(),
    last_modified   DATETIME       NOT NULL DEFAULT GETDATE()
);
GO

-- ─── SEED STORES ──────────────────────────────────────────────────────────
INSERT INTO dbo.stores (store_code, store_name, store_type, city, region, country, manager_name, open_date) VALUES
('STR-ATH-001', 'RetailNova Athens Center',    'Physical', 'Athens',       'Attica',        'Greece', 'Nikos Papadopoulos', '2018-03-15'),
('STR-ATH-002', 'RetailNova Athens South',     'Physical', 'Athens',       'Attica',        'Greece', 'Maria Georgiou',     '2019-06-01'),
('STR-THS-001', 'RetailNova Thessaloniki',     'Physical', 'Thessaloniki', 'Central Macedonia', 'Greece', 'Kostas Nikolaou', '2020-01-10'),
('STR-PAT-001', 'RetailNova Patras',           'Physical', 'Patras',       'Western Greece','Greece', 'Elena Stavrou',      '2021-04-20'),
('STR-ONL-001', 'RetailNova Online',           'Online',   NULL,           NULL,            'Greece', 'Dimitris Alexiou',   '2019-01-01');
GO

-- ─── SEED PRODUCTS ────────────────────────────────────────────────────────
INSERT INTO dbo.products (product_code, product_name, category, subcategory, brand, unit_price, cost_price, weight_kg) VALUES
('ELEC-001', 'Smartphone Pro X15',         'Electronics', 'Smartphones',   'TechBrand',   899.99, 550.00, 0.185),
('ELEC-002', 'Wireless Headphones ANC',    'Electronics', 'Audio',         'SoundMax',    199.99, 100.00, 0.250),
('ELEC-003', 'Laptop UltraBook 14"',       'Electronics', 'Computers',     'CompuTech',  1299.99, 800.00, 1.400),
('ELEC-004', 'Smart Watch Series 5',       'Electronics', 'Wearables',     'WearTech',    349.99, 180.00, 0.045),
('ELEC-005', 'Bluetooth Speaker Mini',     'Electronics', 'Audio',         'SoundMax',     79.99,  35.00, 0.300),
('HOME-001', 'Coffee Machine Deluxe',      'Home',        'Kitchen',       'HomePro',     249.99, 120.00, 3.200),
('HOME-002', 'Air Purifier 300m2',         'Home',        'Air Quality',   'CleanAir',    399.99, 200.00, 5.500),
('HOME-003', 'Smart LED Bulb Pack 4',      'Home',        'Lighting',      'BrightHome',   39.99,  15.00, 0.400),
('HOME-004', 'Robot Vacuum Cleaner',       'Home',        'Cleaning',      'CleanBot',    499.99, 260.00, 3.800),
('HOME-005', 'Electric Kettle 1.7L',       'Home',        'Kitchen',       'HomePro',      49.99,  20.00, 0.900),
('CLTH-001', 'Premium Cotton T-Shirt',     'Clothing',    'Tops',          'FashionNova',  29.99,  10.00, 0.200),
('CLTH-002', 'Slim Fit Jeans Dark',        'Clothing',    'Bottoms',       'DenimCo',      79.99,  30.00, 0.600),
('CLTH-003', 'Winter Jacket Thermal',      'Clothing',    'Outerwear',     'WarmStyle',   149.99,  65.00, 0.900),
('CLTH-004', 'Sports Running Shoes',       'Clothing',    'Footwear',      'ActiveRun',   119.99,  55.00, 0.700),
('CLTH-005', 'Leather Wallet Slim',        'Clothing',    'Accessories',   'LuxeGoods',    49.99,  18.00, 0.100),
('FOOD-001', 'Organic Greek Honey 500g',   'Food',        'Natural',       'GreekNature',  14.99,   6.00, 0.550),
('FOOD-002', 'Premium Olive Oil 1L',       'Food',        'Oils',          'GreekNature',  19.99,   8.00, 1.000),
('FOOD-003', 'Dark Chocolate 72% 200g',    'Food',        'Confectionery', 'ChocoPure',     8.99,   3.50, 0.200),
('FOOD-004', 'Protein Bar Box 12pcs',      'Food',        'Health',        'FitFood',      24.99,  11.00, 0.600),
('FOOD-005', 'Green Tea Premium 50bags',   'Food',        'Beverages',     'TeaTime',      12.99,   5.00, 0.150);
GO

-- ─── SEED CUSTOMERS ───────────────────────────────────────────────────────
INSERT INTO dbo.customers (first_name, last_name, email, phone, address_line1, city, country, postal_code, date_of_birth, gender, loyalty_tier, created_at, last_modified) VALUES
('Alexandros', 'Papadimitriou', 'alex.papadimitriou@email.gr',  '+30-210-1234567', 'Ermou 12',         'Athens',       'Greece', '10563', '1985-03-15', 'M', 'Gold',     '2022-01-10', '2024-06-01'),
('Sofia',       'Georgiou',      'sofia.georgiou@email.gr',      '+30-210-2345678', 'Syntagma 5',       'Athens',       'Greece', '10558', '1990-07-22', 'F', 'Platinum', '2021-05-20', '2024-11-15'),
('Nikos',       'Stavrakis',     'nikos.stavrakis@email.gr',     '+30-231-3456789', 'Aristotelous 8',   'Thessaloniki', 'Greece', '54623', '1978-11-08', 'M', 'Silver',   '2020-03-12', '2024-08-20'),
('Maria',       'Konstantinou',  'maria.konstantinou@email.gr',  '+30-261-4567890', 'Korinthou 22',     'Patras',       'Greece', '26221', '1995-02-14', 'F', 'Bronze',   '2023-06-05', '2023-06-05'),
('Dimitris',    'Alexopoulos',   'dimitris.alex@email.gr',       '+30-210-5678901', 'Kifisias 45',      'Athens',       'Greece', '15231', '1982-09-30', 'M', 'Gold',     '2021-11-18', '2024-09-10'),
('Eleni',       'Papadopoulou',  'eleni.papa@email.gr',          '+30-210-6789012', 'Vouliagmenis 100', 'Athens',       'Greece', '16675', '1988-04-25', 'F', 'Silver',   '2022-08-03', '2024-12-01'),
('Kostas',      'Nikolaou',      'kostas.nikolaou@email.gr',     '+30-231-7890123', 'Tsimiski 33',      'Thessaloniki', 'Greece', '54624', '1975-12-01', 'M', 'Platinum', '2019-09-15', '2024-10-20'),
('Anna',        'Vassilakis',    'anna.vassilakis@email.gr',     '+30-210-8901234', 'Patission 56',     'Athens',       'Greece', '11141', '1993-06-18', 'F', 'Bronze',   '2024-01-08', '2024-01-08'),
('Petros',      'Karagiannis',   'petros.kara@email.gr',         '+30-261-9012345', 'Maizonos 15',      'Patras',       'Greece', '26222', '1980-08-11', 'M', 'Gold',     '2020-07-22', '2024-07-05'),
('Christina',   'Dimitriou',     'christina.dim@email.gr',       '+30-210-0123456', 'Chalandri 78',     'Athens',       'Greece', '15232', '1997-01-29', 'F', 'Silver',   '2022-12-14', '2024-11-30');
GO

-- ─── SEED ORDERS (last 90 days) ───────────────────────────────────────────
DECLARE @base_date DATETIME = DATEADD(DAY, -90, GETDATE());

INSERT INTO dbo.sales_orders (order_number, customer_id, store_id, order_date, status, shipping_cost, discount_pct, payment_method, total_amount, last_modified) VALUES
('ORD-2024-001001', 1, 1, DATEADD(DAY, -88, GETDATE()), 'Delivered',  5.99, 0.00,  'Credit Card',  929.97, DATEADD(DAY,-85,GETDATE())),
('ORD-2024-001002', 2, 5, DATEADD(DAY, -87, GETDATE()), 'Delivered',  0.00, 10.00, 'PayPal',       359.99, DATEADD(DAY,-84,GETDATE())),
('ORD-2024-001003', 3, 3, DATEADD(DAY, -85, GETDATE()), 'Delivered',  5.99, 0.00,  'Credit Card',  219.97, DATEADD(DAY,-82,GETDATE())),
('ORD-2024-001004', 4, 4, DATEADD(DAY, -82, GETDATE()), 'Delivered',  5.99, 5.00,  'Cash',          84.99, DATEADD(DAY,-79,GETDATE())),
('ORD-2024-001005', 5, 1, DATEADD(DAY, -80, GETDATE()), 'Delivered',  5.99, 0.00,  'Credit Card', 1349.97, DATEADD(DAY,-77,GETDATE())),
('ORD-2024-001006', 1, 5, DATEADD(DAY, -75, GETDATE()), 'Delivered',  0.00, 15.00, 'PayPal',       167.99, DATEADD(DAY,-72,GETDATE())),
('ORD-2024-001007', 6, 2, DATEADD(DAY, -70, GETDATE()), 'Delivered',  5.99, 0.00,  'Debit Card',   449.97, DATEADD(DAY,-67,GETDATE())),
('ORD-2024-001008', 7, 3, DATEADD(DAY, -65, GETDATE()), 'Delivered',  5.99, 0.00,  'Credit Card',  699.97, DATEADD(DAY,-62,GETDATE())),
('ORD-2024-001009', 2, 5, DATEADD(DAY, -60, GETDATE()), 'Delivered',  0.00, 5.00,  'PayPal',       524.98, DATEADD(DAY,-57,GETDATE())),
('ORD-2024-001010', 8, 1, DATEADD(DAY, -58, GETDATE()), 'Delivered',  5.99, 0.00,  'Credit Card',   85.97, DATEADD(DAY,-55,GETDATE())),
('ORD-2024-001011', 9, 4, DATEADD(DAY, -55, GETDATE()), 'Shipped',    5.99, 0.00,  'Cash',         529.97, DATEADD(DAY,-53,GETDATE())),
('ORD-2024-001012', 3, 5, DATEADD(DAY, -50, GETDATE()), 'Delivered',  0.00, 0.00,  'Credit Card',  399.99, DATEADD(DAY,-47,GETDATE())),
('ORD-2024-001013', 5, 1, DATEADD(DAY, -45, GETDATE()), 'Delivered',  5.99, 10.00, 'Debit Card',  1169.99, DATEADD(DAY,-42,GETDATE())),
('ORD-2024-001014', 10, 2, DATEADD(DAY, -40, GETDATE()), 'Delivered', 5.99, 0.00,  'Credit Card',  269.97, DATEADD(DAY,-37,GETDATE())),
('ORD-2024-001015', 1, 5, DATEADD(DAY, -35, GETDATE()), 'Delivered',  0.00, 0.00,  'PayPal',       899.99, DATEADD(DAY,-32,GETDATE())),
('ORD-2024-001016', 4, 3, DATEADD(DAY, -30, GETDATE()), 'Processing', 5.99, 0.00,  'Cash',         159.97, DATEADD(DAY,-29,GETDATE())),
('ORD-2024-001017', 6, 5, DATEADD(DAY, -25, GETDATE()), 'Shipped',    0.00, 5.00,  'PayPal',       284.99, DATEADD(DAY,-24,GETDATE())),
('ORD-2024-001018', 7, 1, DATEADD(DAY, -20, GETDATE()), 'Processing', 5.99, 0.00,  'Credit Card',  349.99, DATEADD(DAY,-19,GETDATE())),
('ORD-2024-001019', 2, 5, DATEADD(DAY, -15, GETDATE()), 'Processing', 0.00, 0.00,  'PayPal',      1299.99, DATEADD(DAY,-14,GETDATE())),
('ORD-2024-001020', 9, 4, DATEADD(DAY,  -7, GETDATE()), 'Pending',    5.99, 0.00,  'Credit Card',  119.97, DATEADD(DAY, -6,GETDATE()));
GO

-- ─── SEED ORDER LINES ─────────────────────────────────────────────────────
INSERT INTO dbo.sales_order_lines (order_id, product_id, quantity, unit_price, discount_amt) VALUES
(1, 1, 1, 899.99, 0),   (1, 2, 1, 199.99, 170.00),
(2, 4, 1, 349.99, 0),   (2, 3, 0, 0, 0),
(3, 2, 1, 199.99, 0),   (3, 5, 1, 79.99, 0),
(4, 11, 2, 29.99, 0),   (4, 15, 1, 49.99, 25.00),
(5, 3, 1, 1299.99, 0),  (5, 5, 1, 79.99, 30.00),
(6, 2, 1, 199.99, 32.00),
(7, 7, 1, 399.99, 0),   (7, 8, 1, 39.99, 0),
(8, 1, 0, 899.99, 0),   (8, 4, 0, 349.99, 150.00),
(9, 6, 1, 249.99, 0),   (9, 4, 1, 349.99, 75.00),
(10, 16, 2, 14.99, 0),  (10, 17, 1, 19.99, 0), (10, 18, 2, 8.99, 0),
(11, 9, 1, 499.99, 0),  (11, 5, 1, 79.99, 50.00),
(12, 7, 1, 399.99, 0),
(13, 3, 1, 1299.99, 130.00),
(14, 13, 1, 149.99, 0), (14, 14, 1, 119.99, 0),
(15, 1, 1, 899.99, 0),
(16, 12, 2, 79.99, 0),
(17, 6, 1, 249.99, 0),  (17, 8, 1, 39.99, 5.00),
(18, 4, 1, 349.99, 0),
(19, 3, 1, 1299.99, 0),
(20, 14, 1, 119.99, 0);
GO

-- ─── WATERMARK TABLE (tracks CDC state) ───────────────────────────────────
IF OBJECT_ID('dbo.pipeline_watermarks', 'U') IS NOT NULL DROP TABLE dbo.pipeline_watermarks;
CREATE TABLE dbo.pipeline_watermarks (
    table_name          NVARCHAR(100) NOT NULL PRIMARY KEY,
    last_watermark      DATETIME      NOT NULL DEFAULT '1900-01-01',
    last_run_at         DATETIME,
    rows_extracted      INT           DEFAULT 0,
    status              NVARCHAR(20)  DEFAULT 'Never Run'
);

INSERT INTO dbo.pipeline_watermarks (table_name, last_watermark) VALUES
('customers',       '1900-01-01'),
('products',        '1900-01-01'),
('stores',          '1900-01-01'),
('sales_orders',    '1900-01-01'),
('sales_order_lines','1900-01-01');
GO

PRINT 'RetailNova OLTP database seeded successfully!';
GO
