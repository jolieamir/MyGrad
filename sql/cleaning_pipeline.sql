-- =============================================
-- MyGrad: Data Cleaning Pipeline
-- Run after inserting raw CSV data into Row_data
-- =============================================

USE TestDB;
GO

-- Step 1: Truncate Cleaned_data
TRUNCATE TABLE Cleaned_data;

-- Step 2: Copy all raw data into Cleaned_data
INSERT INTO Cleaned_data
SELECT * FROM Row_data;

-- Step 3: Apply cleaning rules
-- Remove cancelled transactions (InvoiceNo contains 'C')
DELETE FROM Cleaned_data WHERE InvoiceNo LIKE '%C%';

-- Remove anonymous users (no CustomerID)
DELETE FROM Cleaned_data WHERE CustomerID IS NULL;

-- Remove negative quantities
DELETE FROM Cleaned_data WHERE Quantity LIKE '%-%';

-- Remove zero or negative prices
DELETE FROM Cleaned_data WHERE UnitPrice = 0 OR UnitPrice LIKE '%-%';

-- Remove null/invalid descriptions
DELETE FROM Cleaned_data WHERE Description IS NULL OR Description LIKE '%?%';

-- Remove non-product entries (POST = postage charges)
DELETE FROM Cleaned_data WHERE StockCode = 'POST';

-- Verify results
SELECT 'Row_data count' AS metric, COUNT(*) AS value FROM Row_data
UNION ALL
SELECT 'Cleaned_data count', COUNT(*) FROM Cleaned_data
UNION ALL
SELECT 'Unique invoices', COUNT(DISTINCT InvoiceNo) FROM Cleaned_data
UNION ALL
SELECT 'Unique products', COUNT(DISTINCT Description) FROM Cleaned_data;
GO
