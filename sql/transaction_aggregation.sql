-- =============================================
-- MyGrad: Transaction Aggregation for Market Basket Analysis
-- Groups items by InvoiceNo using STRING_AGG (SQL Server 2017+)
-- =============================================

USE TestDB;
GO

-- Aggregate transactions: one row per InvoiceNo with all items
-- Uses DISTINCT inside STRING_AGG to remove duplicate items within same transaction
SELECT
    InvoiceNo,
    STRING_AGG(CAST(Description AS VARCHAR(MAX)), ', ') AS Products
FROM (
    SELECT DISTINCT InvoiceNo, Description
    FROM Cleaned_data
    WHERE Description IS NOT NULL
      AND LTRIM(RTRIM(Description)) <> ''
) AS DistinctItems
GROUP BY InvoiceNo
ORDER BY InvoiceNo;
GO
