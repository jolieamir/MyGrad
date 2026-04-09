-- =============================================
-- MyGrad: Full SQL Server Database Setup
-- Creates all tables required by the application
-- Matches the schema from x.ipynb (TestDB)
-- =============================================

USE [master]
GO

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'TestDB')
BEGIN
    CREATE DATABASE [TestDB];
END
GO

USE [TestDB]
GO

-- =============================================
-- 1. Row_data: Raw CSV data (no modifications)
-- =============================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Row_data')
BEGIN
    CREATE TABLE [dbo].[Row_data](
        [InvoiceNo]   [varchar](100)  NULL,
        [StockCode]   [varchar](100)  NULL,
        [Description] [varchar](1000) NULL,
        [Quantity]    [varchar](1000) NULL,
        [InvoiceDate] [varchar](100)  NULL,
        [UnitPrice]   [decimal](28,4) NULL,
        [CustomerID]  [varchar](100)  NULL,
        [Country]     [varchar](1000) NULL
    ) ON [PRIMARY];
END
GO

-- =============================================
-- 2. Cleaned_data: Processed data after cleaning
-- =============================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Cleaned_data')
BEGIN
    CREATE TABLE [dbo].[Cleaned_data](
        [InvoiceNo]   [varchar](100)  NULL,
        [StockCode]   [varchar](100)  NULL,
        [Description] [varchar](1000) NULL,
        [Quantity]    [varchar](1000) NULL,
        [InvoiceDate] [varchar](100)  NULL,
        [UnitPrice]   [decimal](28,4) NULL,
        [CustomerID]  [varchar](100)  NULL,
        [Country]     [varchar](1000) NULL
    ) ON [PRIMARY];
END
GO

-- =============================================
-- 3. tableed: Aggregated transactions (for mining)
-- =============================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'tableed')
BEGIN
    CREATE TABLE [dbo].[tableed](
        [InvoiceNo] [varchar](100) NULL,
        [products]  [varchar](max) NULL
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
END
GO

-- =============================================
-- 4. FPResults: FP-Growth frequent itemset results
-- =============================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'FPResults')
BEGIN
    CREATE TABLE [dbo].[FPResults](
        [ID]        [int] IDENTITY(1,1) NOT NULL PRIMARY KEY,
        [Items]     [varchar](max) NULL,
        [Frequency] [int] NULL
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
END
GO

-- =============================================
-- 5. new_table: Unique product names
-- =============================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'new_table')
BEGIN
    CREATE TABLE [dbo].[new_table](
        [Product] [varchar](1000) NULL
    ) ON [PRIMARY];
END
GO

-- =============================================
-- 6. user: Admin authentication
-- =============================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'user')
BEGIN
    CREATE TABLE [dbo].[user](
        [id]            [int] IDENTITY(1,1) NOT NULL PRIMARY KEY,
        [username]      [varchar](64)  NOT NULL,
        [password_hash] [varchar](256) NOT NULL,
        [created_at]    [datetime] NULL
    ) ON [PRIMARY];
END
GO

-- =============================================
-- 7. product: Product catalog
-- =============================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'product')
BEGIN
    CREATE TABLE [dbo].[product](
        [id]          [int] IDENTITY(1,1) NOT NULL PRIMARY KEY,
        [name]        [varchar](200) NOT NULL,
        [category]    [varchar](100) NULL,
        [price]       [float] NOT NULL,
        [image_url]   [varchar](500) NULL,
        [description] [varchar](max) NULL,
        [stock_code]  [varchar](50)  NULL
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
END
GO

-- =============================================
-- 8. order: Customer orders
-- =============================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'order')
BEGIN
    CREATE TABLE [dbo].[order](
        [id]             [int] IDENTITY(1,1) NOT NULL PRIMARY KEY,
        [order_number]   [varchar](20)  NOT NULL,
        [customer_name]  [varchar](100) NOT NULL,
        [customer_email] [varchar](120) NULL,
        [total_amount]   [float] NOT NULL,
        [status]         [varchar](20)  NULL,
        [created_at]     [datetime] NULL
    ) ON [PRIMARY];
END
GO

-- =============================================
-- 9. order_item: Items within an order
-- =============================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'order_item')
BEGIN
    CREATE TABLE [dbo].[order_item](
        [id]         [int] IDENTITY(1,1) NOT NULL PRIMARY KEY,
        [order_id]   [int]   NOT NULL,
        [product_id] [int]   NOT NULL,
        [quantity]   [int]   NOT NULL,
        [price]      [float] NOT NULL
    ) ON [PRIMARY];
END
GO

-- =============================================
-- 10. recommendation: Mining-generated recommendations
-- =============================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'recommendation')
BEGIN
    CREATE TABLE [dbo].[recommendation](
        [id]                  [int] IDENTITY(1,1) NOT NULL PRIMARY KEY,
        [product_id]          [int]   NOT NULL,
        [recommended_with_id] [int]   NOT NULL,
        [confidence]          [float] NULL,
        [support]             [float] NULL,
        [lift]                [float] NULL,
        [algorithm]           [varchar](50) DEFAULT 'fpgrowth'
    ) ON [PRIMARY];
END
GO

-- =============================================
-- 11. mining_result: Algorithm execution results
-- =============================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'mining_result')
BEGIN
    CREATE TABLE [dbo].[mining_result](
        [id]         [int] IDENTITY(1,1) NOT NULL PRIMARY KEY,
        [algorithm]  [varchar](50) NOT NULL,
        [parameters] [varchar](max) NULL,
        [results]    [varchar](max) NULL,
        [created_at] [datetime] NULL,
        [created_by] [varchar](64) NULL
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
END
GO

-- =============================================
-- Indexes for data pipeline performance
-- =============================================
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Row_data_InvoiceNo')
    CREATE INDEX IX_Row_data_InvoiceNo ON Row_data(InvoiceNo);
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Cleaned_data_InvoiceNo')
    CREATE INDEX IX_Cleaned_data_InvoiceNo ON Cleaned_data(InvoiceNo);
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Cleaned_data_Description')
    CREATE INDEX IX_Cleaned_data_Description ON Cleaned_data(Description);
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Cleaned_data_StockCode')
    CREATE INDEX IX_Cleaned_data_StockCode ON Cleaned_data(StockCode);
GO

PRINT 'All tables created successfully.';
GO
