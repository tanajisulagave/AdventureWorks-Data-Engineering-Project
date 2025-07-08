-----------------------
-- CREATE VIEW CALENDER 
-----------------------
CREATE VIEW gold.calender
AS
SELECT 
    * 
FROM OPENROWSET(
    BULK 'https://awstoragedatalaketanaji.dfs.core.windows.net/silver/AdventureWorks_Calendar/',
    FORMAT='PARQUET'
) as query1

-----------------------
-- CREATE VIEW CUSTOMERS 
-----------------------
CREATE VIEW gold.Customers
AS
SELECT 
    * 
FROM OPENROWSET(
    BULK 'https://awstoragedatalaketanaji.dfs.core.windows.net/silver/AdventureWorks_Customers/',
    FORMAT='PARQUET'
) as query1

-----------------------
-- CREATE VIEW PRODUCTS 
-----------------------
CREATE VIEW gold.Products
AS
SELECT 
    * 
FROM OPENROWSET(
    BULK 'https://awstoragedatalaketanaji.dfs.core.windows.net/silver/AdventureWorks_Products/',
    FORMAT='PARQUET'
) as query1

-----------------------
-- CREATE VIEW RETURNS 
-----------------------
CREATE VIEW gold.Returns
AS
SELECT 
    * 
FROM OPENROWSET(
    BULK 'https://awstoragedatalaketanaji.dfs.core.windows.net/silver/AdventureWorks_Returns/',
    FORMAT='PARQUET'
) as query1

-----------------------
-- CREATE VIEW SALES 
-----------------------
CREATE VIEW gold.Sales
AS
SELECT 
    * 
FROM OPENROWSET(
    BULK 'https://awstoragedatalaketanaji.dfs.core.windows.net/silver/AdventureWorks_Sales/',
    FORMAT='PARQUET'
) as query1

-----------------------
-- CREATE VIEW SUB-CATEGORIES 
-----------------------
CREATE VIEW gold.SubCategories
AS
SELECT 
    * 
FROM OPENROWSET(
    BULK 'https://awstoragedatalaketanaji.dfs.core.windows.net/silver/AdventureWorks_SubCategories/',
    FORMAT='PARQUET'
) as query1

-----------------------
-- CREATE VIEW TERRITORIES
-----------------------
CREATE VIEW gold.Territories
AS
SELECT 
    * 
FROM OPENROWSET(
    BULK 'https://awstoragedatalaketanaji.dfs.core.windows.net/silver/AdventureWorks_Territories/',
    FORMAT='PARQUET'
) as query1