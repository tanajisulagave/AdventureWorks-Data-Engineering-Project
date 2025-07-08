
------------------
-- 1. CREATE EXTERNAL TABLE for Sales (CETAS)
------------------

CREATE EXTERNAL TABLE gold.extsales
WITH
(
    LOCATION='extsales', -- ADLS location
    DATA_SOURCE = source_gold,
    FILE_FORMAT= format_parquet
) AS

SELECT * from gold.Sales  --- view called

-------- Execute CETAS
select * from gold.extsales

------------------
-- 2. CREATE EXTERNAL TABLE for Calender (CETAS)
------------------
CREATE EXTERNAL TABLE gold.extcalender
WITH
(
    LOCATION='extcalender',
    DATA_SOURCE = source_gold,
    FILE_FORMAT= format_parquet
) AS

SELECT * from gold.calender  --- view called

-------- Execute CETAS
select * from gold.extcalender

------------------
-- 3. CREATE EXTERNAL TABLE for Customers (CETAS)
------------------
CREATE EXTERNAL TABLE gold.extCustomers
WITH
(
    LOCATION='extCustomers',
    DATA_SOURCE = source_gold,
    FILE_FORMAT= format_parquet
) AS

SELECT * from gold.Customers  --- Calender view called

-------- Execute CETAS
select * from gold.extCustomers


------------------
-- 4. CREATE EXTERNAL TABLE for Products (CETAS)
------------------
CREATE EXTERNAL TABLE gold.extProducts
WITH
(
    LOCATION='extProducts',
    DATA_SOURCE = source_gold,
    FILE_FORMAT= format_parquet
) AS

SELECT * from gold.Products  --- Calender view called

-------- Execute CETAS
select * from gold.extProducts


------------------
-- 5. CREATE EXTERNAL TABLE for Returns (CETAS)
------------------
CREATE EXTERNAL TABLE gold.extReturns
WITH
(
    LOCATION='extReturns',
    DATA_SOURCE = source_gold,
    FILE_FORMAT= format_parquet
) AS

SELECT * from gold.Returns  --- Calender view called

-------- Execute CETAS
select * from gold.extReturns


------------------
-- 6. CREATE EXTERNAL TABLE for SubCategories (CETAS)
------------------
CREATE EXTERNAL TABLE gold.extSubCategories
WITH
(
    LOCATION='extSubCategories',
    DATA_SOURCE = source_gold,
    FILE_FORMAT= format_parquet
) AS

SELECT * from gold.SubCategories  --- Calender view called

-------- Execute CETAS
select * from gold.extSubCategories

------------------
-- 7. CREATE EXTERNAL TABLE for Territories (CETAS)
------------------
CREATE EXTERNAL TABLE gold.extTerritories
WITH
(
    LOCATION='extTerritories',
    DATA_SOURCE = source_gold,
    FILE_FORMAT= format_parquet
) AS

SELECT * from gold.Territories  --- Calender view called

-------- Execute CETAS
select * from gold.extTerritories