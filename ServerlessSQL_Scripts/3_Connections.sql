--------------------
-- Create master Key
--------------------
CREATE MASTER KEY ENCRYPTION BY PASSWORD ='Abcd1234**';

------------------
-- Database scope credentials
------------------
CREATE DATABASE SCOPED CREDENTIAL cred_tanaji
WITH
    IDENTITY='Managed Identity' ;

------------------
-- CREATE External Data Source
------------------
---- For silver Layer
CREATE EXTERNAL DATA SOURCE source_silver
WITH
(
    LOCATION='https://awstoragedatalaketanaji.dfs.core.windows.net/silver',
    CREDENTIAL = cred_tanaji
)
---- For Gold Layer
CREATE EXTERNAL DATA SOURCE source_gold
WITH
(
    LOCATION='https://awstoragedatalaketanaji.dfs.core.windows.net/gold',
    CREDENTIAL = cred_tanaji
)

------------------
-- CREATE External File Format
------------------
CREATE EXTERNAL FILE FORMAT format_parquet
WITH
(
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)
