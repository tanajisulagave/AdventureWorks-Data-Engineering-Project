# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # SILVER LAYER SCRIPT

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA ACCESS USING APP

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.awstoragedatalaketanaji.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.awstoragedatalaketanaji.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.awstoragedatalaketanaji.dfs.core.windows.net","47bacbd9-df34-4b1c-ba89-cc373bb260bd")
spark.conf.set("fs.azure.account.oauth2.client.secret.awstoragedatalaketanaji.dfs.core.windows.net", "vRq8Q~XN4taNVV3Vb0XmWiMmeAwjGJT4jq2.6ayJ")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.awstoragedatalaketanaji.dfs.core.windows.net", "https://login.microsoftonline.com/9d894dd8-f4b4-4748-bc76-99871acb6201/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA LOADING

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Calender Table Data

# COMMAND ----------

df_cal=spark.read.format('csv')\
        .option("header",True)\
        .option("inferSchema",True)\
        .load('abfss://bronze@awstoragedatalaketanaji.dfs.core.windows.net/AdventureWorks_Calendar')

# COMMAND ----------

df_cus=spark.read.format('csv')\
        .option("header",True)\
        .option("inferSchema",True)\
        .load('abfss://bronze@awstoragedatalaketanaji.dfs.core.windows.net/AdventureWorks_Customers')

# COMMAND ----------

df_procat=spark.read.format('csv')\
        .option("header",True)\
        .option("inferSchema",True)\
        .load('abfss://bronze@awstoragedatalaketanaji.dfs.core.windows.net/AdventureWorks_Product_Categories')

# COMMAND ----------

df_pro=spark.read.format('csv')\
        .option("header",True)\
        .option("inferSchema",True)\
        .load('abfss://bronze@awstoragedatalaketanaji.dfs.core.windows.net/AdventureWorks_Products')

# COMMAND ----------

df_ret=spark.read.format('csv')\
        .option("header",True)\
        .option("inferSchema",True)\
        .load('abfss://bronze@awstoragedatalaketanaji.dfs.core.windows.net/AdventureWorks_Returns')

# COMMAND ----------

df_Sales=spark.read.format('csv')\
        .option("header",True)\
        .option("inferSchema",True)\
        .load('abfss://bronze@awstoragedatalaketanaji.dfs.core.windows.net/AdventureWorks_Sales*')

# COMMAND ----------

df_ter=spark.read.format('csv')\
        .option("header",True)\
        .option("inferSchema",True)\
        .load('abfss://bronze@awstoragedatalaketanaji.dfs.core.windows.net/AdventureWorks_Territories')

# COMMAND ----------

df_subcat=spark.read.format('csv')\
        .option("header",True)\
        .option("inferSchema",True)\
        .load('abfss://bronze@awstoragedatalaketanaji.dfs.core.windows.net/AdventureWorks_Product_Subcategories')

# COMMAND ----------

# MAGIC %md
# MAGIC ### TRANSFORMATIONS

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Transform Calender data 
# MAGIC Adding two column "Year" and "month" and load into silver layer

# COMMAND ----------

df_cal.display()

# COMMAND ----------

# MAGIC %md
# MAGIC you need to install libraries for Types and functions ( we have uploaded on top this notebook)

# COMMAND ----------

df_cal=df_cal.withColumn('month',month('Date'))\
        .withColumn('year',year('Date'))
df_cal.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### data write modes 
# MAGIC Append()\
# MAGIC overwrite()\
# MAGIC error()\
# MAGIC ignore()

# COMMAND ----------

df_cal.write.format('parquet')\
        .mode('append')\
        .option("path","abfss://silver@awstoragedatalaketanaji.dfs.core.windows.net/AdventureWorks_Calendar")\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Transform Customer data
# MAGIC
# MAGIC Creating "FullName" column using contact() and concat_ws() functions

# COMMAND ----------

df_cus.display()

# COMMAND ----------

df_cus.withColumn("fullName",concat(col('Prefix'),lit(' '),col('FirstName'),lit(' '),col('LastName'))).display()

# COMMAND ----------

df_cus=df_cus.withColumn("FullName",concat_ws(' ',col('prefix'),col('Firstname'),col('LastName')))

# COMMAND ----------

df_cus.display()

# COMMAND ----------

df_cus.write.format('parquet')\
    .mode('append')\
    .option("path","abfss://silver@awstoragedatalaketanaji.dfs.core.windows.net/AdventureWorks_Customers")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Transform Product Sub Category data
# MAGIC
# MAGIC we can just read and load into silver layer there is no transformation

# COMMAND ----------

df_subcat.display()

# COMMAND ----------

df_subcat.write.format('parquet')\
    .mode('append')\
    .option("path","abfss://silver@awstoragedatalaketanaji.dfs.core.windows.net/AdventureWorks_SubCategories")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Transform Product data
# MAGIC
# MAGIC We can use SPLIT() function on ProductSKU and ProductName column using Index.\
# MAGIC We can Transform existing column instade of creating new column

# COMMAND ----------

df_pro.display()

# COMMAND ----------

df_pro = df_pro.withColumn('ProductSKU',split('ProductSKU','-')[0])\
        .withColumn('ProductName',split('ProductName',' ')[0])

# COMMAND ----------

df_pro.display()

# COMMAND ----------

df_pro.write.format('parquet')\
    .mode('append')\
    .option("path","abfss://silver@awstoragedatalaketanaji.dfs.core.windows.net/AdventureWorks_Products")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Transform Returns data
# MAGIC
# MAGIC This is small dataset, We can read , display and Write into silver layer

# COMMAND ----------

df_ret.display()

# COMMAND ----------

df_ret.write.format('parquet')\
    .mode('append')\
    .option("path","abfss://silver@awstoragedatalaketanaji.dfs.core.windows.net/AdventureWorks_Returns")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Transform Territories data
# MAGIC

# COMMAND ----------

df_ter.display()

# COMMAND ----------

df_ter.write.format('parquet')\
    .mode('append')\
    .option("path","abfss://silver@awstoragedatalaketanaji.dfs.core.windows.net/AdventureWorks_Territories")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Transform Sales data
# MAGIC

# COMMAND ----------

df_Sales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Change Date Format

# COMMAND ----------

df_Sales =  df_Sales.withColumn('StockDate',to_timestamp('StockDate'))

# COMMAND ----------

# MAGIC %md
# MAGIC Replace Value

# COMMAND ----------

df_Sales = df_Sales.withColumn('OrderNumber', regexp_replace('OrderNumber', 'S', 'T'))

# COMMAND ----------

# MAGIC %md
# MAGIC Multiplication with 2 columns 

# COMMAND ----------

df_Sales = df_Sales.withColumn('multiply',col('OrderLineItem') * col('OrderQuantity'))

# COMMAND ----------

df_Sales.display()

# COMMAND ----------

df_Sales.write.format('parquet')\
    .mode('append')\
    .option("path","abfss://silver@awstoragedatalaketanaji.dfs.core.windows.net/AdventureWorks_Sales")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. How many orders received every Day

# COMMAND ----------

df_Sales.groupBy('OrderDate').agg(count('OrderNumber').alias('total_order')).display()

# COMMAND ----------

df_procat.display()

# COMMAND ----------

df_ter.display()