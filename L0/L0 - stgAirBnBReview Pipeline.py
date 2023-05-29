# Databricks notebook source
# MAGIC %md
# MAGIC Load/Prepare/Publish AirBnB Review into staging database.

# COMMAND ----------

# MAGIC %md
# MAGIC Import Library

# COMMAND ----------

dbutils.notebook.run('/Users/slsb.brown1@outlook.com/GenericNotebook',60)

# COMMAND ----------

# MAGIC %md
# MAGIC Connect to Blob Storage

# COMMAND ----------

spark.conf.set("fs.azure.account.key.2022datasets.blob.core.windows.net",
              dbutils.secrets.get(scope = "14052022SB" , key = "ContainerFilesForLearning2022"))

# COMMAND ----------

# MAGIC %md
# MAGIC Read data into dataframe

# COMMAND ----------

RowsRead = 0

tempAirBnBreviewStg01 = spark.read.csv("wasb://csvfiles@2022datasets.blob.core.windows.net/AirBnBReviews.csv", header = "true", inferSchema = "true")

RowsRead = tempAirBnBreviewStg01.count()
print(RowsRead)

# COMMAND ----------

# MAGIC %md
# MAGIC Transform dataframe into delta table.

# COMMAND ----------

tempDTVAirBnBreviewStg01 = tempAirBnBreviewStg01.createOrReplaceTempView("tempDTVAirBnBreviewStg01")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE stgdb.stgAirBnBReview
# MAGIC (ListingId INTEGER,
# MAGIC  ReviewDate TIMESTAMP,
# MAGIC  ReviewerName STRING,
# MAGIC  ReviewCommentDesc STRING,
# MAGIC  SentimentTypeName STRING);
# MAGIC
# MAGIC INSERT INTO stgdb.stgAirBnBReview
# MAGIC SELECT 
# MAGIC LISTING_ID,
# MAGIC DATE,
# MAGIC REVIEWER_NAME,
# MAGIC COMMENTS,
# MAGIC SENTIMENT
# MAGIC FROM tempDTVAirBnBreviewStg01;
