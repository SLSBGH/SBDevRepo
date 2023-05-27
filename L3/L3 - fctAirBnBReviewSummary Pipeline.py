# Databricks notebook source
# MAGIC %md
# MAGIC Creates AirBnB Review Summary (Layer 3).

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import *
import datetime
from delta.tables import *
import pandas as pd
import json
import requests
import os
import functools
from py4j.protocol import Py4JJavaError
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE fctdb.fctAirBnBReviewSummary
# MAGIC (ListingId INTEGER,
# MAGIC  ListingCount INTEGER,
# MAGIC  FirstCreatedDate TIMESTAMP,
# MAGIC  ProcessLoadName STRING);
# MAGIC
# MAGIC  INSERT INTO fctdb.fctAirBnBReviewSummary
# MAGIC  SELECT 
# MAGIC  ListingId, 
# MAGIC  count(ListingId),
# MAGIC  current_timestamp(),
# MAGIC  current_user()
# MAGIC  FROM tbldb.tblAirBnBReview
# MAGIC  GROUP BY ListingId;
