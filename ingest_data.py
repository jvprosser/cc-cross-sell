#****************************************************************************
# (C) Cloudera, Inc. 2020-2022
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# #  Author(s): Joe Prosser
#***************************************************************************/

# # Spark-SQL from PySpark
#
# This example shows how to send SQL queries to Spark.

from __future__ import print_function
import os
import sys
from pyspark.sql import SparkSession

# assume we're running in CML
path_root=''

# Are we not running in CML?
if 'CDSW_PROJECT' not in os.environ:
  path_root='/app/mount'

else:
  path_root='/home/cdsw'


import configparser
config = configparser.ConfigParser()
config.read(f"{path_root}/parameters.conf")
data_lake_name=config.get("general","data_lake_name")
s3BucketName=config.get("general","s3BucketName")
tablename=config.get("general","tablename")
database=config.get("general","database")
srcdir=s3BucketName




spark = SparkSession\
    .builder\
    .appName(f"CCLlead-Data-Validation ")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")\
    .config("spark.sql.catalog.spark_catalog.type", "hive")\
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
    .config("spark.yarn.access.hadoopFileSystems", data_lake_name)\
    .getOrCreate()


df = spark.read.options(header='True', inferSchema='True', delimiter=',') \
  .csv(f"{s3BucketName}/train_01.csv")

df.printSchema()

df.writeTo(f"{database}.{tablename}")\
     .tableProperty("write.format.default", "parquet")\
     .using("iceberg")\
     .append()
## do the audit test
audit_df = spark.sql(f" SELECT COUNT(*) AS null_count FROM {database}.{tablename} WHERE id IS NULL")


spark.stop()