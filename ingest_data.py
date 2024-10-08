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
import time
path_root=''
USER_PREFIX=''

# Are we running in CML
if 'CDSW_PROJECT' not in os.environ:
  path_root='/app/mount'

else:
  path_root='/home/cdsw'


print(f"Getting config from {path_root}/parameters.conf")
import configparser

tablename_conf='train_tablename'

config = configparser.ConfigParser()
config.read(f"{path_root}/parameters.conf")
data_lake_name=config.get("general","data_lake_name")
s3BucketName=config.get("general","s3BucketName")
tablename=config.get("general",'train_tablename')

database =f"{USER_PREFIX}_{config.get('general','database')}"
srcdir   =s3BucketName

# see this article for more details and tips. Especially for Iceberg
# https://community.cloudera.com/t5/Community-Articles/Spark-in-CML-Recommendations-for-using-Spark-in-Cloudera/ta-p/372164
#

spark = (
  SparkSession.builder.appName(f"{USER_PREFIX}_CCLead-Data-Loader")
  .config("spark.sql.hive.hwc.execution.mode", "spark")
  .config("spark.sql.extensions", "com.qubole.spark.hiveacid.HiveAcidAutoConvertExtension, org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  .config("spark.sql.catalog.spark_catalog.type", "hive")
  .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
  .config("spark.yarn.access.hadoopFileSystems", data_lake_name)
  .config("spark.hadoop.iceberg.engine.hive.enabled", "true")
  .config("spark.jars", "/opt/spark/optional-lib/iceberg-spark-runtime.jar")
  .getOrCreate()
  )

# Because this gets run in a jupyter app, we can't use normal command-line args
if 'JOB_ARGUMENTS' in os.environ:
  file_list = os.environ.get('JOB_ARGUMENTS').split()
else:
  file_list = "train_1.csv train_2.csv train_3.csv train_4.csv".split()

for file in file_list:

  df = spark.read.options(header='True', inferSchema='True', delimiter=',') \
    .csv(f"{s3BucketName}/{file}")

  df.printSchema()

  df.writeTo(f"{database}.{tablename}")\
     .tableProperty("write.format.default", "parquet")\
     .using("iceberg")\
     .append()
## do the audit test
  audit_df = spark.sql(f" SELECT COUNT(*) AS Row_Count FROM {database}.{tablename} ")
  time.sleep(30)

spark.stop()
