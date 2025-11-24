#
#Copyright (c) 2020 Cloudera, Inc. All rights reserved.
#

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
import sys

### ユーザー名の設定
username = "adminxx" ## 講師の指示に従って更新してください
bucketname = "xxx" ## 講師の指示に従って更新してください

###  DB名・アプリ名をユーザー名に応じて設定
db_name = username + "_TexasPPP"
appName = username + "-Job2"

spark = SparkSession \
    .builder \
    .appName(appName) \
    .getOrCreate()

# インプットパスとして S3 のパスを設定
input_path ="s3a://" + bucketname + "/data/input/PPP-Sub-150k-TX.csv"

print("...............................")
print(f"S3バケットのデータを Spark に取り込みます。")
print(f"※テキサス州のローンの承認状況を示すデータ。こちらは15万ドル未満のデータです。")

# S3バケット から Spark にデータを取り込む
base_df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path, schema="LoanAmount DOUBLE, City STRING, State STRING, Zip STRING, BusinessType STRING, NonProfit STRING, JobsRetained INT, DateApproved STRING, Lender STRING")

# スキーマを表示する
print("...............................")
print(f"スキーマを表示します")
base_df.printSchema()

# 必要なカラムだけを取り出す
filtered_df = base_df.select("LoanAmount", "City", "State", "Zip", "BusinessType", "NonProfit", "JobsRetained", "DateApproved", "Lender")

# テキサスのデータのみを表示（参考用）
print("...............................")
print(f"レコード数は何件?")
tx_cnt = filtered_df.count()
print(f"%i 件でした！" % tx_cnt)

# データベースが存在していない場合は作成
print("...............................")
print(f"{db_name} という名前で、データベースを作成します。\n")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
print(f"名前に {username} がつくデータベースを表示します。\n")
spark.sql(f"SHOW databases like '{username}*' ").show()

print("...............................")
print(f"{db_name} データベースの loan_data テーブルにデータを挿入します。 \n")

# データをテーブルに追加
filtered_df.\
  write.\
  mode("overwrite").\
  saveAsTable(db_name+'.'+"loan_data", format="parquet")

# データ追加の結果（件数）を確認
print("...............................")
print(f"レコード件数 \n")
spark.sql(f"Select count(*) as RecordCount from {db_name}.loan_data").show()

print("...............................")
print(f"DBのデータを、確認のために15件だけ表示します。 \n")
spark.sql(f"Select * from {db_name}.loan_data limit 15").show()
