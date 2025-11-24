#
#Copyright (c) 2020 Cloudera, Inc. All rights reserved.
#

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, regexp_extract, regexp_replace, col
import sys

### ユーザー名を設定
username = "userxx" ## 講師の指示に従って更新してください
bucketname = "xxx"  ## 講師の指示に従って更新してください

### DB名・アプリ名をユーザー名に応じて設定
db_name = username + "_retail"
appName = username + "-Job1"

spark = SparkSession \
    .builder \
    .appName(appName) \
    .getOrCreate()

# インプットパスとして S3 のパスを設定
input_path ="s3a://" + bucketname + "/data/input/access-log.txt"

print("...............................")
print("アクセスログのデータを、S3から Spark に取り込みます。")

# データをまず全量取り込む
base_df=spark.read.text(input_path)

# 必要な部分をスクレイピング
split_df = base_df.select(regexp_extract('value', r'([^ ]*)', 1).alias('ip'),
                          regexp_extract('value', r'(\d\d\/\w{3}\/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})', 1).alias('date'),
                          regexp_extract('value', r'^(?:[^ ]*\ ){6}([^ ]*)', 1).alias('url'),
                          regexp_extract('value', r'(?<=product\/).*?(?=\s|\/)', 0).alias('productstring')
                         )

# product部分の文字列が空行のレコードを除去
filtered_products_df = split_df.filter("productstring != ''")

# カラムの順番を調整
cleansed_products_df=filtered_products_df.select(regexp_replace("productstring", "%20", " ").alias('product'), "ip", "date", "url")

# 試しに1レコードを出力してみる
print("...............................")
print("DBに挿入する前のレコードを1件、試しに表示してみます。")
print(cleansed_products_df.take(1))

# DB存在しない時にDBを作成する
print("...............................")
print(f"{db_name} という名前で、DBを新規作成します。 \n")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
print("...............................")
print(f"{db_name} というDBの tokenized_accesss_logs テーブルにデータを挿入します。 \n")

# テーブルに書き込む
cleansed_products_df.\
  write.\
  mode("overwrite").\
  saveAsTable(db_name+'.'+"tokenized_access_logs", format="parquet")

print(f"レコードの数を確認 \n")
spark.sql(f"Select count(*) as RecordCount from {db_name}.tokenized_access_logs").show()

print(f"DBに挿入したデータを、確認のために15件だけ表示します。 \n")
spark.sql(f"Select * from {db_name}.tokenized_access_logs limit 15").show()
