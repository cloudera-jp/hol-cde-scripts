#
#Copyright (c) 2020 Cloudera, Inc. All rights reserved.
#

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, regexp_replace
import sys

### ユーザー名の設定
username = "userxx" ## 講師の指示に従って更新してください
bucketname = "xxx" ## 講師の指示に従って更新してください

###  DB名・アプリ名をユーザー名に応じて設定
db_name = username + "_TexasPPP"
appName = username + "-Job3"

spark = SparkSession \
    .builder \
    .appName(appName) \
    .getOrCreate()

# インプットパスとして S3 のパスを設定
input_path ="s3a://" + bucketname + "/data/input/PPP-Over-150k-ALL.csv"

print("...............................")
print(f"S3バケットのデータを Spark に取り込みます。")
print(f"※アメリカのローンの承認状況を示すデータ。こちらは15万ドル以上のデータです。")

# S3バケット から Spark にデータを取り込む
base_df=spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

# スキーマを表示（確認用）
print("...............................")
print(f"スキーマを表示します")
base_df.printSchema()

# さまざまな州のローンのデータの中から、テキサス州のデータだけを抽出
print("...............................")
print(f"テキサス州のデータだけを抽出します。")
texas_df = base_df.filter(base_df.State == 'TX')

# 抽出したテキサスのレコードを表示（確認用）
print("...............................")
print(f"テキサス州のレコードは何件?")
tx_cnt = texas_df.count()
print(f"%i 件でした！" % tx_cnt)

print("...............................")
print(f"ここから、データの様々な前処理を行います。")

# カラム名をリネーム（LoanRange -> LoanAmount）して表示
filtered_df = texas_df.select(col("LoanRange").alias("LoanAmount"), "City", "State", "Zip", "BusinessType", "NonProfit", "JobsRetained", "DateApproved", "Lender")
filtered_df.show()

print("...............................")
print(f"ローンの金額を、文字列から数字に変換します。")

# 正規表現を利用し、文字列の情報を数値に変換し平均金額を算出
value_df=filtered_df.withColumn("LoanAmount", regexp_replace(col("LoanAmount"), "[a-z] \$5-10 million", "7500000").cast("double"))
value_df=value_df.withColumn('LoanAmount', regexp_replace(col("LoanAmount"), "[a-z] \$1-2 million", "1500000").cast("double"))
value_df=value_df.withColumn('LoanAmount', regexp_replace(col("LoanAmount"), "[a-z] \$5-10 million", "7500000").cast("double"))
value_df=value_df.withColumn('LoanAmount', regexp_replace(col("LoanAmount"), "[a-z] \$2-5 million", "3500000").cast("double"))
value_df=value_df.withColumn('LoanAmount', regexp_replace(col("LoanAmount"), "[a-z] \$350,000-1 million", "675000").cast("double"))

# 正しく値が算出できたことを確認
testdf = value_df.filter(value_df.LoanAmount != "7500000")
print("...............................")
print("データが正しく変換できたかを確認します。（正しく変換されていれば、空のテーブルが表示されます。）")
testdf.show()
# 最終的なテーブルを確認
print("...............................")
print("加工が完了したテーブルを見てみましょう（LoanAmount が文字→数字に変換されています）")
value_df.show()

# DBが存在しない場合は作成
print("...............................")
print(f"{db_name} という名前のデータベースが存在しなければ作成します。\n")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
print(f"{username} で始まるデータベースの一覧：\n")
spark.sql(f"SHOW databases like '{username}*' ").show()

print("...............................")
print(f"{db_name} データベースの loan_data テーブルに、データを挿入（追加）します。 \n")

# データをテーブルに挿入
value_df.\
  write.\
  mode("append").\
  saveAsTable(db_name+'.'+"loan_data", format="parquet")

# データ件数の表示（確認用）
print("...............................")
print(f"データ件数 \n")
spark.sql(f"Select count(*) as RecordCount from {db_name}.loan_data").show()

print("...............................")
print(f"DBのデータを、確認のために15件だけ表示します。 \n")
spark.sql(f"Select * from {db_name}.loan_data limit 15").show()
