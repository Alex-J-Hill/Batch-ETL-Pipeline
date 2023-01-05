import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, col, sum, avg
from pyspark.sql.functions import when
from datetime import datetime
import argparse
import json


def spark_init(spark_name):
    spark = SparkSession.builder.appName(spark_name).getOrCreate()
    return spark


def read_csv(spark, input_bucket):
    df = (
        spark.read.option("delimiter", ",")
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_bucket)
    )
    return df


def write(df, output_bucket):
    df.write.option("header", "false").mode("overwrite").parquet(output_bucket)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--spark_name", help="spark_name")
    parser.add_argument('--input_paths', help="input from Airflow")
    parser.add_argument('--calpath', help="calendar file path")
    parser.add_argument('--invpath', help="inventory file path")
    parser.add_argument('--prodpath', help="product file path")
    parser.add_argument("--salespath", help="sales file path")
    parser.add_argument('--storepath', help="calendar file path")
    parser.add_argument("--output_bucket", help="output file S3 bucket location.")

    args = parser.parse_args()

    print("Parsing successful")
    

    spark_name = args.spark_name
    output_bucket = args.output_bucket
    calpath = args.calpath
    invpath = args.invpath
    prodpath = args.prodpath
    salespath = args.salespath
    storepath = args.storepath



    #print(calpath)

    spark = spark_init(spark_name)

    #Calendar - DIMENSION - Done
    dfcal=read_csv(spark, calpath)
    dfcal=dfcal.fillna(value=0)
    dfcal=dfcal.na.fill('None')
    dfcal=dfcal.withColumn('UPDATE_TIME',lit(datetime.now()))

    #Store - DIMENSION - Done
    dfstore=read_csv(spark, storepath)
    dfstore=dfstore.fillna(value=0)
    dfstore=dfstore.na.fill('None')
    dfstore=dfstore.withColumn('TLOG_ACTIVE_FLG',lit('TRUE'))
    dfstore=dfstore.withColumn('UPDATE_TIME',lit(datetime.now()))

    #Product - DIMENSION - Done
    dfprod=read_csv(spark, prodpath)
    dfprod=dfprod.fillna(value=0)
    dfprod=dfprod.na.fill('None')
    dfprod=dfprod.withColumn('TLOG_ACTIVE_FLG',lit('TRUE'))
    dfprod=dfprod.withColumn('UPDATE_TIME',lit(datetime.now()))

    # Sales
    dfsales = read_csv(spark, salespath)
    dfsales = dfsales.withColumnRenamed("trans_dt", "cal_dt")
    dfsales = dfsales.fillna(value=0)
    dfsales = dfsales.na.fill("None")



    #Sales by day
    dfsalesdy = dfsales.groupBy(
        "cal_dt",
        "store_key",
        "prod_key").agg(
            sum("sales_qty").alias("sales_qty"),
            sum("sales_amt").alias("sales_amt"),
            avg("sales_price").alias("sales_price"),
            sum("sales_cost").alias("sales_cost"),
            sum("sales_mgrn").alias("sales_mgrn"),
            avg("discount").alias("discount"),
            sum("ship_cost").alias("ship_cost")
            ).orderBy(
                "cal_dt",
                "store_key",
                "prod_key")

    #Inventory
    dfinv=read_csv(spark, invpath)
    dfinv=dfinv.fillna(value=0)
    dfinv=dfinv.na.fill('None')
    dfinv=dfinv.withColumn('UPDATE_TIME',lit(datetime.now()))
    dfinv.show()

    write(dfsalesdy, output_bucket)


## command to run the script in local:
##spark-submit --master local --deploy-mode client spark.py --spark_name 'airflow_lab' --input_bucket './data/orders_amount.csv' --output_bucket './data/orders_amount_output' --avg_order_amount '29171.860335'#
