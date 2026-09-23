import os

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pathlib import Path

#Spark session

spark = (
    SparkSession.builder
    .appName('IndianBankingTransactionsETL')
    .master('local[*]')
    .config('spark.sql.shuffle.partitions', '8')
    .getOrCreate()
)

spark.sparkContext.setLogLevel('WARN')
print("Spark Session Created")

#Reading data
BASE_DIR = Path(__file__).resolve().parent
INPUT_PATH = str(BASE_DIR / "data" / "indian_banking_transactions.csv")

OUTPUT_PATH = 'output'


schema = StructType([
    StructField('transaction_id', StringType(), False),
    StructField('customer_id', StringType(), True),
    StructField('transaction_date', StringType(), True),
    StructField('transaction_time', StringType(), True),
    StructField('account_type', StringType(), True),
    StructField('transaction_type', StringType(), True),
    StructField('transaction_amount', DoubleType(), True),
    StructField("transaction_direction", StringType(), True),
    StructField("account_balance", DoubleType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("state", StringType(), True),
    StructField("credit_score", IntegerType(), True),
    StructField("has_loan", IntegerType(), True),
    StructField("loan_type", StringType(), True),
    StructField("emi_amount", DoubleType(), True),
    StructField("transaction_status", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("kyc_status", StringType(), True),
    StructField("is_fraud", IntegerType(), True),
    StructField("transaction_hour", IntegerType(), True)
])

df = (
    spark.read
    .option('header',True)
    .schema(schema)
    .csv(INPUT_PATH)
    )

print('Number of rows: ', df.count())
print('\nSchema:')
df.printSchema()

print('\nSample: ')
df.show(20)


#Data Cleaning
df_clean = (
    df
        .withColumn('transaction_id', trim(col('transaction_id')))
        .withColumn('customer_id', trim(col('customer_id')))
        .withColumn('account_type', trim(col('account_type')))
        .withColumn('transaction_direction', trim(col('transaction_direction')))
        .withColumn('state', trim(col('state')))
        .withColumn('loan_type', trim(col('loan_type')))
        .withColumn('transaction_status', trim(col('transaction_status')))
        .withColumn('kyc_status', trim(col('kyc_status')))
        .withColumn('channel', trim(col('channel')))

        .withColumn('transaction_date', to_date(col('transaction_date'), 'yyyy-MM-dd'))
        .withColumn(
                "transaction_amount",
                when(
                    col("transaction_amount") >= 0,
                    col("transaction_amount")
                ).otherwise(None)
            ))


#Remove Duplicates
before_dedup = df_clean.count()

df_clean = df_clean.dropDuplicates(["transaction_id"])

after_dedup = df_clean.count()

print("\n========== DEDUPLICATION ==========")

print("Rows before deduplication:", before_dedup)
print("Rows after deduplication:", after_dedup)
print("Duplicates removed:", before_dedup - after_dedup)



# Data validation

valid_condition = (
    col("transaction_id").isNotNull()
    & col("customer_id").isNotNull()
    & col("transaction_date").isNotNull()
    & (col("transaction_amount") > 0)
    & (col("account_balance") > 0)
)

valid_df = df_clean.filter(valid_condition)
rejected_df = df_clean.filter(~valid_condition)

print("Valid records:", valid_df.count())
print("Rejected records:", rejected_df.count())

valid_df.show(20)

#Adding derived columns

df_transformed = (
    valid_df
        .withColumn("year", year(col('transaction_date')))
        .withColumn('month', month(col('transaction_date')))
        .withColumn('day', day(col('transaction_date')))
        .withColumn(
                "month_name",
                date_format(col("transaction_date"), "MMMM")
            )
        .withColumn('amount_category',when(col("transaction_amount") < 1000, "LOW")
                .when(col("transaction_amount") < 10000, "MEDIUM")
                .otherwise("HIGH"))

)


print('Number of rows: ', df_transformed.count())
print('\nSchema:')
df_transformed.printSchema()

df_transformed.show(20)

# Summary by Customer
customer_summary = (
    df_transformed
        .groupBy('customer_id')
        .agg(
            count('*').alias('total_transactions'),
            sum(
                        when(
                            col("transaction_direction") == "DEBIT",
                            col("transaction_amount")
                        ).otherwise(0)
                    ).alias("total_debit_amount"),
            
                    sum(
                        when(
                            col("transaction_direction") == "CREDIT",
                            col("transaction_amount")
                        ).otherwise(0)
                    ).alias("total_credit_amount"),
            round(
                        avg("transaction_amount"),
                        2
                    ).alias("average_transaction_amount"),
            
                    round(
                        max("transaction_amount"),
                        2
                    ).alias("maximum_transaction_amount"),
            
                    round(
                        min("transaction_amount"),
                        2
                    ).alias("minimum_transaction_amount")
        )
)

#Monthly Summary

monthly_summary = (
    df_transformed

        .groupBy("year", "month")
        .agg(

            count("*").alias("transaction_count"),

            round(
                sum("transaction_amount"),
                2
            ).alias("total_transaction_amount"),

            round(
                avg("transaction_amount"),
                2
            ).alias("average_transaction_amount"),

            round(
                max("transaction_amount"),
                2
            ).alias("maximum_transaction_amount")

        )

        .orderBy("year","month")
)


#Account Summary

account_summary = (
    df_transformed
        .groupBy('account_type','transaction_type')

        .agg(
            count('*').alias('transaction_count'),

        round(sum('transaction_amount'), 2).alias('total_amount'),
        round(avg('transaction_amount'), 2).alias('average_amount')        
        )
        .orderBy('account_type', 'transaction_type')
)


#Merchant Summary
merchant_summary = (
    df_transformed

    .groupBy("merchant_category")

    .agg(

        count("*").alias("transaction_count"),

        round(
            sum("transaction_amount"),
            2
        ).alias("total_amount"),

        round(
            avg("transaction_amount"),
            2
        ).alias("average_amount")

    )

    .orderBy(
        col("total_amount").desc()
    )
)


#Output

(
    df_transformed
        .write
        .mode('overwrite')
        .partitionBy('year','month')
        .parquet(
            f"{OUTPUT_PATH}/transactions"
        )

)


(
    customer_summary
    .write
    .mode("overwrite")
    .parquet(
        f"{OUTPUT_PATH}/customer_summary"
    )
)


(
    monthly_summary
    .write
    .mode("overwrite")
    .parquet(
        f"{OUTPUT_PATH}/monthly_summary"
    )
)


(
    account_summary
    .write
    .mode("overwrite")
    .parquet(
        f"{OUTPUT_PATH}/account_summary"
    )
)


(
    merchant_summary
    .write
    .mode("overwrite")
    .parquet(
        f"{OUTPUT_PATH}/merchant_summary"
    )
)


(
    rejected_df
    .write
    .mode("overwrite")
    .parquet(
        f"{OUTPUT_PATH}/rejected_records"
    )
)




print("\nCutomer Summary")

customer_summary.show(10, truncate=False)


print("\nMonthyl Summary")

monthly_summary.show(20, truncate=False)


print("\nAccount Summary")

account_summary.show(20, truncate=False)


print("\nMerchant Summary")

merchant_summary.show(20, truncate=False)



spark.stop()

print("\nETL Ran Successfully!!!")