# Indian Banking Transactions — PySpark ETL

A batch ETL pipeline built with **Python and PySpark** to clean, validate, transform, and summarize Indian banking transaction data.

The project takes raw transaction data in CSV format, applies a set of data quality and transformation rules, and stores the processed data and analytical summaries in **Parquet** format.

## Project Overview

The purpose of this project is to practice building a complete Spark-based ETL workflow using a realistic banking dataset.

The pipeline covers the following steps:

```text
Raw CSV
   ↓
Schema Validation
   ↓
Data Cleaning
   ↓
Deduplication
   ↓
Data Quality Validation
   ↓
Transformation
   ↓
Aggregations
   ↓
Parquet Output
```

Records that fail the validation rules are separated and stored as rejected records rather than being discarded.

---

## Dataset

This project uses the **Indian Banking Transactions 2019–2024** dataset available on Kaggle.

**Source:**
https://www.kaggle.com/datasets/belbino/indian-banking-transactions-20192024

The dataset contains approximately 550,000 banking transactions covering the period from 2019 to 2024.

Some of the fields available in the dataset include:

* Transaction ID
* Customer ID
* Transaction date
* Transaction time
* Account type
* Transaction type
* Transaction amount
* Transaction direction
* Account balance
* Merchant category
* State
* Credit score
* Loan information
* EMI amount
* Transaction status
* Transaction channel
* KYC status
* Fraud indicator
* Transaction hour

### Dataset Setup

The raw CSV file is not included in this repository.

Download the dataset from Kaggle and place the file at:

```text
data/
└── indian_banking_transactions.csv
```

The pipeline expects the filename:

```text
indian_banking_transactions.csv
```

Raw data and generated output files are excluded from Git version control.

---

## Technologies

* Python 3.11
* PySpark 4.2.0
* Apache Spark
* Spark SQL
* Parquet
* Git
* GitHub

---

## Project Structure

```text
Indian-Banking-Transactions-PySpark-ETL/
│
├── banking_etl.py
├── README.md
├── requirements.txt
├── .gitignore
│
├── data/
│   └── indian_banking_transactions.csv
│
└── output/
    ├── transactions/
    ├── customer_summary/
    ├── monthly_summary/
    ├── account_summary/
    ├── merchant_summary/
    └── rejected_records/
```

The `data/` and `output/` directories are kept out of the Git repository through `.gitignore`.

---

# ETL Pipeline

## 1. Spark Session

The pipeline starts a local Spark session using all available CPU cores.

```python
spark = (
    SparkSession.builder
    .appName("IndianBankingTransactionsETL")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
```

For local development, the number of shuffle partitions is set to 8 instead of relying on Spark's default configuration.

---

## 2. Schema Definition

The CSV is loaded using an explicitly defined Spark schema rather than relying on automatic schema inference.

This ensures that important fields are read using the expected data types.

For example:

```text
transaction_id       String
customer_id          String
transaction_amount   Double
account_balance      Double
credit_score        Integer
has_loan            Integer
emi_amount           Double
is_fraud             Integer
transaction_hour     Integer
```

Using an explicit schema also makes the input structure easier to control and maintain.

---

## 3. Data Cleaning

The pipeline performs basic cleaning before validation.

### String cleaning

Whitespace is removed from important string columns using `trim()`.

Columns cleaned include:

* `transaction_id`
* `customer_id`
* `account_type`
* `transaction_direction`
* `state`
* `loan_type`
* `transaction_status`
* `kyc_status`
* `channel`

### Date conversion

The transaction date is converted from a string into a Spark `DateType` using `to_date()`.

### Transaction amount handling

Negative transaction amounts are converted to null before the validation step.

---

## 4. Deduplication

Duplicate records are removed using the transaction ID:

```python
df_clean = df_clean.dropDuplicates(["transaction_id"])
```

The pipeline also records the number of rows before and after deduplication.

This gives a simple measure of how many duplicate records were removed during processing.

Example output:

```text
========== DEDUPLICATION ==========

Rows before deduplication: ...
Rows after deduplication: ...
Duplicates removed: ...
```

---

## 5. Data Quality Validation

After cleaning and deduplication, the data is divided into two datasets:

```text
                 Cleaned Data
                      │
              ┌───────┴───────┐
              │               │
            Valid           Rejected
              │               │
              ▼               ▼
       Transformations   rejected_records
```

A record is considered valid when:

* `transaction_id` is not null
* `customer_id` is not null
* `transaction_date` is not null
* `transaction_amount` is greater than 0
* `account_balance` is greater than 0

The validation condition is implemented using Spark column expressions.

Records that fail these checks are written separately to the `rejected_records` output.

This keeps invalid records available for further investigation instead of silently removing them.

---

# Data Transformation

The valid transaction data is enriched with additional fields derived from the transaction date and amount.

### Date fields

The following columns are created:

* `year`
* `month`
* `day`
* `month_name`

These fields make it easier to perform time-based analysis.

### Transaction amount category

Transactions are also grouped into three categories:

| Transaction Amount | Category |
| ------------------ | -------- |
| Less than ₹1,000   | LOW      |
| ₹1,000 – ₹9,999    | MEDIUM   |
| ₹10,000 or more    | HIGH     |

The category is created using Spark's `when()` and `otherwise()` functions.

---

# Aggregations

The pipeline produces four main analytical summaries.

## Customer Summary

Transactions are grouped by `customer_id`.

The following metrics are calculated:

* Total transactions
* Total debit amount
* Total credit amount
* Average transaction amount
* Maximum transaction amount
* Minimum transaction amount

This provides a customer-level view of transaction activity.

---

## Monthly Summary

Transactions are grouped by:

```text
year
month
```

The summary contains:

* Transaction count
* Total transaction amount
* Average transaction amount
* Maximum transaction amount

The results are ordered by year and month.

---

## Account Summary

Transactions are grouped by:

```text
account_type
transaction_type
```

The summary contains:

* Transaction count
* Total transaction amount
* Average transaction amount

This can be used to compare transaction activity across different account and transaction types.

---

## Merchant Summary

Transactions are grouped by `merchant_category`.

The summary contains:

* Transaction count
* Total transaction amount
* Average transaction amount

The results are ordered by total transaction amount in descending order.

---

# Parquet Storage

The transformed transaction data is stored in **Parquet** format.

Parquet was chosen because it is a columnar format that works well with Spark and analytical workloads.

The main transaction dataset is partitioned by:

```text
year
month
```

The resulting structure looks like:

```text
output/
└── transactions/
    ├── year=2019/
    │   ├── month=1/
    │   ├── month=2/
    │   └── ...
    │
    ├── year=2020/
    ├── year=2021/
    ├── year=2022/
    ├── year=2023/
    └── year=2024/
```

Partitioning the data by year and month allows Spark to avoid reading unrelated partitions when queries filter on these fields.

---

# Output Datasets

The pipeline creates the following Parquet datasets:

| Output             | Description                                 |
| ------------------ | ------------------------------------------- |
| `transactions`     | Cleaned and transformed transaction records |
| `customer_summary` | Customer-level transaction metrics          |
| `monthly_summary`  | Monthly transaction metrics                 |
| `account_summary`  | Account and transaction-type metrics        |
| `merchant_summary` | Merchant category metrics                   |
| `rejected_records` | Records that failed validation              |

All outputs are written using Spark's Parquet writer with overwrite mode.

---

# Data Quality Checks

The pipeline performs several checks during processing:

* Row count before processing
* Schema inspection
* Null checks
* Duplicate detection
* Duplicate removal
* Transaction amount validation
* Account balance validation
* Valid record count
* Rejected record count
* Output generation

These checks make it easier to identify problems in the input data and verify that the ETL process completed successfully.

---

# Running the Project

## Prerequisites

Install:

* Python 3.11
* Java compatible with your Spark installation
* Apache Spark / PySpark
* Git

For Windows development, the project also requires the Hadoop native Windows binaries used by Spark for certain filesystem operations.

---

## 1. Clone the Repository

```bash
git clone https://github.com/Sarshad070121/Indian-Banking-Transactions-PySpark-ETL.git
```

Move into the project directory:

```bash
cd Indian-Banking-Transactions-PySpark-ETL
```

---

## 2. Create the Python Environment

Using Conda:

```bash
conda create -n dataeng python=3.11
```

Activate the environment:

```bash
conda activate dataeng
```

---

## 3. Install Dependencies

```bash
pip install -r requirements.txt
```

The current `requirements.txt` contains:

```text
pyspark==4.2.0
```

---

## 4. Download the Dataset

Download the dataset from Kaggle:

https://www.kaggle.com/datasets/belbino/indian-banking-transactions-20192024

Place the CSV file here:

```text
data/indian_banking_transactions.csv
```

---

## 5. Run the Pipeline

From the project directory:

```bash
python banking_etl.py
```

The pipeline will:

1. Start Spark
2. Read the CSV
3. Apply the predefined schema
4. Clean the data
5. Remove duplicates
6. Validate records
7. Separate rejected records
8. Create derived columns
9. Generate summary datasets
10. Write Parquet outputs
11. Display summary results
12. Stop Spark

---

# Example Output

During execution, the pipeline prints information such as:

```text
Spark Session Created

Number of rows: ...

Schema:
root
 |-- transaction_id: string
 |-- customer_id: string
 |-- transaction_date: date
 ...

========== DEDUPLICATION ==========

Rows before deduplication: ...
Rows after deduplication: ...
Duplicates removed: ...

Valid records: ...
Rejected records: ...

ETL Ran Successfully!!!
```

The exact numbers depend on the version of the dataset used.

---

# What I Learned From This Project

This project helped me work through the complete flow of a Spark batch pipeline rather than focusing only on individual PySpark commands.

The main areas covered were:

* Loading structured data with a defined schema
* Working with Spark DataFrames
* Cleaning and validating input data
* Handling duplicate records
* Separating invalid records
* Creating derived columns
* Performing grouped aggregations
* Writing analytical datasets to Parquet
* Partitioning data for downstream queries
* Managing a Spark project with Git

It also provided practical experience with running Spark locally on Windows and dealing with the supporting Hadoop filesystem configuration required by the environment.

---

# Possible Improvements

There are several areas that can be added in future versions of the project:

* Incremental data processing
* Incremental loads based on transaction dates
* Slowly Changing Dimensions
* Window functions
* Broadcast joins
* More detailed data quality checks
* Spark performance tuning
* Airflow orchestration
* Docker support
* CI/CD using GitHub Actions
* Databricks and Delta Lake
* Cloud deployment
* Structured Streaming

These are intentionally not part of the current version. The current project focuses on building and understanding the core batch ETL workflow first.

---

# Repository

**Dataset:**
https://www.kaggle.com/datasets/belbino/indian-banking-transactions-20192024

---
## Author

**Jishan Attar**
