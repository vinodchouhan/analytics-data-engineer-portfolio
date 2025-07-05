# Databricks notebook source
# MAGIC %md
# MAGIC Section A: Data Ingestion and Bronze Layer

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Create a table from CSV files:

# COMMAND ----------

# Create  a Dataframe for transactions_bronze
transactions_bronze_df = (spark.read
                          .option("header", "true")
                          .option("inferSchema", "true")
                          .csv("/FileStore/tables/66a2634026e86_transactions_dataset.csv"))

# COMMAND ----------

# Create  a Dataframe for customer_bronze
customer_bronze_df = (spark.read
                          .option("header", "true")
                          .option("inferSchema", "true")
                          .csv("/FileStore/tables/66a262dd3659f_customer_dataset.csv"))

# COMMAND ----------

# Create the bronze schema

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

# COMMAND ----------

# Save DataFrames as table in Bronze schema

transactions_bronze_df.write.format("delta").saveAsTable("bronze.transactions_bronze")
customer_bronze_df.write.format("delta").saveAsTable("bronze.customer_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC Preview and count records

# COMMAND ----------

# Display first 5 rows
display(spark.sql("SELECT * FROM bronze.transactions_bronze LIMIT 5"))
display(spark.sql("SELECT * FROM bronze.customer_bronze LIMIT 5"))

# COMMAND ----------

# Count records
display(spark.sql("SELECT COUNT(*) FROM bronze.transactions_bronze"))
display(spark.sql("SELECT COUNT(*) FROM bronze.customer_bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC Questions
# MAGIC
# MAGIC

# COMMAND ----------

# 1.1 Balance > 15000 and loan = 'yes'
spark.sql("""
          SELECT * 
          FROM bronze.customer_bronze
          WHERE balance > 15000 AND loan = 'yes'
          """).show()

# COMMAND ----------

#1.2 Transactions where type is 'purchase' and merchant_category = 'travel'
spark.sql("""
          SELECT * 
          FROM bronze.transactions_bronze
          WHERE transaction_type = 'purchase' AND merchant_category = 'travel'
          """).show()

# COMMAND ----------

#1.3 Job type of customer with max transaction amount
spark.sql("""
          SELECT job
          FROM bronze.customer_bronze c 
          JOIN bronze.transactions_bronze t 
          ON c.customer_id = t.customer_id
          ORDER BY t.transaction_amount DESC
          LIMIT 1
          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Sction B: Create Silver Table with Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Data Cleaning and Transformation

# COMMAND ----------

# Clean transactions_bronze
transactions_silver_df = (spark.sql("SELECT * FROM bronze.transactions_bronze")
                          .dropna(subset=["customer_id"])
                          .dropDuplicates())

# COMMAND ----------

# Create the silver schema

spark.sql("CREATE DATABASE IF NOT EXISTS silver")

# COMMAND ----------

# Save clean data as transactions_silver table
transactions_silver_df.write.format("delta").saveAsTable("silver.transactions_silver")


# COMMAND ----------

# Clean customer_bronze similarly
customer_silver_df = (spark.sql("SELECT * FROM bronze.customer_bronze")
                          .dropna(subset=["customer_id"])
                          .dropDuplicates())

# COMMAND ----------

# Save clean data as customer_silver table
customer_silver_df.write.format("delta").saveAsTable("silver.customer_silver")


# COMMAND ----------

# MAGIC %md
# MAGIC Optimizations

# COMMAND ----------

# Apply partitioning on transactions_silver
transactions_silver_df.write.partitionBy("transaction_date").format("delta")

# COMMAND ----------

# Apply bucketing on customer_silver by customer_id
customer_silver_df.write.bucketBy(8, "customer_id").format("delta")

# COMMAND ----------

# MAGIC %md
# MAGIC Questions

# COMMAND ----------

#2.1 Cumulative transaction amount by merchant_category
spark.sql("""
          SELECT 
            merchant_category,
            transaction_date,
            SUM(transaction_amount) OVER (PARTITION BY merchant_category ORDER BY transaction_date) as cumulative_transaction_amount
          FROM silver.transactions_silver
          """).show()

# COMMAND ----------

#2.2 Total number of transactions by status and category
spark.sql("""
          SELECT
            transaction_status,
            merchant_category,
            COUNT(*) as total_transactions
          FROM silver.transactions_silver
          GROUP BY transaction_status, merchant_category
          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC 2.3 Calculate Aggregates

# COMMAND ----------

#2.3.1 Top 10 customers by max transactions per week
spark.sql("""
          WITH weekly_transactions AS (
            SELECT 
              customer_id,
              WEEKOFYEAR(transaction_date) as week,
              COUNT(*) as transaction_count
            FROM silver.transactions_silver
            GROUP BY customer_id, WEEKOFYEAR(transaction_date)
          )
          SELECT customer_id, MAX(transaction_count) as max_transactions
          FROM weekly_transactions
          GROUP BY customer_id
          ORDER BY max_transactions DESC
          LIMIT 10
          """).show()

# COMMAND ----------

#2.3.2 Percentage contribution of each segment to total monthly transactions
spark.sql("""
          SELECT 
            customer_id,
            MONTH(transaction_date) as month,
            COUNT(*) as total_transactions,
            COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY MONTH(transaction_date)) as percentage_contribution
           FROM silver.transactions_silver
           GROUP BY customer_id, MONTH(transaction_date)
          """).show()

# COMMAND ----------

#2.3.3 Segment customers into high and low spenders based on monthly transactions
spark.sql("""
          with monthly_transactions AS (
              SELECT 
                customer_id,
                MONTH(transaction_date) as month,
                SUM(transaction_amount) as monthly_spending
              FROM silver.transactions_silver
              GROUP BY customer_id, MONTH(transaction_date)
          )
          SELECT
            customer_id,
            month,
            CASE
                WHEN monthly_spending > 1000 THEN 'High Spender'
                ELSE 'Low Spender'
            END as spender_segment
          FROM monthly_transactions
          """).show()

# COMMAND ----------

#2.3.4 Rank job categories based on total transaction_amount
spark.sql("""
          SELECT 
            job,
            SUM(transaction_amount) as total_amount,
            RANK() OVER (ORDER BY SUM(transaction_amount) DESC) as rank
          FROM silver.customer_silver c
          JOIN silver.transactions_silver t
          ON c.customer_id = t.customer_id
          GROUP BY job
          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Section C: Gold Tables and Final Aggregations

# COMMAND ----------

# MAGIC %md
# MAGIC Create Fact and Dimension Tables

# COMMAND ----------

# Create the gold schema

spark.sql("CREATE DATABASE IF NOT EXISTS gold")

# COMMAND ----------

# Create Fact table for transactions
fact_transactions = spark.sql("""
                              SELECT 
                                t.transaction_id,
                                t.customer_id,
                                t.transaction_amount,
                                t.transaction_date,
                                t.transaction_type,
                                t.merchant_category,
                                t.transaction_status,
                                c.age,
                                c.job,
                                c.marital_status,
                                c.education,
                                c.balance,
                                c.loan,
                                c.contact_type,
                                c.last_contact_month,
                                c.days_since_last_contact,
                                c.previous_campaign_outcome
                              FROM silver.transactions_silver t 
                              JOIN silver.customer_silver c 
                              ON t.customer_id = c.customer_id
                              """)


# COMMAND ----------

# Creating gold table
fact_transactions.write.format("delta").saveAsTable("gold.fact_transactions")

# COMMAND ----------

fact_transactions.write.partitionBy("transaction_date").format("delta")

# COMMAND ----------

# Create View
spark.sql("""
          CREATE OR REPLACE VIEW gold.customer_transactions_vw AS
          SELECT 
            c.customer_id,
            c.age,
            c.job,
            t.balance,
            t.transaction_id,
            t.transaction_amount,
            t.transaction_date,
            t.merchant_category,
            t.transaction_status
          FROM gold.fact_transactions t 
          JOIN silver.customer_silver c 
          ON t.customer_id = c.customer_id
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC Questions

# COMMAND ----------

#3.1 total balance and number of customers by job category
spark.sql("""
          SELECT 
            job,
            SUM(balance) as total_balance,
            COUNT(*) as customer_count
          FROM gold.customer_transactions_vw
          GROUP BY job
          """).show()

# COMMAND ----------

#3.2 Visual chart for total transactions by merchant category and job type
agg_df = spark.sql("""
          SELECT 
            job,
            merchant_category,
            SUM(transaction_amount) as total_transaction_amount
          FROM gold.customer_transactions_vw
          GROUP BY merchant_category, job
          """)

# COMMAND ----------

# Convert spark dataframe to Pandas dataframe
agg_pd_df = agg_df.toPandas()

# COMMAND ----------

# Ploting using seaborn
import matplotlib.pyplot as plt 
import seaborn as sns

plt.figure(figsize=(14, 10))
sns.barplot(x="total_transaction_amount", y="merchant_category", hue="job", data=agg_pd_df, palette="viridis")
plt.title("Total Transactions by merchant category and Job type")
plt.xlabel("Total Transaction Amount")
plt.ylabel("Merchant Category")
plt.legend(title="Job Type", bbox_to_anchor=(1.05, 1), loc="upper left")
plt.show()

# COMMAND ----------

#3.3.1 Average purchase amount per customer
spark.sql("""
          SELECT 
            customer_id,
            AVG(transaction_amount) as average_purchase_amount
          FROM gold.fact_transactions
          WHERE transaction_type = 'purchase'
          GROUP BY customer_id
          """).show()

# COMMAND ----------

#3.3.2 Customer with highest transaction amount (sum of purchase minus refund) weekly
spark.sql("""
          WITH weekly_spending AS (
              SELECT 
                customer_id,
                WEEKOFYEAR(transaction_date) as week,
                SUM(CASE WHEN transaction_type = 'purchase' THEN transaction_amount ELSE 0 END) - SUM(CASE WHEN transaction_type = 'refund' THEN transaction_amount ELSE 0 END) as net_spending
              FROM gold.fact_transactions
              GROUP BY customer_id, WEEKOFYEAR(transaction_date)
          )
          SELECT customer_id, week, MAX(net_spending) as max_spending
          FROM weekly_spending
          GROUP BY customer_id, week 
          ORDER BY max_spending DESC 
          LIMIT 1
          """).show()


# COMMAND ----------

#3.4 Reconcile the total number of records in the bronze, silver, gold tables

# For customer tables
spark.sql("SELECT 'bronze' as layer, COUNT(*) as record_count FROM bronze.customer_bronze").show()
spark.sql("SELECT 'silver' as layer, COUNT(*) as record_count FROM silver.customer_silver").show()

# COMMAND ----------

# For transaction tables
spark.sql("SELECT 'bronze' as layer, COUNT(*) as record_count FROM bronze.transactions_bronze").show()
spark.sql("SELECT 'silver' as layer, COUNT(*) as record_count FROM silver.transactions_silver").show()
spark.sql("SELECT 'gold' as layer, COUNT(*) as record_count FROM gold.fact_transactions").show()
