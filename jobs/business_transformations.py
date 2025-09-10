import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'BUCKET',
    'CLEAN_DATABASE',
    'INCONSISTENCIES_DATABASE',
    'COUNT'
])
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket = args['BUCKET']
clean_db = args['CLEAN_DATABASE']
inconsistencies_db = args['INCONSISTENCIES_DATABASE']
count = args['COUNT']

orders_dyf = glueContext.create_dynamic_frame.from_catalog(database=clean_db, table_name='orders')
order_items_dyf = glueContext.create_dynamic_frame.from_catalog(database=clean_db, table_name='order_items')
customers_dyf = glueContext.create_dynamic_frame.from_catalog(database=clean_db, table_name='customers')
products_dyf = glueContext.create_dynamic_frame.from_catalog(database=clean_db, table_name='products')
payments_dyf = glueContext.create_dynamic_frame.from_catalog(database=clean_db, table_name='payments')

orders_df = orders_dyf.toDF()
order_items_df = order_items_dyf.toDF()
customers_df = customers_dyf.toDF()
products_df = products_dyf.toDF()
payments_df = payments_dyf.toDF()

orders_df = orders_df.limit(count)
order_items_df = order_items_df.limit(count)
customers_df = customers_df.limit(count)
products_df = products_df.limit(count)
payments_df = payments_df.limit(count)

orders_df.createOrReplaceTempView("orders")
order_items_df.createOrReplaceTempView("order_items")
customers_df.createOrReplaceTempView("customers")
products_df.createOrReplaceTempView("products")
payments_df.createOrReplaceTempView("payments")

# ORPHANED RECORD DETECTION

# orders with no order items
orphaned_orders_oi_df = spark.sql("""
    SELECT *
    FROM orders o
    LEFT JOIN order_items oi ON o.order_id = oi.order_id
    WHERE oi.order_item_id IS NULL
""")

# order items with non existent orders
orphaned_oi_orders_df = spark.sql("""
    SELECT *
    FROM order_items oi
    LEFT JOIN orders o ON oi.order_id = o.order_id
    WHERE o.order_id IS NULL
""")

# order items with non existent products
orphaned_oi_products_df = spark.sql("""
    SELECT *
    FROM order_items oi
    LEFT JOIN products pr ON oi.product_id = pr.product_id
    WHERE pr.product_id IS NULL
""")

# payments with non existent orders
orphaned_payments_orders_df = spark.sql("""
    SELECT *
    FROM payments p
    LEFT JOIN orders o ON p.order_id = o.order_id
    WHERE o.order_id IS NULL
""")

# FINANCIAL RECONCILIATION

total_order_items_amount = spark.sql("""
    SELECT order_id, SUM(total_price) as calculated_total_oi
    FROM order_items
    GROUP BY order_id
""")

total_payments_amount = spark.sql("""
    SELECT order_id, SUM(payment_value) as calculated_total_payments
    FROM payments
    GROUP BY order_id
""")

total_order_items_amount.createOrReplaceTempView("total_oi")
total_payments_amount.createOrReplaceTempView("total_p")

inconsistent_payments_df = spark.sql("""
    SELECT toi.order_id, toi.calculated_total_oi, tp.calculated_total_payments, abs(toi.calculated_total_oi - tp.calculated_total_payments) as difference
    FROM total_oi toi
    LEFT JOIN total_p tp ON toi.order_id = tp.order_id
    WHERE abs(toi.calculated_total_oi - tp.calculated_total_payments) > 0.01
""")

# BUSINESS RULE VALIDATION

# orders with status 'delivered' but no payments
delivered_no_payments_df = spark.sql("""
    SELECT o.*
    FROM orders o
    JOIN total_p tp ON o.order_id = tp.order_id
    WHERE o.order_status = 'delivered' AND tp.calculated_total_payments IS NULL
""")

# orders with payments but status 'cancelled'
cancelled_with_payments_df = spark.sql("""
    SELECT o.*
    FROM orders o
    JOIN total_p tp ON o.order_id = tp.order_id
    WHERE o.order_status = 'cancelled' AND tp.calculated_total_payments IS NOT NULL
""")

# ORDER FACTS TABLE
order_facts_table_df = spark.sql("""
    SELECT  o.order_id, 
            c.customer_id, 
            c.customer_unique_id, 
            c.customer_city, 
            c.customer_state,
            o.order_status, 
            o.order_purchase_timestamp, 
            o.order_delivered_customer_date,
            toi.calculated_total_oi as order_calculated_total, 
            tp.calculated_total_payments as order_paid_total, 
            CASE
                WHEN tp.calculated_total_payments = 0 OR tp.calculated_total_payments IS NULL THEN 'unpaid'
                WHEN abs(toi.calculated_total_oi - tp.calculated_total_payments) <= 0.01 THEN 'fully_paid'
                WHEN tp.calculated_total_payments < toi.calculated_total_oi - 0.01 THEN 'underpaid'
                WHEN tp.calculated_total_payments > toi.calculated_total_oi + 0.01 THEN 'overpaid'
            END AS payment_status,
            COUNT(oi.order_item_id) as total_items_count,
            COUNT(DISTINCT oi.product_id) as unique_products_count,
            CASE
                WHEN o.order_delivered_customer_date IS NULL THEN NULL
                ELSE datediff(o.order_delivered_customer_date, o.order_purchase_timestamp)
            END AS days_to_delivery,
            year(current_date()) as year,
            month(current_date()) as month
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN total_oi toi ON o.order_id = toi.order_id
    JOIN total_p tp ON o.order_id = tp.order_id
    GROUP BY o.order_id,
             c.customer_id, 
             c.customer_unique_id, 
             c.customer_city, 
             c.customer_state,
             o.order_status, 
             o.order_purchase_timestamp, 
             o.order_delivered_customer_date,
             toi.calculated_total_oi, 
             tp.calculated_total_payments
""")

order_facts_table_df.createOrReplaceTempView("order_facts_table")

# ORDER LINE ITEM FACTS TABLE
order_line_item_facts_table_df = spark.sql("""
    SELECT  o.order_id, 
            c.customer_id, 
            c.customer_unique_id, 
            c.customer_city, 
            c.customer_state,
            o.order_status, 
            o.order_purchase_timestamp, 
            o.order_delivered_customer_date,
            pr.product_id,
            pr.product_category_name,
            pr.product_category_name_english,
            toi.calculated_total_oi as order_calculated_total, 
            tp.calculated_total_payments as order_paid_total,
            year(current_date()) as year,
            month(current_date()) as month
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products pr ON pr.product_id = oi.product_id
    JOIN total_oi toi ON o.order_id = toi.order_id
    JOIN total_p tp ON o.order_id = tp.order_id
    GROUP BY pr.product_id,
             o.order_id,
             c.customer_id, 
             c.customer_unique_id, 
             c.customer_city, 
             c.customer_state,
             o.order_status, 
             o.order_purchase_timestamp, 
             o.order_delivered_customer_date,
             pr.product_category_name,
             pr.product_category_name_english,
             toi.calculated_total_oi, 
             tp.calculated_total_payments
""")

order_line_item_facts_table_df.createOrReplaceTempView("order_line_item_facts_table")

# CUSTOMER SUMMARY TABLE
customer_summary_table_df = spark.sql("""
    SELECT  customer_id, 
            customer_unique_id, 
            customer_city, 
            customer_state,
            MIN(order_purchase_timestamp) as first_order_date,
            MAX(order_purchase_timestamp) as last_order_date,
            COUNT(order_id) as total_orders,
            SUM(order_paid_total) as total_lifetime_value,
            SUM(order_paid_total) / COUNT(order_id) as avg_order_value,
            datediff(MAX(order_purchase_timestamp), MIN(order_purchase_timestamp)) as days_as_customer,
            CASE
                WHEN COUNT(order_id) = 1 THEN NULL
                ELSE datediff(MAX(order_purchase_timestamp), MIN(order_purchase_timestamp)) / (COUNT(order_id) - 1)
            END AS avg_days_between_orders,
            CASE
                WHEN COUNT(order_id) = 1 THEN 'new'
                WHEN datediff(CURRENT_DATE(), MAX(order_purchase_timestamp)) > 90 THEN 'inactive'
                ELSE 'regular'
            END AS customer_status
    FROM order_facts_table
    GROUP BY customer_id,
             customer_unique_id, 
             customer_city, 
             customer_state
""")

# PRODUCT PERFORMANCE TABLE
product_performance_table_df = spark.sql("""
    SELECT  product_id, 
            product_category_name_english,
            COUNT(DISTINCT order_id) as number_of_orders,
            COUNT(DISTINCT customer_id) as number_of_customers,
            MIN(order_purchase_timestamp) as first_sale_date,
            MAX(order_purchase_timestamp) as last_sale_date,
            order_calculated_total as total_revenue_expected,
            order_paid_total as total_actual_revenue,
            order_calculated_total / COUNT(DISTINCT order_id) as expected_revenue_per_product,
            order_paid_total / COUNT(DISTINCT order_id) as actual_revenue_per_product
    FROM order_line_item_facts_table
    GROUP BY product_id,
             product_category_name_english,
             order_calculated_total,
             order_paid_total
""")

total_orders_processed = orders_df.count()
total_order_items_processed = order_items_df.count()
total_payments_processed = payments_df.count()
orphaned_orders_count = orphaned_orders_oi_df.count()
orphaned_order_items_count = orphaned_oi_orders_df.count()
invalid_product_references_count = orphaned_oi_products_df.count()
orphaned_payments_count = orphaned_payments_orders_df.count()
payment_mismatch_count = inconsistent_payments_df.count()
unpaid_orders_count = order_facts_table_df.filter(order_facts_table_df['payment_status'] == 'unpaid').count()
overpaid_orders_count = order_facts_table_df.filter(order_facts_table_df['payment_status'] == 'overpaid').count()
underpaid_orders_count = order_facts_table_df.filter(order_facts_table_df['payment_status'] == 'underpaid').count()
delivered_but_unpaid_count = delivered_no_payments_df.count()
cancelled_but_paid_count = cancelled_with_payments_df.count()

data = [Row(
    total_orders_processed = total_orders_processed,
    total_order_items_processed = total_order_items_processed,
    total_payments_processed = total_payments_processed,
    orphaned_orders_count = orphaned_orders_count,
    orphaned_order_items_count = orphaned_order_items_count,
    invalid_product_references_count = invalid_product_references_count,
    orphaned_payments_count = orphaned_payments_count,
    payment_mismatch_count = payment_mismatch_count,
    unpaid_orders_count = unpaid_orders_count,
    overpaid_orders_count = overpaid_orders_count,
    underpaid_orders_count = underpaid_orders_count,
    delivered_but_unpaid_count = delivered_but_unpaid_count,
    cancelled_but_paid_count = cancelled_but_paid_count
)]

data_quality_summary_table_df = spark.createDataFrame(data).withColumn("job_run_timestamp", current_timestamp())

rows = [
    Row(
        issue_type = "orphaned_orders",
        issue_description = "Orders exist without corresponding order items",
        count=orphaned_orders_count
    ),
    Row(
        issue_type = "orphaned_payments",
        issue_description = "Payments reference orders that don't exist",
        count = orphaned_payments_count
    ),
    Row(
        issue_type = "orphaned_order_items",
        issue_description = "Order items reference orders that don't exist",
        count = orphaned_order_items_count
    ),
    Row(
        issue_type = "invalid_product_references",
        issue_description = "Order items reference products that don't exist",
        count = invalid_product_references_count
    ),
    Row(
        issue_type = "payment_mismatches",
        issue_description = "Calculated order total differs from paid amount",
        count = payment_mismatch_count
    ),
    Row(
        issue_type = "delivered_but_unpaid",
        issue_description = "Delivered orders with no payments",
        count = delivered_but_unpaid_count
    ),
    Row(
        issue_type = "cancelled_but_paid",
        issue_description = "Cancelled orders with payments",
        count = cancelled_but_paid_count
    )
]

issues_log_table_df = spark.createDataFrame(rows).withColumn("job_run_timestamp", current_timestamp())

orphaned_orders_oi_dyf = DynamicFrame.fromDF(orphaned_orders_oi_df, glueContext, "orphaned_orders_oi_dyf")
orphaned_oi_orders_dyf = DynamicFrame.fromDF(orphaned_oi_orders_df, glueContext, "orphaned_oi_orders_dyf")
orphaned_oi_products_dyf = DynamicFrame.fromDF(orphaned_oi_products_df, glueContext, "orphaned_oi_products_dyf")
orphaned_payments_orders_dyf = DynamicFrame.fromDF(orphaned_payments_orders_df, glueContext, "orphaned_payments_orders_dyf")
inconsistent_payments_dyf = DynamicFrame.fromDF(inconsistent_payments_df, glueContext, "inconsistent_payments_dyf")
delivered_no_payments_dyf = DynamicFrame.fromDF(delivered_no_payments_df, glueContext, "delivered_no_payments_dyf")
cancelled_with_payments_dyf = DynamicFrame.fromDF(cancelled_with_payments_df, glueContext, "cancelled_with_payments_dyf")
order_facts_table_dyf = DynamicFrame.fromDF(order_facts_table_df, glueContext, "order_facts_table_dyf")
order_line_item_facts_table_dyf = DynamicFrame.fromDF(order_line_item_facts_table_df, glueContext, "order_line_item_facts_table_dyf")
customer_summary_table_dyf = DynamicFrame.fromDF(customer_summary_table_df, glueContext, "customer_summary_table_dyf")
product_performance_table_dyf = DynamicFrame.fromDF(product_performance_table_df, glueContext, "product_performance_table_dyf")
data_quality_summary_table_dyf = DynamicFrame.fromDF(data_quality_summary_table_df, glueContext, "data_quality_summary_table_dyf")
issues_log_table_dyf = DynamicFrame.fromDF(issues_log_table_df, glueContext, "issues_log_table_dyf")

# ORPHANED RECORDS

# orphaned_orders_oi
s3output = glueContext.getSink(
    path=f"s3://{bucket}/inconsistencies/orphaned_orders_oi",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",  
    partitionKeys=[],      
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="s3output"
)

s3output.setCatalogInfo(
    catalogDatabase=inconsistencies_db, catalogTableName="orphaned_orders_oi"
)

s3output.setFormat("parquet", useGlueParquetWriter=True)
s3output.writeFrame(orphaned_orders_oi_dyf)

# orphaned_oi_orders
s3output = glueContext.getSink(
    path=f"s3://{bucket}/inconsistencies/orphaned_oi_orders",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",  
    partitionKeys=[],      
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="s3output"
)

s3output.setCatalogInfo(
    catalogDatabase=inconsistencies_db, catalogTableName="orphaned_oi_orders"
)

s3output.setFormat("parquet", useGlueParquetWriter=True)
s3output.writeFrame(orphaned_oi_orders_dyf)

# orphaned_oi_products
s3output = glueContext.getSink(
    path=f"s3://{bucket}/inconsistencies/orphaned_oi_products",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",  
    partitionKeys=[],      
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="s3output"
)

s3output.setCatalogInfo(
    catalogDatabase=inconsistencies_db, catalogTableName="orphaned_oi_products"
)

s3output.setFormat("parquet", useGlueParquetWriter=True)
s3output.writeFrame(orphaned_oi_products_dyf)

# orphaned_payments_orders
s3output = glueContext.getSink(
    path=f"s3://{bucket}/inconsistencies/orphaned_payments_orders",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",  
    partitionKeys=[],      
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="s3output"
)

s3output.setCatalogInfo(
    catalogDatabase=inconsistencies_db, catalogTableName="orphaned_payments_orders"
)

s3output.setFormat("parquet", useGlueParquetWriter=True)
s3output.writeFrame(orphaned_payments_orders_dyf)

# FINANCIAL RECONCILIATION
s3output = glueContext.getSink(
    path=f"s3://{bucket}/inconsistencies/inconsistent_payments",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",  
    partitionKeys=[],      
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="s3output"
)

s3output.setCatalogInfo(
    catalogDatabase=inconsistencies_db, catalogTableName="inconsistent_payments"
)

s3output.setFormat("parquet", useGlueParquetWriter=True)
s3output.writeFrame(inconsistent_payments_dyf)

# BUSINESS RULE VIOLATIONS

# delivered_no_payments
s3output = glueContext.getSink(
    path=f"s3://{bucket}/inconsistencies/delivered_no_payments",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",  
    partitionKeys=[],      
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="s3output"
)

s3output.setCatalogInfo(
    catalogDatabase=inconsistencies_db, catalogTableName="delivered_no_payments"
)

s3output.setFormat("parquet", useGlueParquetWriter=True)
s3output.writeFrame(delivered_no_payments_dyf)

# cancelled_with_payments
s3output = glueContext.getSink(
    path=f"s3://{bucket}/inconsistencies/cancelled_with_payments",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",  
    partitionKeys=[],      
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="s3output"
)

s3output.setCatalogInfo(
    catalogDatabase=inconsistencies_db, catalogTableName="cancelled_with_payments"
)

s3output.setFormat("parquet", useGlueParquetWriter=True)
s3output.writeFrame(cancelled_with_payments_dyf)

# AGGREGATION TABLES

# order_facts_table
s3output = glueContext.getSink(
    path=f"s3://{bucket}/clean/order_facts_table",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",  
    partitionKeys=["year", "month"],      
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="s3output"
)

s3output.setCatalogInfo(
    catalogDatabase=clean_db, catalogTableName="order_facts_table"
)

s3output.setFormat("parquet", useGlueParquetWriter=True)
s3output.writeFrame(order_facts_table_dyf)

# order_line_item_facts_table
s3output = glueContext.getSink(
    path=f"s3://{bucket}/clean/order_line_item_facts_table",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",  
    partitionKeys=["year", "month"],      
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="s3output"
)

s3output.setCatalogInfo(
    catalogDatabase=clean_db, catalogTableName="order_line_item_facts_table"
)

s3output.setFormat("parquet", useGlueParquetWriter=True)
s3output.writeFrame(order_line_item_facts_table_dyf)

# customer_summary_table
s3output = glueContext.getSink(
    path=f"s3://{bucket}/clean/customer_summary_table",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",  
    partitionKeys=["customer_state"],      
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="s3output"
)

s3output.setCatalogInfo(
    catalogDatabase=clean_db, catalogTableName="customer_summary_table"
)

s3output.setFormat("parquet", useGlueParquetWriter=True)
s3output.writeFrame(customer_summary_table_dyf)

# product_performance_table
s3output = glueContext.getSink(
    path=f"s3://{bucket}/clean/product_performance_table",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",  
    partitionKeys=[],      
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="s3output"
)

s3output.setCatalogInfo(
    catalogDatabase=clean_db, catalogTableName="product_performance_table"
)

s3output.setFormat("parquet", useGlueParquetWriter=True)
s3output.writeFrame(product_performance_table_dyf)

# DATA QUALITY REPORTS

# data_quality_summary_table
s3output = glueContext.getSink(
    path=f"s3://{bucket}/inconsistencies/data_quality_summary_table",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",  
    partitionKeys=[],      
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="s3output"
)

s3output.setCatalogInfo(
    catalogDatabase=inconsistencies_db, catalogTableName="data_quality_summary_table"
)

s3output.setFormat("parquet", useGlueParquetWriter=True)
s3output.writeFrame(data_quality_summary_table_dyf)

# issues_log_table
s3output = glueContext.getSink(
    path=f"s3://{bucket}/inconsistencies/issues_log_table",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",  
    partitionKeys=[],      
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="s3output"
)

s3output.setCatalogInfo(
    catalogDatabase=inconsistencies_db, catalogTableName="issues_log_table"
)

s3output.setFormat("parquet", useGlueParquetWriter=True)
s3output.writeFrame(issues_log_table_dyf)

job.commit()