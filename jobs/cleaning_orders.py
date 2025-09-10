import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import lower, regexp_replace, col
  
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'BUCKET',
    'RAW_DATABASE',
    'PROCESSED_DATABASE',
    'COUNT'
])
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket = args['BUCKET']
raw_db = args['RAW_DATABASE']
processed_db = args['PROCESSED_DATABASE']
count = args['COUNT']

dyf = glueContext.create_dynamic_frame.from_catalog(database=raw_db, table_name='orders')

dyf = dyf.apply_mapping([
    ("col0","string","order_id","string"),
    ("col1","string","customer_id","string"),
    ("col2","string","order_status","string"),
    ("col3","string","order_purchase_timestamp","timestamp"),
    ("col4","string","order_approved_at","timestamp"),
    ("col5","string","order_delivered_carrier_date","timestamp"),
    ("col6","string","order_delivered_customer_date","timestamp"),
    ("col7","string","order_estimated_delivery_date","timestamp")
])

df = dyf.toDF()
df_sample = df.limit(count)

# Data cleaning

# Step 1: remove all the rows whose order_id column is null
cleaned_df = df_sample.filter(df_sample.order_id.isNotNull())

# Step 2: standardize the column 'status', to remove spaces and makes sure all values are in lowercase
cleaned_df = cleaned_df.withColumn("order_status", lower(regexp_replace(col("order_status"), " ", "")))

# Step 3: remove all columns with duplicate order_id values 
cleaned_df = cleaned_df.dropDuplicates(["order_id"])

cleaned_dyf = DynamicFrame.fromDF(cleaned_df, glueContext, "orders_dyf")

# Write the cleaned data to S3 in Parquet format as output for the next job
s3output = glueContext.getSink(
  path=f"s3://{bucket}/clean/orders",
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="s3output",
)
s3output.setCatalogInfo(
  catalogDatabase=processed_db, catalogTableName="orders"
)
s3output.setFormat("glueparquet")
s3output.writeFrame(cleaned_dyf)

job.commit()