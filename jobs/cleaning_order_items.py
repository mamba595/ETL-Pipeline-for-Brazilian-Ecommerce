import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
  
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

order_items_dyf = glueContext.create_dynamic_frame.from_catalog(database=raw_db, table_name='order_items')

order_items_dyf = order_items_dyf.apply_mapping([
    ("order_id","string","order_id","string"),
    ("order_item_id","bigint","order_item_id","string"),
    ("product_id","string","product_id","string"),
    ("seller_id","string","seller_id","string"),
    ("shipping_limit_date","string","shipping_limit_date","timestamp"),
    ("price","double","price","double"),
    ("freight_value","double","freight_value","double")
])

# I transform both DynamicFrames into pyspark Dataframes to use pyspark SQl
order_items_df = order_items_dyf.toDF()
order_items_df_sample = order_items_df.limit(count)

# Step 1: Keep only rows with positive prices
cleaned_oi_df = order_items_df_sample.filter(order_items_df["price"] >= 0.0)

# Step 2: Calculate total price for an item, which is price + freight value (shipping costs)
cleaned_oi_df = cleaned_oi_df.withColumn("total_price", cleaned_oi_df["price"] + cleaned_oi_df["freight_value"])

# Convert the pyspark Dataframe back into a DynamicFrame
cleaned_dyf = DynamicFrame.fromDF(cleaned_oi_df, glueContext, "order_items_dyf")

# Store the cleaned dataset as Parquet in S3
s3output = glueContext.getSink(
  path=f"s3://{bucket}/clean/order_items",
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="s3output",
)
s3output.setCatalogInfo(
  catalogDatabase=processed_db, catalogTableName="order_items"
)
s3output.setFormat("parquet", useGlueParquetWriter=True)
s3output.writeFrame(cleaned_dyf)

job.commit()