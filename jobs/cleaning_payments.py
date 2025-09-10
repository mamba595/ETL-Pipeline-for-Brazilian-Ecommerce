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

payments_dyf = glueContext.create_dynamic_frame.from_catalog(database=raw_db, table_name='order_payments')

payments_dyf = payments_dyf.apply_mapping([
    ("order_id", "string", "order_id", "string"),
    ("payment_sequential", "long", "payment_sequential", "long"),
    ("payment_type", "string", "payment_type", "string"),
    ("payment_installments", "long", "payment_installments", "long"),
    ("payment_value", "double", "payment_value", "double")
])

payments_df = payments_dyf.toDF()
payments_df_sample = payments_df.limit(count)

# Step 1: Drop rows with null values in order_id and payment_value columns
cleaned_df = payments_df_sample.dropna(subset=["order_id","payment_value"])

# Step 2: Keep rows with positive payment_value column
cleaned_df = cleaned_df.filter(cleaned_df["payment_value"] > 0)

# Step 3: Drop duplicate columns
cleaned_df = cleaned_df.dropDuplicates()

# Convert the pyspark Dataframe back into a DynamicFrame
cleaned_dyf = DynamicFrame.fromDF(cleaned_df, glueContext, "cleaned_dyf")

# Store the cleaned dataset as Parquet in S3
s3output = glueContext.getSink(
  path=f"s3://{bucket}/clean/payments",
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="s3output",
)
s3output.setCatalogInfo(
  catalogDatabase=processed_db, catalogTableName="payments"
)
s3output.setFormat("parquet", useGlueParquetWriter=True)
s3output.writeFrame(cleaned_dyf)

job.commit()