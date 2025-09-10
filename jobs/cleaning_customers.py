import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import upper, lower, trim, col
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

dyf = glueContext.create_dynamic_frame.from_catalog(database=raw_db, table_name='customers')

df = dyf.toDF()

df_sample = df.limit(count)

# Step 1: Remove rows with duplicate customer_unique_id values
cleaned_df = df_sample.dropDuplicates(["customer_unique_id"])

# Step 2: Standardize city names
cleaned_df = cleaned_df.withColumn("customer_city", lower(trim(col("customer_city"))))

# Step 3: Fill missing customer_zip_code_prefix values with 0
cleaned_df = cleaned_df.na.fill(0, subset=["customer_zip_code_prefix"])

# Step 4: Validate customer_state codes with the official list for Brazil state codes
brazil_codes = ["AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG",
               "PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO"]

cleaned_df = cleaned_df.withColumn("customer_state", upper(col("customer_state")))

cleaned_df = cleaned_df.filter(upper(col("customer_state")).isin(brazil_codes))

# Convert the pyspark Dataframe back into a DynamicFrame
cleaned_dyf = DynamicFrame.fromDF(cleaned_df, glueContext, "customers_dyf")

# Store the cleaned dataset as Parquet in S3
s3output = glueContext.getSink(
  path=f"s3://{bucket}/clean/customers",
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="s3output",
)
s3output.setCatalogInfo(
  catalogDatabase=processed_db, catalogTableName="customers"
)
s3output.setFormat("glueparquet")
s3output.writeFrame(cleaned_dyf)

job.commit()