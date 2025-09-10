import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import lower, upper, trim, col
  
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

sellers_dyf = glueContext.create_dynamic_frame.from_catalog(database=raw_db, table_name='sellers')

sellers_df = sellers_dyf.toDF()
sellers_df_sample = sellers_df.limit(count)

# Step 1: Drop rows with null value in seller_id column
cleaned_df = sellers_df_sample.dropna(subset=["seller_id"])

# Step 2: Standardize city names and state codes
cleaned_df = cleaned_df.withColumn("seller_city", lower(trim(cleaned_df["seller_city"])))
cleaned_df = cleaned_df.withColumn("seller_state", upper(trim(cleaned_df["seller_state"])))

# Step 3: Drop rows with duplicate seller_id columns
cleaned_df = cleaned_df.dropDuplicates(["seller_id"])

# Step 4: Validate state codes
brazil_codes = ["AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG",
               "PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO"]

cleaned_df = cleaned_df.filter(col("seller_state").isin(brazil_codes))

# Convert the pyspark Dataframe back into a DynamicFrame
cleaned_dyf = DynamicFrame.fromDF(cleaned_df, glueContext, "cleaned_dyf")

# Store the cleaned dataset as Parquet in S3
s3output = glueContext.getSink(
  path=f"s3://{bucket}/clean/sellers",
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="s3output",
)
s3output.setCatalogInfo(
  catalogDatabase=processed_db, catalogTableName="sellers"
)
s3output.setFormat("glueparquet")
s3output.writeFrame(cleaned_dyf)

job.commit()