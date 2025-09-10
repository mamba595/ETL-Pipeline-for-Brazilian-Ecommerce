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

products_dyf = glueContext.create_dynamic_frame.from_catalog(database=raw_db, table_name='products')
category_translations_dyf = glueContext.create_dynamic_frame.from_catalog(database=raw_db, table_name='product_category_name_translation')

# fix the typos in product_name_length and product_description_length columns
products_dyf = products_dyf.apply_mapping([
    ("product_id","string","product_id","string"),
    ("product_category_name","string","product_category_name","string"),
    ("product_name_lenght","bigint","product_name_length","bigint"),
    ("product_description_lenght","bigint","product_description_length","bigint"),
    ("product_photos_qty","bigint","product_photos_qty","bigint"),
    ("product_weight_g","bigint","product_weight_g","bigint"),
    ("product_length_cm","bigint","product_length_cm","bigint"),
    ("product_height_cm","bigint","product_height_cm","bigint"),
    ("product_width_cm","bigint","product_width_cm","bigint"),
])

# wrong crawler schema, fixed this
category_translations_dyf = category_translations_dyf.apply_mapping([
    ("col0", "string", "product_category_name", "string"),
    ("col1", "string", "product_category_name_english", "string")
])

# I transform both DynamicFrames into pyspark Dataframes to use pyspark SQl
products_df = products_dyf.toDF()
category_translations_df = category_translations_dyf.toDF()

products_df.createOrReplaceTempView("products")
category_translations_df.createOrReplaceTempView("category_translations")

joined_product_df = spark.sql("""
    SELECT * FROM products p1
    JOIN category_translations c1 
    ON p1.product_category_name=c1.product_category_name
""")

joined_product_df = joined_product_df.limit(count)

# Step 1: Drop all columns whose 'product_id' column is null
cleaned_jp_df = joined_product_df.dropna(subset=["product_id"])

# Step 2: Calculate the dimensions of the product
cleaned_jp_df = cleaned_jp_df.withColumn("product_dimension_cm3", cleaned_jp_df["product_length_cm"] * 
                                                                  cleaned_jp_df["product_height_cm"] *
                                                                  cleaned_jp_df["product_width_cm"])

# Convert the pyspark Dataframe back into a DynamicFrame
cleaned_dyf = DynamicFrame.fromDF(cleaned_jp_df, glueContext, "products_dyf")

# Store the cleaned dataset as Parquet in S3
s3output = glueContext.getSink(
  path=f"s3://{bucket}/clean/products",
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="s3output",
)
s3output.setCatalogInfo(
  catalogDatabase=processed_db, catalogTableName="products"
)
s3output.setFormat("glueparquet")
s3output.writeFrame(cleaned_dyf)

job.commit()