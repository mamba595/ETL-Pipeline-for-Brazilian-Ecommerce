# ETL Pipeline for Brazilian Ecommerce

## Description
In this project, I built an ETL pipeline to process the data from a [Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) from Kaggle with 100,000 rows for 9 datasets, which includes orders, customers, order items, and more.

The purpose of this project was to learn data engineering by building, and that's why I focused on data quality validation, business logic, complex SQL queries and the PySpark library.

I used AWS Glue for initially creating and debugging the notebooks, and then passed them to Python scripts so that I could use Apache Airflow to orchestrate the Glue jobs using a DAG (Directed Acyclic Graph), which allows me to give a concrete order to the jobs to be run with dependencies, so that the business transformations job waited for the cleaning jobs to finish.

In the /jobs folder, there are the Python scripts, which use Apache Airflow variables like BUCKET or RAW_DATABASE, so that it's easier to migrate the data from one bucket to the other without me having to manually change each script, which ends up being monotonous once you have 7 scripts.

In the /dags folder, I have the airflow_dag file that manages the orchestration for the jobs using Apache Airflow.

In the /jobs and /dags folder, there are READMEs to explain each part of the project. I've upload also the AWS Glue notebooks in the /notebooks, which are a little bit informal because I was learning all of these from scratch, so I was trying things out, seeing what worked and debugging wrong schemas in the Data Catalog and more.

## Project Structure
This project required to create an S3 bucket, an AWS Glue crawler with the correct IAM permissions, and an IAM role for the AWS Glue notebooks for me to run them and see if they worked correctly.

The crawler was used for scanning the datasets and infer schemas for the Glue Data Catalog, so that it wasn't necessary to move the datasets to a database, and just with the schemas, a tool like Athena could read the datasets with SQL queries.

The datasets are stored in different folders, with their names, for example, the "olist_order_items_dataset.csv" file will be stored in the "order_items" folder, which will then be stored in the "raw" folder in the S3 bucket.

The processed datasets after the cleaning stage are stored the same way, in the same bucket, but this time in a "clean" folder with Parquet format.

There are 9 datasets, which are "orders", "customers", "order_items", "products", "payments", "sellers", "product_category_name_translation", "geolocation" and "reviews", but the last 2 ones weren't used.

After the cleaning stage, there are 6 processed datasets, which are "orders", "customers", "order_items", "products", "payments" and "sellers", but the last one wasn't used because it wasn't valuable for the metrics to be computed.

Then, in the business transformations job, data quality issues are logged and multiple facts and summary tables are created from joining cleaned datasets, which are previously converted to SQL tables, and aggregations.

The Apache Airflow DAG orchestrates the pipeline so that the cleaning jobs are executed first in parallel and then the business transformations job is executed at last.

## Annotations
I have run the pipeline with a small sample of data, taking only 1000 rows from each dataset for the cleaning stage and up to 10,000 rows for the business transformations stage, but the AWS Glue costs have been very high in my opinion, since I haven't thought 1000 rows would require so expensive.

That's the reason why I haven't run the pipeline with the whole dataset, because the AWS Glue costs would be enormous just to prove the point that it's a production level pipeline, because since I have 9 datasets, each with 100,000 rows, it would be 900,000 rows processed only in the cleaning stage, and then in the business transformations I have create many new tables, some for data quality issues, others for complex joins to provide good aggregated tables for data analysis and others for computing KPIs (Key Perfomance Indicators) for the customers and products.

If AWS Glue costs were cheaper, I would have run the pipeline completely, and then used the results for data analysis and doing PowerBI dashboards, since I was interested in learning about data analysis, but I'll have to leave that for another project.
