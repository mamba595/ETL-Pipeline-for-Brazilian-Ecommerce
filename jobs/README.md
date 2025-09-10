# Jobs: PySpark scripts

## Description
In this folder, I will document each job separately, even though the cleaning jobs are very similar for the beginning and the end, even though the data cleaning is different.

As I mentioned in the general README of the project, I take 7 datasets, clean them and join some of them, and then I store the processed datasets, so that the business transformations job take these datasets, and find data quality issues and creates new tables from joining multiple tables at once, whose SQL queries will be explained in depth since they were long and complex, and required a good schema design.

All the scripts create initially a Spark Context, which is the connection point with Apache Spark API, which in the case of Glue jobs, would run in the Glue DPUs (Data Processing Units), then Glue Context adds AWS functionalities that allows me to connect with AWS services, letting me import the data from the S3 bucket or use DynamicFrames, which are the AWS equivalent of PySpark Dataframes. And then, the Spark Session, which is the abstraction of the Spark Context, but with more capabilities, including the Dataframes and the ability to use Spark SQL to run SQL queries with Dataframes.

## Cleaning stage
The jobs in this phase follow the same blueprint, importing the datasets as DynamicFrames, convert them to PySpark Dataframes and limit the number of samples to what I set in the Apache Airflow DAG tasks. At the end, the Dataframes are converted back to DynamicFrames, and stored in their respective folder in the S3 bucket, and it includes a schema added to the Glue Data Catalog, so that the data can be imported by other job, and read.

In most of the scripts, I modify the schema because the Glue crawler didn't infer it correctly, so I manually set it. In some cases, I only had "col0, ..., col7" schemas which were a mess because it wouldn't let me to join the tables in the business transformations job.

### Orders
In the orders script, being this the first one I developed, I remove rows with "order_id" column with null values, because without that value, you can't work or identify in any way possible with that order. Then, I standardized the column "status" by making the characters lowercase and removing spaces, so that it was easier to compare later, because I wanted to make sure string comparisons where reliable in the following stage. Finally, I removed all columns with duplicated "column_id".

### Customers
For customers, I dropped rows with duplicate "customer_id" column, standardize the "customer_city" names, filled the missing "customer_zip_code_prefix" with 0 and validated the "customer_state" codes with the official list of Brazil state codes.

### Order Items
For order items, I kept only the rows with non negative prices and calculated total price for an item for financial reconciliation validation in the next stage.

### Products
For products, I joined the products dataset with the "product_category_name_translation" dataset, because the products one had the category in portugues, while the translation dataset provided the translation for each one in english. After joining tables, I removed rows with "product_id" missing values and calculated the product dimension in volume, by using the product's length, height and width.

### Payments
For payments, I removed rows with missing values in "order_id" and "payment_value" because I only wanted payments with real orders and real payments, kept only rows with positive "payment_value" because a payment requires money transaction, one can't just pay 0 dollars. And finally, I dropped duplicate columns so that they didn't mess up with the financial reconciliation validation in the next stage.

### Sellers
Even though, this wasn't used in the next stage, I still did the data cleaning, by removing rows with missing "seller_id" values, standardizing "seller_city" names and "seller_state" codes, dropping rows with duplicate "seller_id" columns and validating "seller_state" with the official list of Brazil state codes.


## Business Transformations stage
This stage is very extensive, I import the cleaned datasets, limit their size and create temporal tables, because I will dive deep into complex SQL queries.

### Orphaned Record Detection
I validate the data quality by comparing foreign keys and making sure the data makes sense and it's consistent, since it's an issue when there are orders without order items, because how can a customer place an order with no items, or when items have been ordered, but they don't belong to an existing order. I also check items whose product they are referencing doesn't exist, because a customer shouldn't be able to order something the store doesn't sell, and payments without orders.

All of these inconsistencies are issues, because they leave rows that have foreign keys that do not exist, which is why they are called orphaned records, because no order can claim the item or no payment can claim the order it belongs to.

### Financial Reconciliation
In this step of this job, I make sure the financial data is consistent, because in a real business, a customer must pay the same amount its order costs, neither more nor less, since in that case it would be underpaid or overpaid, or worse, unpaid.

For this, I sum all the costs for each item in each order, and I do the same for the payments, so that I get the amount to be paid for the order, and the amount that was paid by the customer, and I add a tolerance threshold of $0.01 since we are working with float point values and they can round up the wrong way sometimes. If the difference between what has been paid and the amount to be paid is bigger than $0.01, the issue is logged to be reviewed later, since there could be an issue with the payment process or an exploit that customers are using to pay less for the products.

### Business Rule Validation
No order can be delivered and unpaid, or cancelled but paid for, since it doesn't make sense with the logic behind the business operations, and it threatens the business to sell items losing money or getting sued for financial fraud, when maybe the issue is in the payment process or in updating the information received by the supply chain.

### Order Facts Table
This is the first of the complex SQL queries that required joining and aggregating multiple cleaned datasets, to present a complete order facts table, with all the information necessary to understand the orders and dissect them as necessary. I join 5 tables in total, being 2 "total_oi" and "total_p" which are the tables previously computed to know the amount to be paid and that has been paid per order, and I make sure to account the payment status for each order and customer, as well as the number of items and unique products ordered, and adding also the days for the delivery to arrive, if it hasn't arrived yet, and the year and month so that the table could be partitioned by those values, since an online store would receive orders each month, and the dataset could become huge, and therefore harder to analyze in the future.

To summarize, it shows all the complete information available for each order.

### Order Line Item Facts Table
This is similar to the previous one, but this time it focuses more on the products, to understand better the items that were ordered, and this way, I can provide complete information with 2 tables rather than introducing too much information in just one. It performs a similar complex join and it groups by multiple columns too.

This table is also partitioned by year and month.

### Customer Summary Table
Taking the order facts table, I compute KPIs for the individual customers, such as average value per order, average days between orders and more, including segmenting customers into different groups, so that customers can be studied and analyzed separately for their complete behavior rather than having to find them in a sea of orders. The table is partitioned by "customer_state" codes.

### Product Performance Table
Using the same concept as the previous table, I take the order line item facts table to compute metrics on the performance of individual products, such as number of customers and expected revenue per product, which can help a business know in which products to focus on selling more. This table is not partitioned since the amounts of products offered to be sold in a store doesn't change as much as the number of orders, items ordered or customers, since these tables get new rows each month.

### Data Quality Summary Table
This table shows, as its name indicates, a summary of the data quality issues and the general state of the data, and each time the pipeline is run, it creates a row, and it appends the row to an existing, if not already created, table, that is stored as Parquet format, and allows me to see the health of the data for each run.

### Issues Log Table
Works similarly as the data quality summary table, but this time, all the data quality issues that were found in this job, will be added to a table, with one per type of data quality, so that each issue has a separate count and description.

