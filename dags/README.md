# ETL Pipeline Orchestration using Apache Airflow

For context, Apache Airflow is an application that allows you to define a DAG (Direct Acyclic Graph) that established an order for the jobs to run, and makes sure that when one job fails, the pipeline is stopped, since it would be a waste of resources to run the business transformations job with data that hasn't been cleaned properly because one of the cleaning jobs failed.

You can run Apache Airflow locally, by installing it in a Python virtual environment (venv) or using a Docker container, and it allows you to have a web interface and a database, so that you can monitor your jobs and see which one failed and when, and which one succeeded when running.

The app can also be run in an Amazon Managed Workflows for Apache Airflow, which lets you create an environment to orchestrate Glue jobs with Apache Airflow, even though a VPC and a monthly subscription is required, so it has more overhead but it's more simple because the Glue jobs are already created and you don't have to manage anything. Locally you would need to run your machine as a server, so it's not feasible for production to run Apache Airflow in your machine.

For this pipeline, I took advantage of the cleaning jobs being independent of each other, so I could run each job in parallel, as I set in the DAG. Also, for each job, I provided my own "environment" variables, so that I could change the number of rows to be processed or the bucket to store the data and more, so that it was easier to experiment with different sizes.

The pipeline is scheduled to run every day since October 1st, it doesn't run the pipeline multiple times at once if it missed some days, and it uses the default AWS role for the jobs, which means that if my Glue jobs have an specific IAM role attached that I created for example, then it uses that one rather than using a different one I attached to Apache Airflow.