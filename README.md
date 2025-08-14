# E-Commerce Analysis – Dagster Azure
Technical Architecture & Implementation Documentation

## Project Overview

The E-Commerce Analysis built with Dagster, dbt and Azure is developed to automate and streamline the entire data workflow for e-commerce analytics. Leveraging Azure Data Factory for ingestion, Azure Data Lake for storage, Snowflake as the data warehouse, dbt for transformation and testing, and Dagster for orchestration, this pipeline enables businesses to collect, clean, and analyze their data with minimal manual effort.

## Data Source

The raw data for this project consists of multiple Excel files containing e-commerce transactions, customer information, product details, reviews, and related business data. These files were originally collected and maintained by business stakeholders and stored centrally in a Google Drive folder for easy access and collaboration.
To automate the ingestion process, Azure Data Factory was configured to connect to Google Drive, periodically download the latest Excel files, and land them in the raw zone of Azure Data Lake Storage Gen2 (ADLS Gen2). This approach ensures that the pipeline always works with the most up-to-date data, while maintaining a secure and auditable data ingestion process.

## Project Architecture

Our data pipeline is designed following the Lakehouse architecture paradigm, combining the scalability and flexibility of a data lake with the reliability and performance of a data warehouse. This approach enables us to efficiently ingest, store, process, and analyze large volumes of e-commerce data, while maintaining strong data governance and supporting advanced analytics use cases.

### Data Ingestion

Azure Data Factory (ADF) orchestrates the ingestion process by connecting to Google Drive and retrieving the latest raw Excel files. The pipeline is configured to run on a scheduled basis, automatically copying new or updated files from Google Cloud storage into the raw zone of Azure Data Lake Storage Gen2 (ADLS Gen2). This ensures that the data lake is consistently populated with up-to-date source data.

![Pipeline Debug](./pl_debug.png)

![Pipeline Run](./pipeline_runs.png)

![Trigger Run](./trigger_runs.png)

### Data Lake Storage (Raw and Processed Zones)

Ingested files are landed in the raw zone of Azure Data Lake Storage Gen2, preserving the original data for traceability and auditability.
Initial data cleaning, type enforcement, and normalization (e.g., standardizing text case, removing duplicates) are performed using ADF Data Flows or Azure Databricks. The cleansed data is then written as Parquet files to the processed zone of ADLS Gen2.

