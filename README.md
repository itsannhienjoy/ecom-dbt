# E-Commerce Analysis – Dagster Azure
Technical Architecture & Implementation Documentation

## Project Overview

The E-Commerce Analysis built with Dagster, dbt and Azure is developed to automate and streamline the entire data workflow for e-commerce analytics. Leveraging Azure Data Factory for ingestion, Azure Data Lake for storage, Snowflake as the data warehouse, dbt for transformation and testing, and Dagster for orchestration, this pipeline enables businesses to collect, clean, and analyze their data with minimal manual effort.

## Data Source

The raw data for this project consists of multiple Excel files containing e-commerce transactions, customer information, product details, reviews, and related business data. These files were originally collected and maintained by business stakeholders and stored centrally in a Google Drive folder for easy access and collaboration.
To automate the ingestion process, Azure Data Factory was configured to connect to Google Drive, periodically download the latest Excel files, and land them in the raw zone of Azure Data Lake Storage Gen2 (ADLS Gen2). This approach ensures that the pipeline always works with the most up-to-date data, while maintaining a secure and auditable data ingestion process.

## Project Architecture

Our data pipeline is designed following the Lakehouse architecture paradigm, combining the scalability and flexibility of a data lake with the reliability and performance of a data warehouse. This approach enables us to efficiently ingest, store, process, and analyze large volumes of e-commerce data, while maintaining strong data governance and supporting advanced analytics use cases.

1. Data Ingestion

Azure Data Factory (ADF) is used to automate the extraction of these raw Excel files from Google Drive. ADF pipelines are scheduled to run at regular intervals, ensuring that the latest data is always ingested.
![Pipeline Debug](./pl_debug.png)
![Pipeline Run](./pipeline_runs.png)
![Trigger Run](./trigger_runs.png)

2. Data Lake Storage (Raw and Processed Zones)
Process:
Ingested files are landed in the raw zone of Azure Data Lake Storage Gen2 (ADLS Gen2), preserving the original data for traceability and auditability.
Initial data cleaning, type enforcement, and normalization (e.g., standardizing text case, removing duplicates) are performed using ADF Data Flows or Azure Databricks. The cleansed data is then written as Parquet files to the processed zone of ADLS Gen2.
Rationale:
ADLS Gen2 provides scalable, secure, and cost-effective storage for both raw and processed data. The Lakehouse approach allows us to decouple storage from compute, retain all historical data, and support both batch and streaming workloads.
3. Data Warehousing (Lakehouse Analytics Layer)
Process:
Processed data is loaded from ADLS Gen2 into Snowflake, which serves as the analytics layer of our Lakehouse architecture.
Technology:
Snowflake is chosen for its elastic compute, separation of storage and compute, and strong support for semi-structured data.
Rationale:
By using Snowflake as the Lakehouse analytics engine, we enable high-performance SQL analytics, data sharing, and seamless integration with BI tools, while maintaining the flexibility of the underlying data lake.
4. Transformation & Data Modeling
Process:
dbt (Data Build Tool) is used to transform raw and processed data into analytics-ready models, following best practices in dimensional modeling (star schema: fact and dimension tables). dbt also enforces data quality through automated tests and generates model documentation.
Rationale:
dbt enables modular, version-controlled SQL transformations, supports automated testing, and provides clear lineage, which is essential for maintainability and governance in a modern data stack.
5. Orchestration & Containerization
Process:
Dagster orchestrates the end-to-end workflow, managing dependencies, scheduling, and monitoring each stage of the pipeline.
All pipeline components (Dagster, dbt, supporting services) are containerized using Docker and orchestrated with Docker Compose.
Rationale:
Dagster provides a modern orchestration framework with strong support for data assets, observability, and integration with dbt and cloud services.
Containerization ensures consistent, reproducible environments across development, testing, and production, simplifying deployment and scaling.
6. Analytics & Reporting
Process:
Power BI connects directly to Snowflake, enabling business users to explore data, build dashboards, and generate insights in real time.
Rationale:
Power BI is a widely adopted BI tool that integrates seamlessly with Snowflake, supporting self-service analytics and interactive reporting.
