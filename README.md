# Trip_Transaction_Data_Analytics
This project aims to implement an ETL to make Analytics/Insights on the trip transaction and ride-based source data from SQL Server, utilizing a combination of Azure Data Factory (ADF), Azure Blob Storage (ADLS Gen2), and Azure Databricks, under a Medallion Architecture approach. The project entails data ingestion into a Bronze Zone, data transformation through Azure Databricks, and data loading into Delta tables for the Gold Zone. Moreover, we plan to create a pipeline and schedule it using Azure Data Factory.
## Model Architecture
![Project Model Architecture](https://github.com/zhel316/Trip_Transaction_Data_Analytics/blob/main/figures/archtecture.png?raw=true)
## Fact- Dimension Data Modelling
In the data warehouse, there are two fact tables and four dimension tables.
![Fact- Dimension Data Modelling](https://github.com/zhel316/Trip_Transaction_Data_Analytics/blob/main/figures/Fact-%20Dimension.png?raw=true)
