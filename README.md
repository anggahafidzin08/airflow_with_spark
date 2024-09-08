# Processing Big Data by Utilizing Spark & Airflow

**Here are the flow of the project**

![image](https://github.com/user-attachments/assets/8eba9f2b-1c50-47b8-b1c3-7e9302d50789)

- The pipeline starts with a rental data source stored in a database (in this case I used PostgreSQL, that running inside a Docker container).
- Apache Spark extract the data from DVD rental database, transformed it as needed, and then loaded into a Parquet file format. Parquet is chosen due to its efficiency in handling large amounts of data, making it suitable for big data processing tasks.
- Once the transformed data is available in Parquet format, Pandas is used to further process or interact with this data.
- The processed data is then sent to TiDB via MySQL Connector and SQLAlchemy engine.
- The entire process is orchestrated by Apache Airflow, ensuring that each step, from ETL in Spark to data processing with Pandas and interaction with TiDB, runs smoothly and in sequence.

All components, including Airflow, PostgreSQL, and Spark, are containerized within ***Docker***. This setup ensures a consistent environment, easier deployment, and scalability across different systems, eliminating dependency issues between components.

This project was created based on this [study case](https://imminent-locust-045.notion.site/Session-18-Project-3-Batch-Processing-Using-Airflow-and-Spark-7118ea1e15624e619e22138486443823)
