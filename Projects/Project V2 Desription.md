# *Project : Scalable Server Log Analysis Using PySpark and Delta Lake.*

### Overview:
Designed and implemented a scalable server log analysis pipeline using PySpark on Databricks, focusing on parsing, cleaning, optimizing, and analyzing web server logs. The project demonstrates efficient big data handling, quality checks, and reliable storage for downstream analytics.

## Key Responsibilities and Workflow:
### Data Ingestion:

Loaded raw Apache server logs stored in Databricks FileStore into a Spark DataFrame.

### Log Parsing:

Applied regular expressions (regexp_extract) to parse complex log patterns into structured fields like IP address, timestamp, HTTP method, URL, status code, and user agent.

### Data Cleaning and Validation:

Filtered out records with missing or blank critical fields.

Converted timestamp strings into TimestampType for accurate time-based analysis.

Isolated and stored invalid/bad records separately for auditing purposes.

### Performance Optimization:

Applied DataFrame caching and repartitioning to minimize data shuffle and improve transformation performance.

### Data Analysis:

Aggregated metrics such as:

Top requesting IP addresses.

Status code distribution.

Total response size per URL.

Request patterns aggregated by hourly windows using time-based grouping.

### Reliable Data Storage:

Stored processed, cleaned, and aggregated data outputs in Delta Lake format for ACID transaction support and efficient querying.

### Data Quality Checks:

Implemented simple automated validation assertions to ensure data completeness after processing (e.g., no null timestamps or blank IPs).

## Tools and Technologies Used:
Big Data Processing: Apache Spark (PySpark)

Platform: Databricks (Community Edition)

Data Storage: Delta Lake (Databricks File System)

Language: Python

Orchestration (Future Scope Mentioned): Apache Airflow / Azure Data Factory

# *Highlights:*
Optimized parsing and aggregation over a large log dataset with Spark distributed processing.

Introduced auditing for bad data capture and maintained reliability through Delta Lake storage.

Demonstrated intermediate-level Spark optimizations like caching and repartitioning.

Scalable design, easily extendable to real-time streaming or cloud deployment scenarios.
