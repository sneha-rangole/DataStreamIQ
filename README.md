**Project Notes: DataStreamIQ - Real-Time Streaming & Processing of Unstructured Data**  
**Student:** Sneha Rangole | **Group:** 1  

---

### **Project Overview**  
**Objective:** Build a scalable pipeline to process unstructured job descriptions (JSON, TXT, PDF) from the City of Los Angeles, enabling structured querying and visualization.  
**Key Goals:**  
1. **Data Processing:** Transform unstructured data into structured format using PySpark.  
2. **Storage:** Use AWS S3 for reliable, scalable storage.  
3. **Querying:** Catalog data with AWS Glue and enable SQL queries via Athena.  
4. **Insights:** Visualize processed data in Power BI.  

---

### **Tools & Technologies**  
- **PySpark:** Real-time processing, UDFs for parsing unstructured data.  
- **AWS S3:** Store raw/processed data.  
- **AWS Glue:** Data cataloging.  
- **AWS Athena:** Query processed data.  
- **Power BI:** Visualization.  

---

### **Workflow**  
1. **Ingestion:** Collect data from JSON, TXT, and PDF files.  
2. **Processing:**  
   - **PySpark UDFs:** Extract fields (salary, dates, requirements) using regex.  
   - **Unified Schema:** Standardize data across formats.  
3. **Storage:** Save raw/processed data to S3 in Parquet format.  
4. **Cataloging:** Use AWS Glue Crawler to create metadata tables.  
5. **Querying:** Run SQL queries in Athena for analysis.  
6. **Visualization:** Export results to Power BI for dashboards.  

---

### **Code Structure**  
- **`main.py`:**  
  - Sets up Spark Session with AWS credentials.  
  - Reads streaming data from JSON, TXT, and PDF directories.  
  - Applies UDFs to extract structured fields (e.g., `extract_salary`, `extract_end_date`).  
  - Unions data from all sources and writes to S3.  
- **`udf_utils.py`:**  
  - Custom functions for parsing text (e.g., regex patterns for salary ranges, dates).  
  - Handles PDF text extraction using PyMuPDF.  

---

### **Challenges & Solutions**  
1. **Unstructured Data Complexity**  
   - **Issue:** Varied formats (PDF, JSON, TXT) required flexible parsing.  
   - **Solution:** UDFs with regex patterns and PySpark’s schema enforcement.  
2. **AWS Permissions**  
   - **Issue:** Configuring IAM roles and S3 bucket policies.  
   - **Solution:** Defined granular permissions for Glue, Athena, and S3 access.  
3. **Real-Time Processing**  
   - **Issue:** Optimizing Spark Streaming for diverse inputs.  
   - **Solution:** Checkpointing in S3 and microbatch processing (5-second intervals).  

---

### **Results**  
- **Structured Data Lake:** Processed data stored in S3 as Parquet files.  
- **Queryable Catalog:** AWS Glue tables enable SQL queries via Athena.  
- **Actionable Dashboards:** Power BI visualizations for trends in job postings (e.g., salary ranges, application deadlines).  
![Project](https://github.com/user-attachments/assets/ab4d8f8e-6f4f-40f0-b2ca-ddab79644dc7)
