# 🚀 End-to-End SQL Data Warehouse Project
 ### Raw Files → Bronze → Silver → Gold → Advanced SQL Analytics

This project presents a full end-to-end SQL Data Warehouse built in SQL Server, starting from raw source data to analytical insights.
It follows a layered architecture where data moves through structured transformation stages before being used for reporting and analysis.
The goal was to solve the common business problem of "Data Silos" by integrating multiple sources into a Single Source of Truth for advanced reporting and business decision-making.

## 🛠 Tech Stack & Skills
**1 Database:**  SQL Server (T-SQL)

**2 Architecture:** Medallion Architecture (Bronze, Silver, Gold)

**3 Data Modeling:** Dimensional Modeling (Star Schema, Fact & Dimension Tables)

**4 ETL/ELT:** Automated SQL scripts for extraction, transformation, and loading

**5 Advanced SQL:** CTEs, Window Functions, Subqueries, Joins, and Data Cleansing

**6 DevOps/Version Control:** Git, GitHub, Documentation

---

## 🏗 Data Architecture & Pipeline

<img width="772" height="456" alt="data_architecture" src="https://github.com/user-attachments/assets/00447883-9ad2-4941-895f-e437d23cd344" />


I implemented a Separation of Concerns (SoC) approach to ensure scalability and data traceability:

**1 Bronze Layer (Raw):**
  * Role: Landing zone for raw data from multiple sources (CSV/Source Systems).

  * Process: Truncate and Insert logic to ensure data is captured exactly as-is.

**2 Silver Layer (Cleaned):**

   * Role: Standardized and cleansed data.

   * Process: Handled data type conversions, null value management, and formatting. Applied data integrity constraints to remove duplicates.

**3 Gold Layer (Analytical):**

   * Role: Business-ready tables optimized for reporting.

   * Process: Implemented a Star Schema with Fact and Dimension tables, enabling lightning-fast queries for reporting tools.



----

## 💎 Key Features & Engineering Highlights
**1 Star Schema Implementation:** Designed highly efficient tables including Fact_Sales, Dim_Customers, and Dim_Products.

**2 Data Quality Assurance:** Built custom SQL Quality Check scripts to validate primary keys, foreign key relationships, and data completeness before loading into the Gold layer.

**3 Performance Optimization:** Utilized indexing and efficient T-SQL logic to minimize processing time during ETL runs.

**4 Documentation:** Created full data flow diagrams and data dictionaries to ensure the system is maintainable by other engineers.

----

## 📊 Business Impact & Analytics

Beyond just moving data, I used Advanced SQL to uncover insights - :

Please find attached [file](https://github.com/AmeeJoshi-MCA/SQL-Server-DataWarehouse-and-Analytics/blob/main/DataAnalysis/%F0%9F%93%8A%20Sales%20Data%20Analysis%20Report.docx) for more detail

| **Analysis**               |   **Focus**                        |
| -------------------        |:----------------------------------:|
|  Yearly Sales Trend        | Revenue growth over time           |
| Running Sales (Cumulative) | Sales acceleration pattern         |
| Product YoY Comparison     | Historical benchmarking            |
| Category Contribution      | Share of total sales               |
| Customer Segmentation      | VIP / Regular / New classification |


-----

## ⚙️ How to Run
* **Clone the Repo:** git clone [https://github.com/AmeeJoshi-MCA/SQL-Server-DataWarehouse-and-Analytics.git](https://github.com/AmeeJoshi-MCA/SQL-Server-DataWarehouse-and-Analytics.git)

* **Environment:** Open SQL Server Management Studio (SSMS).

* **Execute Scripts:** Run the scripts in order (Bronze -> Silver -> Gold).

* **Validate:** Run the scripts in the Quality_Checks folder to verify data integrity.
  
-----

## 🏁 Conclusion
This project successfully demonstrates a production-grade data engineering workflow—transitioning from fragmented, raw data to a high-performance Single Source of Truth. By implementing the Medallion Architecture, I ensured that the data is not only accessible but also governed, cleaned, and optimized for business logic.

Through this build, I have mastered the bridge between Data Infrastructure (ETL/Modeling) and Data Insights (Advanced SQL Analysis), proving my ability to handle the complexities of a modern data stack.


