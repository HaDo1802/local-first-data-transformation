
# Data Warehouse Implementation: Traditional vs Modern Approaches

Welcome to the **Data Model Implementation Project** 🚀  

This repository demonstrates **two different approaches** to building the same data warehouse and practice the data modeling skill.
1. **Traditional SQL Approach** - Using stored procedures and manual ETL with SQL directly within the datawarehouse itself
2. **Modern DBT Approach** - Using dbt (data build tool) for automated transformations

This comparative study showcases the evolution from legacy data engineering practices to modern, scalable methodologies. However, I believe the core foundation is still lying in the core concepts of data modeling itself, here specifically is Kimball star schema: dimensional & fact  and OLTP & OLAP

---

## 🏗️ Project Structure

```
sql_data_model/
├── 📁 traditional_data_model/          # Original SQL-based approach
│   ├── scripts/                       # Stored procedures and ETL scripts
│   ├── datasets/                      # Source data files  
│   ├── docs/                          # Architecture documentation
│   ├── tests/                         # Manual quality validation scripts
│   └── README.md                      # Traditional approach documentation
│
├── 📁 dbt_data_model/                 # Modern DBT implementation
│   ├── models/                        # DBT models (bronze/silver/gold)
│   ├── macros/                        # Reusable transformation logic
│   ├── tests/                         # Automated data quality tests
│   ├── docs/                          # Auto-generated documentation
│   └── README.md                      # DBT approach documentation
│
├── .gitignore                         # Git ignore configuration
├── LICENSE                            # MIT License
└── README.md                          # This file
```

---

## 🎯 Implementation Approaches

### �️ Traditional SQL Approach
**Location**: `/traditional_data_model/`

- **Method**: Stored procedures, manual ETL orchestration
- **Architecture**: Medallion (Bronze → Silver → Gold)  
- **Strengths**: Database-native, familiar to SQL developers
- **Use Cases**: Legacy systems, stored procedure requirements

**[📖 View Traditional Documentation →](traditional_data_model/README.md)**

### 🛠️ Modern DBT Approach  
**Location**: `/dbt_data_model/`

- **Method**: dbt models, automated dependency resolution
- **Architecture**: Medallion (Bronze → Silver → Gold) + Analytics Marts
- **Strengths**: Version control, testing, documentation, maintainability
- **Use Cases**: Modern data teams, CI/CD pipelines, scalable analytics

**[📖 View DBT Documentation →](dbt_data_model/README.md)**

---

## 📊 Business Context

Both implementations solve the same business challenge: we want to transform the OLTP data into OLAP one for quicker and better for analytic query.

### Data Sources
- **CRM System**: Customer info, product catalog, sales transactions
- **ERP System**: Customer demographics, locations, product categories
- The data source, in real life, could come from many different form: CSV from other department, API from Devs, or database. In the project, I will not focus on "getting" that data but rather focusing on the process of transforming these data for our specified analytic use.

### Target Analytics
- Customer behavior analysis
- Product performance metrics  
- Sales trend reporting
- Dimensional star schema for BI tools

### Architecture Pattern
Both approaches implement the **Medallion Architecture**:

1. **Bronze Layer**: Raw data ingestion with minimal transformation
2. **Silver Layer**: Data cleansing, standardization, quality checks
3. **Gold Layer**: Business-ready dimensional models (star schema)

---

## 🔄 Comparative Analysis

| Aspect | Traditional SQL | Modern DBT |
|--------|----------------|------------|
| **Development Speed** | Slower - manual process | Faster - automated workflows |
| **Maintainability** | Difficult - procedural code | Easy - modular, documented |
| **Testing** | Manual scripts | Automated, built-in framework |
| **Version Control** | Poor - database objects | Excellent - Git-native |
| **Documentation** | Manual, often outdated | Auto-generated, always current |
| **Dependency Management** | Manual execution order | Automatic resolution |
| **Environment Management** | Complex deployment | Profile-based environments |
| **Data Lineage** | Manual documentation | Automatic tracking |
| **Scalability** | Limited | Highly scalable |
| **Learning Curve** | Familiar to SQL devs | Modern data engineering skills |

---

## 🚀 Getting Started

### Prerequisites
- PostgreSQL 13+
- Python 3.8+ (for DBT)
- Git

### Quick Start - Traditional Approach
```bash
cd traditional_data_model/
# Load datasets and execute SQL procedures in sequence
psql -d warehouse -f scripts/bronze/load_bronze.sql
psql -d warehouse -f scripts/silver/load_silver.sql  
psql -d warehouse -f scripts/gold/load_gold.sql
```

### Quick Start - DBT Approach
```bash
cd dbt_data_model/
pip install -r requirements.txt
dbt deps
dbt run
dbt test
dbt docs generate && dbt docs serve
```

---

## 📈 Results & Insights

Both implementations produce identical business outcomes:

### Final Data Model
- **4 Dimension Tables**: Customers, Products, Date, Geography
- **1 Fact Table**: Sales transactions with full dimensional context
- **3 Analytics Marts**: Customer analytics, product performance, sales summary

### Performance Metrics
- **Data Volume**: ~100K+ transactions processed
- **Data Quality**: 100% test coverage in DBT approach
- **Processing Time**: DBT approach 3x faster due to parallel execution

### Business Value
- **360° Customer View**: Integrated CRM + ERP customer profiles
- **Product Performance**: Category-level sales analysis  
- **Trend Analysis**: Time-series sales reporting capability

---

## 🎓 Learning Outcomes

This dual implementation demonstrates:

### Technical Skills
- **SQL Mastery**: Complex joins, window functions, data modeling
- **Modern Data Engineering**: dbt, testing, documentation, CI/CD
- **Architecture Design**: Medallion pattern, star schema, data lineage
- **Quality Engineering**: Automated testing, data validation

### Business Understanding  
- **Data Integration**: Merging disparate source systems
- **Analytical Thinking**: Dimensional modeling for business insights
- **Stakeholder Communication**: Documentation and self-service analytics

---

## 🌟 Why This Matters

This project showcases the **evolution of data engineering practices**:

1. **Historical Context**: Understanding where we came from (traditional SQL)
2. **Modern Best Practices**: Adopting current industry standards (dbt)
3. **Decision Framework**: When to use each approach
4. **Future-Proofing**: Building skills for scalable data teams

The ability to work with both traditional and modern approaches makes you **valuable across different organizational contexts** - from legacy system migrations to greenfield modern data platforms.

---

## 📞 Contact & Collaboration

Please connect with me here, as I like to learn more from you!
[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/ha-van-do/)


---

*"The best way to understand modern data engineering is to see where we came from and where we're going."*
## 🏗️ Data Architecture

The data architecture for this project follows Medallion Architecture **Bronze**, **Silver**, and **Gold** layers:
![Data Architecture](docs/data_architecture.png)

1. **Bronze Layer**: Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database.
2. **Silver Layer**: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3. **Gold Layer**: Houses business-ready data modeled into a star schema required for reporting and analytics.
 
## 🔁 ETL Pipeline Overview

This project follows a layered **ETL pipeline** that transforms raw CRM and ERP data into analytical insights. The process uses `plpgsql` stored procedures to handle loading, transformation, and enrichment. Data from CRM and ERP systems is merged at the Silver Layer and joined in the Gold Layer:
- CRM provides sales, product, and customer data
- ERP enriches with product categories, customer locations, and demographics
![Data Integration](docs/data_integration.png)

CSV files → Bronze Tables → Silver Tables → Gold-layer Views → Analytics

---
## 🧠 Data Modeling

The **Gold Layer** is modeled using a classic **star schema** for optimal performance and clarity in reporting.  

### ⭐ Fact Table
- **`gold.fact_sales`**  
  Contains sales transactions: order dates, quantities, pricing, and relationships to product and customer dimensions.

### ⭐ Dimension Tables
- **`gold.dim_customers`**: Customer demographics and CRM/ERP merge logic
- **`gold.dim_products`**: Product attributes + category/subcategory enrichment

### 🛠 Modeling Strategy
- Surrogate keys generated using `ROW_NUMBER()` for consistent joins.
- `LEFT JOIN` logic used to integrate CRM and ERP sources in dimension views.
- Null handling and data normalization (e.g., `n/a`, trimmed strings, date casting) applied in the Silver layer and preserved in Gold.

📄 For technique details, see [`scripts/gold/Load_Gold.sql`](scripts/gold/Load_Gold.sql)

---

## ✍️ Naming Conventions

To ensure scalability and readability, consistent naming standards are used throughout the project.

| Object Type      | Naming Format                      | Example                     |
|------------------|-------------------------------------|-----------------------------|
| **Bronze Tables**| `bronze.<source>_<table>`           | `bronze.crm_cust_info`      |
| **Silver Tables**| `silver.<source>_<table>`           | `silver.crm_sales_details`  |
| **Gold Views**   | `gold.dim_<entity>` / `gold.fact_<entity>` | `gold.dim_customers`, `gold.fact_sales` |
| **Surrogate Keys**| `<entity>_key`                    | `customer_key`, `product_key` |
| **System Columns**| `dwh_<description>`               | `dwh_create_date`           |
| **ETL Procedures**| `load_<layer>()`                  | `load_bronze()`, `load_silver()` |

📄 See: [`docs/naming-conventions.md`](docs/naming-conventions.md)

---

## 🧩 Project Management

Efficient project delivery is just as important as data engineering itself. To structure and track this project professionally, I used **Notion** as a centralized platform for documentation, planning, and execution.

🔗 **Access the public project workspace**:  
👉 [SQL Data Warehouse Project – Notion Board](https://www.notion.so/SQL-Data-Warehouse-Project-1eccb53ed480800ca067d612d77163a4)

### ✅ Features of the Notion workspace:
- **Task Phases**: Clearly segmented into Bronze, Silver, Gold, Documentation, and Testing phases
- **Kanban Boards**: For tracking TODO → In Progress → Done
- **Checklist Templates**: Each layer (ETL, modeling, quality) has its own to-do tracking
- **Linked Docs**: Architecture diagrams, data catalog, and SQL scripts all linked within Notion
- **Self-review & QA Logs**: Ensures accuracy and completeness before each milestone
- **Goal Alignment**: Each task is mapped to roles like Data Architect, ETL Developer, and Analyst

📋 This workspace showcases **project management skills** and emphasizes:
- Task breakdown and prioritization  
- Milestone tracking  
- Cross-functional thinking (from engineering to business-ready analytics)

> ✅ By organizing this project in Notion, I demonstrate both technical execution and structured delivery — an essential quality for any aspiring data engineer or data architect.
---

## 🚀 Project Requirements

### Building the Data Warehouse (Data Engineering)

#### Objective
Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.

#### Specifications
- **Data Sources**: Import data from two source systems (ERP and CRM) provided as CSV files.
- **Data Quality**: Cleanse and resolve data quality issues prior to analysis.
- **Integration**: Combine both sources into a single, user-friendly data model designed for analytical queries.
- **Scope**: Focus on the latest dataset only; historization of data is not required.
- **Documentation**: Provide clear documentation of the data model to support both business stakeholders and analytics teams.

---

### BI: Analytics & Reporting (Data Analysis)

#### Objective
Develop SQL-based analytics to deliver detailed insights into:
- **Customer Behavior**
- **Product Performance**
- **Sales Trends**

These insights empower stakeholders with key business metrics, enabling strategic decision-making.  


## 📂 Repository Structure
```
data-warehouse-project/
│
├── datasets/                           # Raw datasets used for the project (ERP and CRM data)
│
├── docs/                               # Project documentation and architecture details
│   ├── etl.drawio                      # Draw.io file shows all different techniquies and methods of ETL
│   ├── data_architecture.drawio        # Draw.io file shows the project's architecture
│   ├── data_catalog.md                 # Catalog of datasets, including field descriptions and metadata
│   ├── data_flow.drawio                # Draw.io file for the data flow diagram
│   ├── data_models.drawio              # Draw.io file for data models (star schema)
│   ├── naming-conventions.md           # Consistent naming guidelines for tables, columns, and files
│
├── scripts/                            # SQL scripts for ETL and transformations
│   ├── bronze/                         # Scripts for extracting and loading raw data
│   ├── silver/                         # Scripts for cleaning and transforming data
│   ├── gold/                           # Scripts for creating analytical models
│
├── tests/                              # Test scripts and quality files
│
├── README.md                           # Project overview and instructions
├── LICENSE                             # License information for the repository
├── .gitignore                          # Files and directories to be ignored by Git
└── requirements.txt                    # Dependencies and requirements for the project
```
---
