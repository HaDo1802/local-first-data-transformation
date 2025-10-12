# SQL Data Warehouse - DBT Implementation

[![DBT Version](https://img.shields.io/badge/dbt-1.6+-blue.svg)](https://docs.getdbt.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13+-blue.svg)](https://www.postgresql.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Data Models](#data-models)
- [Getting Started](#getting-started)
- [Development Workflow](#development-workflow)
- [Data Quality & Testing](#data-quality--testing)
- [Deployment](#deployment)
- [Business Intelligence](#business-intelligence)
- [Contributing](#contributing)
- [Support](#support)

## 🎯 Overview

This DBT project implements a modern data warehouse using the **Medallion Architecture** (Bronze → Silver → Gold) for a retail sales organization. The project transforms raw CRM and ERP data into business-ready analytical models following dimensional modeling best practices.

### 🎪 Business Context

The data warehouse consolidates sales data from two primary source systems:
- **CRM System**: Customer information, product catalog, and sales transactions
- **ERP System**: Customer demographics, geographic data, and product categories

### 🏆 Key Features

- ✅ **Medallion Architecture**: Bronze (raw) → Silver (cleaned) → Gold (business-ready) → Marts (analytics)
- ✅ **Star Schema Design**: Optimized dimensional models for analytics
- ✅ **Data Quality Framework**: Comprehensive testing and validation
- ✅ **Incremental Loading**: Performance-optimized for large datasets
- ✅ **Self-Documenting**: Auto-generated documentation and lineage

## 🏗️ Architecture



### Medallion Architecture Layers

| Layer | Purpose | Materialization | Schema | Data Quality |
|-------|---------|----------------|--------|--------------|
| **Bronze** | Raw data ingestion | Table | `bronze` | Basic validation |
| **Silver** | Cleaned & standardized | Table/Incremental | `silver` | Comprehensive testing |
| **Gold** | Business-ready dimensions | View/Table | `gold` | Business rule validation |


## 📁 Project Structure

```
sql_data_warehouse/
├── README.md                           # This file
├── dbt_project.yml                     # Project configuration
├── packages.yml                        # DBT package dependencies
├── profiles.yml.example                # Connection profile template
├── .gitignore                          # Git ignore patterns
│
├── models/                             # DBT models
│   ├── bronze/                         # Raw data ingestion layer
│   │   ├── _bronze__sources.yml        # Source system definitions
│   │   ├── _bronze__models.yml         # Bronze model documentation
│   │   ├── bronze_crm_cust_info.sql    # CRM customer raw data
│   │   ├── bronze_crm_prd_info.sql     # CRM product raw data
│   │   ├── bronze_crm_sales_details.sql # CRM sales raw data (incremental)
│   │   ├── bronze_erp_cust_az12.sql    # ERP customer demographics
│   │   ├── bronze_erp_loc_a101.sql     # ERP location data
│   │   └── bronze_erp_px_cat_g1v2.sql  # ERP product categories
│   │
│   ├── silver/                         # Cleaned and standardized layer
│   │   ├── _silver__models.yml         # Silver model documentation
│   │   ├── silver_crm_customers.sql    # Cleaned CRM customers
│   │   ├── silver_crm_products.sql     # Cleaned CRM products
│   │   ├── silver_crm_sales.sql        # Cleaned CRM sales (incremental)
│   │   ├── silver_erp_customer_demographics.sql # Cleaned ERP demographics
│   │   ├── silver_erp_locations.sql    # Cleaned ERP locations
│   │   └── silver_erp_product_categories.sql # Cleaned ERP categories
│   │
│   ├── gold/                           # Business-ready dimensional models
│       ├── _gold__models.yml           # Gold model documentation
│       ├── dim_customers.sql           # Customer dimension
│       ├── dim_products.sql            # Product dimension
│       ├── dim_date.sql                # Date dimension
│       └── fact_sales.sql              # Sales fact table
│   
│  
│
├── macros/                             # Reusable SQL macros
│   ├── data_transformations.sql        # Data cleaning macros
│   └── audit_helpers.sql               # Logging and audit macros
│
├── tests/                              # Custom data quality tests
│   └── generic/                        # Generic test definitions
│       └── test_positive_values.sql    # Custom validation tests
│
├── seeds/                              # Reference data
│   ├── country_mappings.csv            # Country standardization
│   └── product_category_hierarchy.csv  # Product taxonomy
│
├── snapshots/                          # Type 2 SCD implementations
│   └── customer_snapshot.sql           # Customer history tracking
│
├── analyses/                           # Ad-hoc analysis queries
│   ├── customer_cohort_analysis.sql    # Customer segmentation
│   └── revenue_trend_analysis.sql      # Revenue pattern analysis
│
└── docs/                               # Additional documentation
    ├── business_glossary.md             # Business term definitions
    └── data_dictionary.md              # Technical data dictionary
```

## 🗃️ Data Models

### Source Systems

| Source | System | Tables | Description |
|--------|--------|--------|-------------|
| `crm_raw` | CRM | `cust_info`, `prd_info`, `sales_details` | Customer master, product catalog, sales transactions |
| `erp_raw` | ERP | `cust_az12`, `loc_a101`, `px_cat_g1v2` | Demographics, locations, product categories |

### Gold Layer - Star Schema

#### 📊 Fact Table
- **`fact_sales`**: Central fact table containing sales transactions
  - **Grain**: One row per sales order line item
  - **Measures**: Sales amount, quantity, unit price, total revenue
  - **Dimensions**: Customer, Product, Order Date, Ship Date, Due Date

#### 🎭 Dimension Tables
- **`dim_customers`**: Unified customer dimension (CRM + ERP)
  - **Attributes**: Demographics, geography, customer segments
  - **SCD Type**: Type 1 (current state) with Type 2 via snapshots
  
- **`dim_products`**: Complete product catalog with categories
  - **Attributes**: Product hierarchy, cost categories, lifecycle status
  - **Business Logic**: Product grouping, maintenance requirements
  
- **`dim_date`**: Comprehensive date dimension
  - **Range**: 2010-2025 (configurable via variables)
  - **Attributes**: Business calendar, relative date flags, fiscal periods

## 🚀 Getting Started

### Prerequisites

- PostgreSQL 13+
- Python 3.8+
- DBT Core 1.6+

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd sql_data_model/crm_data_model
   ```

2. **Install DBT and dependencies**
   ```bash
   pip install dbt-postgres
   dbt deps  # Install package dependencies
   ```

3. **Configure database connection**
   ```bash
   cp profiles.yml.example ~/.dbt/profiles.yml
   # Edit profiles.yml with your database credentials
   ```

4. **Test connection**
   ```bash
   dbt debug
   ```

5. **Load reference data**
   ```bash
   dbt seed
   ```

6. **Run initial build**
   ```bash
   dbt run
   dbt test
   ```

### Database Connection Profile

Create `~/.dbt/profiles.yml`:

```yaml
sql_data_model:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: your_username
      password: your_password
      port: 5432
      dbname: your_database
      schema: dbt_dev
      threads: 4
      keepalives_idle: 0
    
    prod:
      type: postgres
      host: your_prod_host
      user: your_prod_username
      password: your_prod_password
      port: 5432
      dbname: your_prod_database
      schema: dbt_prod
      threads: 8
      keepalives_idle: 0
```

## 🔄 Development Workflow

### Daily Development

```bash
# 1. Pull latest changes
git pull origin main

# 2. Create feature branch
git checkout -b feature/your-feature-name

# 3. Develop and test changes
dbt run --select +your_model+
dbt test --select +your_model+

# 4. Generate documentation
dbt docs generate
dbt docs serve

# 5. Commit and push
git add .
git commit -m "feat: add your feature description"
git push origin feature/your-feature-name
```

### Model Development Commands

```bash
# Run specific models
dbt run --select dim_customers
dbt run --select +fact_sales+  # Include upstream/downstream

# Run by layer
dbt run --select bronze
dbt run --select silver
dbt run --select gold

# Test specific models
dbt test --select dim_customers
dbt test --store-failures  # Store test failures for debugging

# Fresh rebuild
dbt run --full-refresh
```

### Incremental Model Refresh

```bash
# Refresh incremental models
dbt run --select silver_crm_sales --full-refresh
dbt run --select bronze_crm_sales_details --full-refresh
```

## 🧪 Data Quality & Testing

### Testing Framework

The project implements comprehensive data quality testing:

#### Test Types

| Test Type | Purpose | Example |
|-----------|---------|---------|
| **Schema Tests** | Basic data validation | `not_null`, `unique`, `accepted_values` |
| **Generic Tests** | Custom business rules | `positive_values`, `date_range_check` |
| **Singular Tests** | Complex validations | Revenue reconciliation, data consistency |
| **Relationship Tests** | Referential integrity | Foreign key validation |

#### Test Coverage

- **Bronze Layer**: 15+ basic validation tests
- **Silver Layer**: 45+ comprehensive data quality tests
- **Gold Layer**: 30+ business rule validations
- **Marts Layer**: 20+ KPI validation tests

### Running Tests

```bash
# Run all tests
dbt test

# Run tests by layer
dbt test --select bronze
dbt test --select silver
dbt test --select gold

# Store failures for investigation
dbt test --store-failures
```

### Data Quality Monitoring

Built-in data quality features:
- ✅ Automated null rate monitoring
- ✅ Duplicate detection and handling
- ✅ Business rule validation
- ✅ Cross-system data consistency checks
- ✅ Audit logging and lineage tracking

## 🚢 Deployment

### Environment Strategy

| Environment | Purpose | Refresh Frequency | Data Volume |
|-------------|---------|------------------|-------------|
| **Development** | Feature development | On-demand | Sample data |
| **Staging** | Integration testing | Daily | Full data |
| **Production** | Business operations | Hourly | Full data |

### Production Deployment

1. **Continuous Integration**
   ```yaml
   # .github/workflows/dbt-ci.yml
   - name: Run DBT
     run: |
       dbt deps
       dbt run
       dbt test
   ```

2. **Production Refresh**
   ```bash
   # Daily production refresh
   dbt deps
   dbt seed --full-refresh
   dbt run
   dbt test
   dbt docs generate
   ```

3. **Monitoring**
   - DBT Cloud monitoring
   - Custom audit logging
   - Data freshness alerts

## 📊 Business Intelligence

### Supported BI Tools

The star schema design supports various BI tools:

- **Tableau**: Native support for dimensional models
- **Power BI**: Direct query and import modes
- **Looker**: LookML semantic layer integration
- **Metabase**: Self-service analytics
- **SQL Clients**: Direct database access

### Key Performance Indicators

| KPI Category | Metrics Available |
|--------------|------------------|
| **Sales Performance** | Revenue, growth rates, seasonal trends |
| **Customer Analytics** | Acquisition, retention, lifetime value |
| **Product Performance** | Category analysis, inventory turnover |
| **Operational** | Order fulfillment, delivery performance |


## 🤝 Contributing

### Development Guidelines

1. **Naming Conventions**
   - Models: `layer_source_entity.sql` (e.g., `silver_crm_customers.sql`)
   - Tests: `test_business_rule.sql`
   - Macros: `action_entity.sql` (e.g., `standardize_gender.sql`)

2. **Documentation Standards**
   - All models must have descriptions
   - Key columns must be documented
   - Business logic must be explained in comments

3. **Code Quality**
   - Use consistent formatting (SQLFluff)
   - Follow DBT best practices
   - Include comprehensive testing


## 📚 Additional Resources 
Here are some resources I used when doing the project

### Documentation Links

- [DBT Documentation](https://docs.getdbt.com/)
- [Dimensional Modeling Guide](https://www.kimballgroup.com/)
- [SQL Style Guide](https://www.sqlstyle.guide/)
- [Data with Baraa](https://www.youtube.com/watch?v=9GVqKuTVANE&t=15637s): got inspired from this tutorial

### Project Documentation

- **Business Glossary**: `/docs/business_glossary.md`
- **Data Dictionary**: `/docs/data_dictionary.md`
- **Generated Docs**: Run `dbt docs serve` for interactive documentation

### Training Materials

- [DBT Fundamentals](https://courses.getdbt.com/)
- [Dimensional Modeling](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/)
- [Data Warehouse Concepts](https://www.datawarehouse4u.info/)


### Common Issues

| Issue | Solution |
|-------|----------|
| Connection errors | Check `profiles.yml` configuration: sometimes, I messed up with the project name folder and dbt folder |
| Test failures | Review test logs and data quality |
| Performance issues | Consider incremental materialization |
| Model dependencies | Check DBT lineage graph |

### Troubleshooting Commands

```bash
# Debug connection issues
dbt debug

# Check model dependencies
dbt deps --dry-run

# Validate model compilation
dbt compile

# View compiled SQL
cat target/compiled/dbt_data_model/models/...
```

---

## 🏆 Acknowledgments

- **DBT Labs** for the excellent transformation framework
- **Kimball Group** for dimensional modeling methodology

---

**Built with ❤️ using DBT and PostgreSQL**

*Iam leanring and will make mistakes: so any feedback is appreciated.*
