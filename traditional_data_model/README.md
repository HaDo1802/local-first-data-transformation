# Traditional Data Model Implementation

This folder contains the **original SQL-based approach** for implementing the data warehouse using traditional stored procedures and SQL scripts.

## 📁 Folder Structure

```
traditional_data_model/
├── scripts/                    # SQL stored procedures and ETL scripts
│   ├── bronze/                # Raw data ingestion procedures
│   ├── silver/                # Data cleansing and transformation procedures  
│   └── gold/                  # Business logic and dimensional modeling procedures
├── datasets/                  # Source data files
│   ├── source_crm/           # CRM system data
│   └── source_erp/           # ERP system data
├── docs/                     # Documentation and diagrams
├── tests/                    # Data quality validation scripts
└── Architecture_Design/      # System architecture diagrams and designs
```

## 🏗️ Architecture Approach

This implementation follows the **Medallion Architecture** pattern using traditional SQL:

### Bronze Layer (Raw Data)
- **Location**: `scripts/bronze/`
- **Purpose**: Direct ingestion of source data with minimal transformation
- **Files**: 
  - `bronze_crm_*.sql` - CRM data ingestion procedures
  - `bronze_erp_*.sql` - ERP data ingestion procedures

### Silver Layer (Cleansed Data)  
- **Location**: `scripts/silver/`
- **Purpose**: Data quality checks, standardization, and cleansing
- **Files**:
  - `silver_crm_*.sql` - CRM data cleansing procedures
  - `silver_erp_*.sql` - ERP data cleansing procedures

### Gold Layer (Business Ready)
- **Location**: `scripts/gold/`
- **Purpose**: Dimensional modeling and business logic implementation
- **Files**:
  - `gold_dim_*.sql` - Dimension table procedures
  - `gold_fact_*.sql` - Fact table procedures

## 🔄 ETL Process

The traditional approach uses **stored procedures** executed in sequence:

1. **Bronze Layer**: Execute all bronze procedures to ingest raw data
2. **Silver Layer**: Execute silver procedures for data cleansing
3. **Gold Layer**: Execute gold procedures for dimensional modeling
4. **Quality Checks**: Run validation scripts from `tests/` folder

## 📊 Data Sources

### CRM System
- **cust_info**: Customer information
- **prd_info**: Product catalog
- **sales_details**: Sales transactions

### ERP System  
- **cust_az12**: Customer demographics
- **loc_a101**: Location data
- **px_cat_g1v2**: Product categorization

## 🔧 Deployment

### Prerequisites
- PostgreSQL 13+
- Database admin privileges
- Source data files in CSV format

### Execution Steps
1. Load source data into staging tables
2. Execute bronze layer procedures
3. Execute silver layer procedures  
4. Execute gold layer procedures
5. Run data quality tests

### Example Execution
```sql
-- Bronze Layer
CALL bronze_crm_cust_info();
CALL bronze_crm_prd_info();
CALL bronze_crm_sales_details();

-- Silver Layer  
CALL silver_crm_customer_master();
CALL silver_crm_product_catalog();
CALL silver_crm_sales_fact();

-- Gold Layer
CALL gold_dim_customers();
CALL gold_dim_products(); 
CALL gold_fact_sales();
```

## 📈 Comparison with DBT Implementation

| Aspect | Traditional SQL | DBT Implementation |
|--------|----------------|-------------------|
| **Code Organization** | Stored procedures | Modular SQL models |
| **Version Control** | Manual | Git-native |
| **Testing** | Manual scripts | Built-in testing framework |
| **Documentation** | External docs | Self-documenting |
| **Dependency Management** | Manual execution order | Automatic dependency resolution |
| **Incremental Processing** | Custom logic | Built-in incremental models |
| **Data Lineage** | Manual documentation | Automatic lineage tracking |
| **Environment Management** | Manual deployment | Profile-based environments |

## 🎯 When to Use This Approach

The traditional SQL approach is suitable when:

- Working with legacy systems that require stored procedures
- Database-first organizations with strong SQL expertise
- Environments with restrictions on external tools
- Need for complex, database-specific optimizations
- Integration with existing stored procedure frameworks

## 🔍 Migration Notes

This implementation serves as the **baseline** for comparison with the modern DBT approach. The business logic and transformations are equivalent, but the implementation methodology differs significantly.

For the **modern, recommended approach**, see the `../dbt_data_model/` folder which provides:
- Better maintainability
- Version control integration  
- Automated testing
- Self-documenting code
- Environment management
- Data lineage tracking

---

*This traditional implementation demonstrates the evolution from procedural SQL to modern data engineering practices using DBT.*