# Frontier Analytics — dbt Project

## Overview
A dbt transformation project built on top of a Python ETL pipeline that extracts data from SAP HANA B1 (Sage CRM) and Sage CRM into a centralised PostgreSQL database.

## Architecture
```
SAP HANA B1 + Sage CRM (source systems)
        ↓
Python ETL Pipeline (scheduled nightly)
        ↓
PostgreSQL — raw tables (SAP, CRM, Excel_Data schemas)
        ↓
dbt Transformation Layer
        ↓
BI Tools (Metabase, Zoho Analytics)
```

## Project Structure
- **Staging** (8 models) — clean and rename raw source tables
- **Intermediate** (4 models) — apply business logic, joins, filters
- **Marts** (2 models) — final reporting tables for BI consumption

## Models

### Staging
| Model | Source | Description |
|-------|--------|-------------|
| stg_sap__orders | SAP.ORDR | Sales order headers |
| stg_sap__order_lines | SAP.RDR1 | Sales order line items |
| stg_sap__profit_centres | SAP.OPRC | Profit centres (Blocks + KAMs) |
| stg_sap__margin_sharing | SAP.@F_MARGINSHARING | Margin sharing entries |
| stg_crm__opportunities | CRM.Opportunity | Pipeline opportunities |
| stg_crm__users | CRM.Users | Sales users with employee ID extraction |
| stg_crm__companies | CRM.Company | Account/company records |
| stg_crm__custom_captions | CRM.Custom_Captions | Lookup values (SBU, OEM, Block) |

### Intermediate
| Model | Description |
|-------|-------------|
| int_sap__orders_not_sharing | Standard orders with filters and employee ID cleaning |
| int_sap__orders_sharing | Margin sharing orders from @F_MARGINSHARING |
| int_sap__orders_combined | UNION ALL of both streams + Block/KAM enrichment |
| int_crm__opportunities_expanded | Multi-SBU/OEM array unnesting via CROSS JOIN LATERAL |

### Marts
| Model | Rows | Description |
|-------|------|-------------|
| mart_sap__sales_orders | ~2,000 | Current FY sales orders with full enrichment |
| mart_crm__pipeline | ~6,000 | Current FY pipeline opportunities by SBU/OEM |

## Key Business Logic
- Excludes cancelled orders (CANCELED = N)
- Excludes zero margin orders
- Excludes IS subcategory orders
- Excludes POWER SERVICE / SPWR SBU
- Splits orders into Sharing vs Not Sharing streams
- Applies Indian financial year logic (Apr-Mar)
- Cleans employee IDs (strips E000 prefix)
- Dynamically filters to current financial year

## Tech Stack
- **Transformation:** dbt (dbt-postgres 1.10)
- **Database:** PostgreSQL
- **Sources:** SAP HANA B1, Sage CRM
- **BI Tools:** Metabase, Zoho Analytics

## Running the Project
```bash
# Activate virtual environment
source ~/.dbt-env/bin/activate

# Run all models
dbt run

# Run tests
dbt test

# Generate and serve docs
dbt docs generate
dbt docs serve --host 0.0.0.0 --port 8080
```
