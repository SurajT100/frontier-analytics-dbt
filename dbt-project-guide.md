# Complete dbt Project Guide — Frontier Analytics

## Table of Contents
1. [What We Built](#what-we-built)
2. [Why dbt Over SQL Views](#why-dbt-over-sql-views)
3. [Prerequisites Setup](#prerequisites-setup)
4. [Project Architecture](#project-architecture)
5. [Step-by-Step Build Process](#step-by-step-build-process)
6. [Testing & Documentation](#testing--documentation)
7. [Interview Talking Points](#interview-talking-points)

---

## What We Built

A production-ready dbt transformation project that sits on top of a Python ETL pipeline extracting from SAP HANA B1 and Sage CRM into PostgreSQL.

**Final Output:**
- 14 dbt models (8 staging, 4 intermediate, 2 marts)
- 8 data tests
- Auto-generated documentation with lineage graph
- GitHub repository: https://github.com/SurajT100/frontier-analytics-dbt

**Data Flow:**
```
SAP HANA B1 + Sage CRM (source systems)
        ↓
Python ETL Pipeline (scheduled nightly)
        ↓
PostgreSQL — raw tables (SAP, CRM, Excel_Data schemas)
        ↓
dbt Transformation Layer (14 models)
        ↓
BI Tools (Metabase, Zoho Analytics)
```

---

## Why dbt Over SQL Views

### Your Original Approach
You had 2 PostgreSQL views:
1. **SAP view** — complex UNION ALL joining ORDR, RDR1, @F_MARGINSHARING with filters
2. **CRM view** — array unnesting with CROSS JOIN LATERAL for multi-SBU opportunities

These views worked. Metabase and Zoho were connected. CEO was happy.

### What dbt Adds (The Honest Truth)

For your **current** setup at Frontier with just you as the analyst, dbt doesn't add new business value immediately. Your views were sufficient.

**But here's where dbt shows value:**

#### 1. **At Scale (Multiple Analysts)**
Imagine joining a company with 200 SQL views written by 5 analysts over 3 years:
- **Without dbt:** No one knows what views depend on what. No descriptions. Cryptic column names. Duplicate logic everywhere.
- **With dbt:** Every model has a description. Every column is documented. Lineage graph shows all dependencies instantly. Shared logic lives in one place.

#### 2. **Data Quality Assurance**
- **Your views:** If someone inserts bad data (`CANCELED = NULL`), your view keeps running but returns wrong results silently. You find out when CEO reports wrong numbers.
- **With dbt:** Tests catch this immediately. `not_null` test on `is_cancelled` fails the pipeline before bad data reaches dashboards.

Example test that would catch this:
```yaml
columns:
  - name: is_cancelled
    data_tests:
      - not_null
      - accepted_values:
          values: ['Y', 'N']
```

#### 3. **Change Impact Analysis**
- **Your views:** If SAP renames a column in ORDR, which of your 50 views break? You have to manually trace it.
- **With dbt:** Lineage graph shows every downstream model immediately. Click on `ORDR` source and see 8 models that reference it.

#### 4. **No Duplicate Logic**
Your employee ID cleaning logic (`E000` stripping) appears in **both** your SAP and CRM views — copy-pasted code.

**In dbt:** Write it once in an intermediate model, both marts `ref()` it. Change once, updates everywhere.

#### 5. **Version Control & Collaboration**
- **Your views:** Stored in PostgreSQL. Changes are irreversible. No history. If you break something, you can't roll back.
- **With dbt:** Every model is a `.sql` file in Git. Full change history. Pull requests. Code review. Rollback to any previous version.

#### 6. **Separation of Concerns**
Your current SAP view does everything in one query:
- Cleans columns (staging work)
- Applies filters (intermediate work)
- Joins lookup tables (intermediate work)
- Aggregates final output (mart work)

**With dbt's 3-layer architecture:**
- Staging: Just clean and rename
- Intermediate: Apply business logic
- Marts: Final aggregations

This makes debugging and maintenance much easier. If the SBU mapping breaks, you know it's in the intermediate layer, not mixed in with staging logic.

### When Does dbt Make Sense?

**Use dbt when:**
- Team has 3+ analysts/engineers
- More than 20 transformation models
- Need automated testing and monitoring
- Models reference each other frequently
- Working on Snowflake, BigQuery, Redshift, or Databricks

**Your views are fine when:**
- Solo analyst
- Small number of views (< 10)
- Ad-hoc analysis
- Simple transformations

**For your job search:** Every modern Analytics Engineering and Data Engineering role expects dbt. Learning it makes you hireable, even if you never needed it at Frontier.

---

## Prerequisites Setup

### 1. Environment
- **OS:** Windows with WSL (Ubuntu 24.04)
- **Database:** PostgreSQL with remote access to Frontier's server (10.0.9.25)
- **Database Name:** FBS_DB
- **Schemas:** SAP, CRM, Excel_Data

### 2. Install dbt in WSL

```bash
# Open WSL
wsl

# Update system packages
sudo apt update

# Install Python and pip
sudo apt install python3-pip python3-venv -y

# Create virtual environment
python3 -m venv ~/.dbt-env

# Activate virtual environment (do this every new session)
source ~/.dbt-env/bin/activate

# Install dbt with PostgreSQL adapter
pip install dbt-postgres

# Verify installation
dbt --version
```

**Expected output:**
```
Core:
  - installed: 1.11.5
Plugins:
  - postgres: 1.10.0
```

### 3. VS Code with WSL Extension (Optional but Recommended)

In VS Code:
- Install **Remote - WSL** extension (by Microsoft)
- From WSL terminal run: `code .` to open project in VS Code

This lets you edit SQL files in a proper editor instead of nano.

---

## Project Architecture

### Core Concepts

#### 1. Sources vs Models
- **Sources** = Raw tables your ETL loads (SAP.ORDR, CRM.Opportunity)
- **Models** = SQL transformations dbt runs (stg_sap__orders, mart_sap__sales_orders)

#### 2. Jinja Templating
dbt uses Jinja (`{{ }}`) for dynamic SQL:

```sql
-- Instead of hardcoding schema names:
FROM "SAP"."ORDR"

-- Use source() function:
FROM {{ source('sap', 'ORDR') }}

-- Instead of hardcoding table names:
FROM dbt_dev_staging.stg_sap__orders

-- Use ref() function:
FROM {{ ref('stg_sap__orders') }}
```

**Why?** dbt builds a dependency graph from `ref()` and `source()` calls. This creates the lineage graph and determines run order.

#### 3. Materialization
dbt can create your models as:
- **View** — fast to build, slower to query (default for staging/intermediate)
- **Table** — slower to build, fast to query (used for marts)
- **Incremental** — only adds new data (advanced, not covered)

Configured in `dbt_project.yml`:
```yaml
models:
  frontier_analytics:
    staging:
      +materialized: view
    intermediate:
      +materialized: view
    marts:
      +materialized: table
```

#### 4. Three-Layer Architecture

```
┌─────────────────────────────────────────────────────────┐
│ STAGING LAYER                                           │
│ Purpose: Clean and rename source columns                │
│ Rules:                                                  │
│   - One model per source table                          │
│   - No business logic, no filtering                     │
│   - Just rename cryptic names to readable snake_case    │
│   - Materialized as views                               │
└─────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────┐
│ INTERMEDIATE LAYER                                      │
│ Purpose: Apply business logic                           │
│ Rules:                                                  │
│   - Joins across staging models                         │
│   - Filtering (exclude cancelled, zero margin, etc.)    │
│   - Calculations and transformations                    │
│   - Complex logic isolated here                         │
│   - Materialized as views                               │
└─────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────┐
│ MART LAYER                                              │
│ Purpose: Final reporting tables                         │
│ Rules:                                                  │
│   - Wide, denormalized tables                           │
│   - Ready for BI tools (Metabase, Zoho)                │
│   - Lookup joins for human-readable names               │
│   - Materialized as tables (physical storage)           │
└─────────────────────────────────────────────────────────┘
```

### Project Structure

```
frontier_analytics/
├── dbt_project.yml           # Project configuration
├── profiles.yml              # Database connection (in ~/.dbt/)
├── models/
│   ├── staging/
│   │   ├── sap/
│   │   │   ├── schema.yml              # Source declarations
│   │   │   ├── stg_sap__orders.sql
│   │   │   ├── stg_sap__order_lines.sql
│   │   │   ├── stg_sap__profit_centres.sql
│   │   │   └── stg_sap__margin_sharing.sql
│   │   ├── crm/
│   │   │   ├── schema.yml
│   │   │   ├── stg_crm__opportunities.sql
│   │   │   ├── stg_crm__users.sql
│   │   │   ├── stg_crm__companies.sql
│   │   │   └── stg_crm__custom_captions.sql
│   │   └── excel_data/
│   │       └── schema.yml
│   ├── intermediate/
│   │   ├── int_sap__orders_not_sharing.sql
│   │   ├── int_sap__orders_sharing.sql
│   │   ├── int_sap__orders_combined.sql
│   │   └── int_crm__opportunities_expanded.sql
│   └── marts/
│       ├── schema.yml                  # Model descriptions + tests
│       ├── mart_sap__sales_orders.sql
│       └── mart_crm__pipeline.sql
├── macros/
│   └── generate_schema_name.sql        # Custom schema naming logic
├── tests/                              # Custom SQL tests (empty for now)
├── seeds/                              # CSV files to load (not used)
└── README.md
```

---

## Step-by-Step Build Process

### Step 1: Initialize dbt Project

```bash
cd ~
dbt init frontier_analytics
```

**Questions asked:**
```
Which database? → postgres
host → 10.0.9.25
port → 5432
user → postgres
pass → [your password]
dbname → FBS_DB
schema → dbt_dev
threads → 4
```

**What this creates:**
- Project folder: `~/frontier_analytics/`
- Connection profile: `~/.dbt/profiles.yml` (stored separately for security)
- Sample models in `models/example/` (we delete these)

**Navigate into project:**
```bash
cd frontier_analytics
```

---

### Step 2: Configure Project Settings

**Edit `dbt_project.yml`:**

```yaml
name: 'frontier_analytics'
version: '1.0.0'
profile: 'frontier_analytics'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

clean-targets:
  - "target"
  - "dbt_packages"

models:
  frontier_analytics:
    staging:
      +materialized: view
      +schema: staging
    intermediate:
      +materialized: view
      +schema: intermediate
    marts:
      +materialized: table
      +schema: marts
```

**What each setting means:**
- `+materialized: view` — create as PostgreSQL view (fast build, slower query)
- `+materialized: table` — create as PostgreSQL table (slower build, fast query)
- `+schema: staging` — models go into `dbt_dev_staging` schema (dbt prepends the target schema automatically)

**Test connection:**
```bash
dbt debug
```

**Expected output:**
```
All checks passed!
```

---

### Step 3: Create Folder Structure

```bash
mkdir -p models/staging/sap
mkdir -p models/staging/crm
mkdir -p models/staging/excel_data
mkdir -p models/intermediate
mkdir -p models/marts
rm -rf models/example  # Delete sample models
```

**Verify structure:**
```bash
find models -type d
```

---

### Step 4: Declare Sources

Sources tell dbt which raw tables exist that your ETL loads.

**Create `models/staging/sap/schema.yml`:**

```yaml
version: 2

sources:
  - name: sap
    schema: SAP
    quoting:
      schema: true
      identifier: true
    tables:
      - name: ORDR
        description: "SAP Sales Order headers"
      - name: RDR1
        description: "SAP Sales Order line items"
      - name: OPRC
        description: "SAP Profit Centres - used for both Blocks and KAM mapping"
      - name: OITM
        description: "SAP Item master data"
      - name: "@F_MARGINSHARING"
        description: "SAP Margin sharing entries for shared orders"
```

**Why `quoting`?**
PostgreSQL's case-sensitivity issue:
- Without quotes: `ORDR` → postgres treats it as `ordr` (lowercased)
- With quotes: `"ORDR"` → postgres treats it as exact case

Your tables are stored in uppercase, so we need quoting enabled.

**Create `models/staging/crm/schema.yml`:**

```yaml
version: 2

sources:
  - name: crm
    schema: CRM
    quoting:
      schema: true
      identifier: true
    tables:
      - name: Opportunity
        description: "CRM Opportunity pipeline records"
      - name: Users
        description: "CRM system users and sales reps"
      - name: Company
        description: "CRM company/account records"
      - name: Custom_Captions
        description: "CRM lookup values for SBU, OEM, Block, Vertical etc"
      - name: Lead
        description: "CRM lead records linked to opportunities"
```

**Create `models/staging/excel_data/schema.yml`:**

```yaml
version: 2

sources:
  - name: excel_data
    schema: Excel_Data
    quoting:
      schema: true
      identifier: true
    tables:
      - name: SAP_Account_Type_Mapping
        description: "Maps SAP customer groups to account types"
      - name: SAP_Block_Master_Table
        description: "Maps SAP block codes to final block names"
      - name: SBU_Target
        description: "Target data with Block, Region, Block Head mapping"
      - name: SBU_Name_Master
        description: "Standardizes SBU names from SAP codes"
      - name: CRM_Account_Type_Mapping
        description: "Maps CRM opportunity segments to account types"
      - name: KAM_Target
        description: "KAM-level targets by block"
```

**Test sources are recognized:**
```bash
dbt parse
```

Expected: No errors.

---

### Step 5: Fix Cross-Database Reference Issue

**Problem encountered:**
```
Database Error: cross-database references are not implemented: "fbs_db.SAP.ORDR"
```

PostgreSQL doesn't support `database.schema.table` syntax. You're always connected to one database, so you only use `schema.table`.

**Fix 1 — Remove database from profiles:**

The `dbname` in `~/.dbt/profiles.yml` is for connection only, not for query prefixes.

**Fix 2 — Create macro to override database naming:**

Create `macros/generate_schema_name.sql`:

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

{% macro generate_database_name(custom_database_name, node) -%}
    {{ target.database }}
{%- endmacro %}
```

**What this does:**
- `generate_schema_name` — creates schema names like `dbt_dev_staging`, `dbt_dev_marts`
- `generate_database_name` — always returns just the target database, never prefixes it

**Fix 3 — Update profiles.yml to include search_path:**

```yaml
frontier_analytics:
  outputs:
    dev:
      type: postgres
      host: 10.0.9.25
      port: 5432
      user: postgres
      pass: Frontier@123
      dbname: FBS_DB
      schema: dbt_dev
      threads: 4
      search_path: "SAP,CRM,Excel_Data,public"
  target: dev
```

`search_path` tells PostgreSQL which schemas to look in without explicit qualification.

---

### Step 6: Build Staging Layer

Staging models follow a consistent two-CTE pattern:

```sql
with source as (
    select * from {{ source('schema_name', 'TABLE_NAME') }}
),

renamed as (
    select
        "OldUglyName" as new_readable_name,
        "AnotherColumn" as another_column
    from source
)

select * from renamed
```

**Why this pattern?**
- `source` CTE — pulls from raw table via `source()` (creates lineage connection)
- `renamed` CTE — cleans column names (cryptic SAP names → readable snake_case)
- No business logic here — that's for intermediate layer

**Example: `models/staging/sap/stg_sap__orders.sql`**

```sql
with source as (

    select * from {{ source('sap', 'ORDR') }}

),

renamed as (

    select
        "DocEntry"          as order_id,
        "DocNum"            as order_number,
        "DocDate"           as order_date,
        "CardName"          as customer_name,
        "CANCELED"          as is_cancelled,
        "U_Marginpl"::numeric as margin,
        "U_SBU"             as sbu_code,
        "U_Ordertype"       as order_type,
        "U_AVA_MARGINTYPE"  as margin_type,
        "U_SalesExecutive"  as sales_executive_code,
        "U_AVA_Block"       as block_code,
        "U_Cgroup"          as customer_group_code,
        "U_Ldcst"           as ld_cost,
        "Comments"          as comments

    from source

)

select * from renamed
```

**Create all 8 staging models:**

1. `stg_sap__orders.sql` (shown above)
2. `stg_sap__order_lines.sql` — from RDR1
3. `stg_sap__profit_centres.sql` — from OPRC (used for both Blocks and KAMs)
4. `stg_sap__margin_sharing.sql` — from @F_MARGINSHARING
5. `stg_crm__opportunities.sql` — from Opportunity
6. `stg_crm__users.sql` — from Users (includes employee ID extraction: `regexp_replace("User_Logon"::text, '[^0-9]', '', 'g')`)
7. `stg_crm__companies.sql` — from Company
8. `stg_crm__custom_captions.sql` — from Custom_Captions

**Run all staging models:**
```bash
dbt run --select staging
```

**Expected output:**
```
Completed successfully
Done. PASS=8 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=8
```

**Verify in database:**
```bash
psql -h 10.0.9.25 -U postgres -d FBS_DB -c "\dv dbt_dev_staging.*"
```

You should see all 8 views created in the `dbt_dev_staging` schema.

---

### Step 7: Build Intermediate Layer

Intermediate models contain business logic. They reference staging models via `ref()`.

**Example: `models/intermediate/int_sap__orders_not_sharing.sql`**

This rebuilds the first half of your original SAP view — the "Not Sharing" orders from ORDR directly.

```sql
with orders as (

    select * from {{ ref('stg_sap__orders') }}

),

order_lines as (

    select * from {{ ref('stg_sap__order_lines') }}

),

profit_centres as (

    select * from {{ ref('stg_sap__profit_centres') }}

),

-- Join orders to lines, take first line only (rn = 1)
-- This matches your original view logic
orders_with_lines as (

    select
        o.order_id,
        o.order_number,
        o.order_date,
        o.customer_name,
        o.margin,
        o.sbu_code,
        o.order_type,
        o.margin_type,
        o.sales_executive_code,
        o.block_code,
        o.customer_group_code,
        o.ld_cost,
        o.comments,
        lower(l.subcategory) as subcategory,
        'Not Sharing' as sharing_type,
        row_number() over (partition by l.order_id) as rn

    from orders o
    join order_lines l on l.order_id = o.order_id

    -- Your original business filters
    where o.margin <> 0
      and o.is_cancelled = 'N'
      and o.margin_sharing_type = 'NS'
      and (lower(l.subcategory) not like '%is%' or l.subcategory is null)
      and (o.sbu_code is null or o.sbu_code not in ('POWER SERVICE', 'SPWR'))

),

-- Clean employee ID - your original CASE logic
employee_id_cleaned as (

    select
        order_id,
        order_number,
        order_date,
        customer_name,
        margin,
        sbu_code,
        order_type,
        margin_type,
        block_code,
        customer_group_code,
        ld_cost,
        comments,
        subcategory,
        sharing_type,
        rn,
        case
            when left(replace(sales_executive_code, 'E000', ''), 1) = '0'
                then right(replace(sales_executive_code, 'E000', ''), 3)
            when sales_executive_code = 'DIR00001'
                then '1'
            else replace(sales_executive_code, 'E000', '')
        end as employee_id,
        sales_executive_code as kam_profit_centre_code

    from orders_with_lines
    where rn = 1

)

select * from employee_id_cleaned
```

**Key differences from your original view:**
- Uses `ref()` instead of hardcoded schema.table names
- Broken into named CTEs for readability
- Business logic isolated (filters, row_number, employee ID cleaning)
- No lookup joins yet — those happen in the combined layer

**Create all 4 intermediate models:**

1. `int_sap__orders_not_sharing.sql` (shown above)
2. `int_sap__orders_sharing.sql` — rebuilds second half of your view from @F_MARGINSHARING
3. `int_sap__orders_combined.sql` — UNION ALL of both + Block/KAM/Region lookups
4. `int_crm__opportunities_expanded.sql` — your CROSS JOIN LATERAL array unnesting logic

**Run intermediate layer:**
```bash
dbt run --select intermediate
```

**Expected output:**
```
Done. PASS=4 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=4
```

---

### Step 8: Build Mart Layer

Marts are the final reporting tables. They reference intermediate models via `ref()` and pull in lookup tables via `source()` for human-readable names.

**Example: `models/marts/mart_sap__sales_orders.sql`**

```sql
with orders as (

    select * from {{ ref('int_sap__orders_combined') }}

),

sbu_mapping as (

    select * from {{ source('excel_data', 'SBU_Name_Master') }}

),

account_type as (

    select * from {{ source('excel_data', 'SAP_Account_Type_Mapping') }}

),

sbu_target as (

    select * from {{ source('excel_data', 'SBU_Target') }}

),

-- Apply SBU name standardization and account type mapping
final as (

    select distinct
        o.order_number              as so_number,
        o.comments                  as orn,
        o.order_id,
        o.order_date,
        o.order_month,
        o.margin,
        coalesce(sbu."SBU Name", o.sbu_code) as sbu,
        o.kam_name,
        o.block_name                as block,
        o.order_type,
        o.subcategory,
        o.customer_name,
        o.sharing_type,
        o.financial_quarter,
        o.month_name,
        o.ld_cost,
        o.employee_id,
        at."Final Ac Type"          as account_type,
        st."Region"                 as region

    from orders o

    -- SBU name standardization
    left join sbu_mapping sbu
        on o.sbu_code = sbu."PrcName"

    -- Account type mapping
    left join account_type at
        on o.customer_group_code = at."SAP"

    -- Region from SBU target via block
    left join sbu_target st
        on o.block_name = st."Block"

    -- Current financial year filter
    where o.order_date >= case
        when extract(month from current_date) >= 4
            then (extract(year from current_date)::text || '-04-01')::date
        else ((extract(year from current_date) - 1)::text || '-04-01')::date
    end

    order by o.order_number

)

select * from final
```

**This is the final output table** — column names match your original view, filters applied, enriched with lookup data.

**Materialization:** This is created as a **table** (not view) because `dbt_project.yml` specifies `marts: +materialized: table`. This makes BI queries faster.

**Create both marts:**

1. `mart_sap__sales_orders.sql` (shown above) — ~2,010 rows
2. `mart_crm__pipeline.sql` — CRM opportunities expanded by SBU/OEM — ~6,287 rows

**Run marts:**
```bash
dbt run --select marts
```

**Expected output:**
```
1 of 2 OK created sql table model dbt_dev_marts.mart_crm__pipeline ............. [SELECT 6287 in 0.60s]
2 of 2 OK created sql table model dbt_dev_marts.mart_sap__sales_orders ......... [SELECT 2010 in 1.32s]
```

**Verify in database:**
```bash
psql -h 10.0.9.25 -U postgres -d FBS_DB -c "\dt dbt_dev_marts.*"
```

You should see 2 actual tables (not views) with real data.

---

### Step 9: Run Full Pipeline

```bash
dbt run
```

This runs all 14 models in dependency order:
1. Staging models first (no dependencies)
2. Intermediate models next (depend on staging)
3. Marts last (depend on intermediate)

**Expected output:**
```
Done. PASS=14 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=14
```

---

## Testing & Documentation

### Data Tests

Tests are declared in `schema.yml` files inside model folders. dbt generates and runs the SQL automatically.

**Built-in tests:**
- `not_null` — column cannot be NULL
- `unique` — column must be unique
- `accepted_values` — column must be in a specific list
- `relationships` — foreign key check (not used in our project)

**Create `models/marts/schema.yml`:**

```yaml
version: 2

models:
  - name: mart_sap__sales_orders
    description: "Final SAP sales orders table combining sharing and non-sharing orders. Filtered to current financial year, excluding cancelled orders, zero margin, IS subcategory, and POWER SERVICE SBU."
    columns:
      - name: order_id
        description: "Unique SAP sales order entry ID"
        data_tests:
          - not_null
      
      - name: so_number
        description: "SAP sales order document number"
        data_tests:
          - not_null
      
      - name: margin
        description: "Order margin value in INR"
        data_tests:
          - not_null
      
      - name: sharing_type
        description: "Whether order is Sharing or Not Sharing"
        data_tests:
          - accepted_values:
              values:
                - 'Sharing'
                - 'Not Sharing'
      
      - name: financial_quarter
        description: "Indian financial year quarter (Q1=Apr-Jun, Q2=Jul-Sep, Q3=Oct-Dec, Q4=Jan-Mar)"
        data_tests:
          - accepted_values:
              values:
                - 'Q1'
                - 'Q2'
                - 'Q3'
                - 'Q4'

  - name: mart_crm__pipeline
    description: "CRM pipeline opportunities expanded by SBU and OEM. One row per opportunity-SBU-OEM combination. Filtered to current financial year close dates."
    columns:
      - name: opportunity_id
        description: "Unique CRM opportunity ID"
        data_tests:
          - not_null
      
      - name: stage
        description: "Current opportunity stage in the sales pipeline"
        data_tests:
          - not_null
      
      - name: bottomline
        description: "Bottom line value for this SBU/OEM combination"
        data_tests:
          - not_null
```

**Run tests:**
```bash
dbt test
```

**Expected output:**
```
Done. PASS=8 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=8
```

**What happens behind the scenes:**

For `not_null` test on `order_id`:
```sql
-- dbt generates this SQL automatically
select *
from dbt_dev_marts.mart_sap__sales_orders
where order_id is null
```

If this returns rows, test fails.

For `accepted_values` test on `sharing_type`:
```sql
-- dbt generates this SQL automatically
select *
from dbt_dev_marts.mart_sap__sales_orders
where sharing_type not in ('Sharing', 'Not Sharing')
```

If this returns rows, test fails.

---

### Documentation & Lineage Graph

**Generate documentation:**
```bash
dbt docs generate
```

This creates `target/index.html`, `target/manifest.json`, and `target/catalog.json`.

**Serve documentation site:**
```bash
dbt docs serve --host 0.0.0.0 --port 8080
```

Open browser: `http://127.0.0.1:8080`

**What you'll see:**

1. **Project overview** — list of all models, sources, tests
2. **Model details** — click any model to see:
   - Description
   - Column list with descriptions
   - SQL code
   - Compiled SQL (with `ref()` and `source()` replaced)
   - Test results
3. **Lineage graph** — click the blue circle icon at bottom right of any model page

**Example lineage for `mart_sap__sales_orders`:**

```
SAP.ORDR ──→ stg_sap__orders ──→ int_sap__orders_not_sharing ──┐
                                                                 ├──→ int_sap__orders_combined ──→ mart_sap__sales_orders
SAP.@F_MARGINSHARING ──→ stg_sap__margin_sharing ──→ int_sap__orders_sharing ──┘

SAP.RDR1 ──→ stg_sap__order_lines ──→ (both intermediate models)

SAP.OPRC ──→ stg_sap__profit_centres ──→ int_sap__orders_combined

Excel_Data.SBU_Name_Master ──→ mart_sap__sales_orders
Excel_Data.SAP_Account_Type_Mapping ──→ mart_sap__sales_orders
Excel_Data.SBU_Target ──→ mart_sap__sales_orders
```

This visual graph is auto-generated from your `ref()` and `source()` calls. Zero manual work.

**Screenshot this for your portfolio.**

---

## Interview Talking Points

### "Tell me about your dbt experience"

**Good answer:**

*"I built a production dbt project at Frontier Business Systems that transforms data from SAP HANA B1 and Sage CRM. We have a Python ETL pipeline that loads raw tables into PostgreSQL, and dbt handles the transformation layer on top. The project has 14 models across three layers — 8 staging models for cleaning and renaming, 4 intermediate models for business logic like UNION ALL operations and array unnesting, and 2 mart tables that serve Metabase and Zoho Analytics. I've implemented 8 data tests on key columns to catch data quality issues early, and we use the auto-generated lineage graph for impact analysis when source schemas change. The marts serve real business reporting for our CEO and regional heads, tracking sales pipeline and order metrics."*

### "What's the difference between staging, intermediate, and mart models?"

**Good answer:**

*"Staging models are one-to-one with source tables — they just clean and rename columns, no business logic. Intermediate models apply filters, joins, and transformations — this is where business rules live. Marts are the final wide, denormalized tables ready for BI consumption. We materialize staging and intermediate as views since they're fast to rebuild and rarely queried directly, but marts are materialized as tables for query performance since BI tools hit them constantly."*

### "How do you handle dependencies between models?"

**Good answer:**

*"Every model references upstream dependencies using the `ref()` function instead of hardcoding table names. For example, `FROM {{ ref('stg_sap__orders') }}` instead of `FROM dbt_dev_staging.stg_sap__orders`. dbt builds a dependency graph from these `ref()` calls and runs models in the correct order automatically — staging first, intermediate next, marts last. The same `ref()` calls also power the lineage graph in the documentation."*

### "What tests do you have in place?"

**Good answer:**

*"We have 8 data tests across our mart tables — `not_null` tests on key columns like order_id and opportunity_id to catch missing data, and `accepted_values` tests on categorical fields like sharing_type and financial_quarter to ensure only valid values exist. These tests run after each `dbt run` and fail the pipeline if data quality issues are detected, preventing bad data from reaching our dashboards."*

### "Why dbt instead of just SQL views?"

**Good answer:**

*"We actually had SQL views in place before dbt and they worked. But dbt adds four key things — automated testing so we catch data quality issues before they reach dashboards, version control and documentation so anyone can understand what each model does and how it's built, lineage graphs showing exactly which downstream models are affected when a source table changes, and a proper layered architecture that separates cleaning logic from business logic, making the codebase much easier to maintain as it scales."*

### "What would you do if a model starts failing in production?"

**Good answer:**

*"First I'd check the dbt logs to identify which step failed — is it a source freshness issue, a model build failure, or a test failure? If it's a test failure, I'd query the underlying model directly to see what data violated the test. If it's a build failure, I'd look at the compiled SQL in the `target/` folder to see the exact query dbt tried to run and debug from there. I'd also check if any upstream source schema changed by reviewing the lineage graph to see dependencies. Once fixed, I'd do a targeted `dbt run --select model_name+` to rebuild just that model and its downstream dependencies."*

---

## Common Errors & Solutions

### Error 1: "cross-database references are not implemented"

**Cause:** PostgreSQL doesn't support `database.schema.table` syntax.

**Solution:** Remove `database` parameter from source definitions in `schema.yml` files and create the `generate_database_name` macro to prevent dbt from prefixing database names.

---

### Error 2: "column does not exist"

**Cause:** Column name mismatch between staging and intermediate models, or missing column in SELECT list.

**Solution:** Check the staging model to see exact column names used. In intermediate models, ensure all selected columns from CTEs are carried through to the final SELECT.

Example: If `customer_group_code` is in the `combined` CTE but not in the `enriched` CTE, add it:
```sql
enriched as (
    select
        c.order_id,
        c.order_number,
        c.customer_group_code,  -- Add this line
        ...
    from combined c
)
```

---

### Error 3: "zero-length delimited identifier"

**Cause:** Double-quoting in source definitions — `"ORDR"` becomes `""ORDR""` when dbt adds its own quotes.

**Solution:** Remove quotes from table names in `schema.yml` and use the `quoting` block instead:

```yaml
sources:
  - name: sap
    schema: SAP
    quoting:
      schema: true
      identifier: true
    tables:
      - name: ORDR  # No quotes here
```

---

### Error 4: dbt docs won't load in browser (CORS error)

**Cause:** Chrome blocks local file:// URLs from loading JSON files.

**Solution 1:** Use dbt docs server instead:
```bash
dbt docs serve --host 0.0.0.0 --port 8080
# Open http://127.0.0.1:8080
```

**Solution 2:** Open in Firefox (less strict than Chrome).

---

### Error 5: "Deprecation warning: MissingArgumentsPropertyInGenericTestDeprecation"

**Cause:** Old `accepted_values` test syntax. In dbt 1.11, the format changed slightly.

**Solution:** Use list syntax for values:

```yaml
# Old syntax (deprecated)
data_tests:
  - accepted_values:
      values: ['Q1', 'Q2', 'Q3', 'Q4']

# New syntax (recommended)
data_tests:
  - accepted_values:
      values:
        - 'Q1'
        - 'Q2'
        - 'Q3'
        - 'Q4'
```

---

## Next Steps

### 1. Add More Tests
Add tests to intermediate models:

```yaml
# models/intermediate/schema.yml
version: 2

models:
  - name: int_sap__orders_combined
    columns:
      - name: order_id
        data_tests:
          - unique
          - not_null
      - name: margin
        data_tests:
          - not_null
```

### 2. Add Column-Level Documentation

Expand your schema.yml files to document every column:

```yaml
columns:
  - name: order_id
    description: "Unique sales order entry ID from SAP ORDR.DocEntry"
  - name: order_number
    description: "User-visible document number from SAP ORDR.DocNum"
  - name: margin
    description: "Order margin in INR. Excludes zero margin orders."
```

### 3. Create Custom Tests

For business-specific logic, write custom SQL tests in `tests/` folder:

```sql
-- tests/assert_no_future_orders.sql
select *
from {{ ref('mart_sap__sales_orders') }}
where order_date > current_date
```

If this returns rows, test fails.

### 4. Schedule with Airflow

Orchestrate your full pipeline:
- Extract from SAP (Python)
- Extract from CRM (Python)
- Run dbt models (`dbt run`)
- Run dbt tests (`dbt test`)
- Send success/failure alerts

(This is the next learning topic — covered in a separate session.)

### 5. Add to Your CV

Under Frontier Business Systems:

*"Implemented dbt transformation layer on PostgreSQL, refactoring complex SAP and CRM SQL views into a modular 14-model pipeline across staging, intermediate, and mart layers — with automated data tests and auto-generated lineage documentation supporting CEO-level reporting."*

---

## Key SQL Concepts Used

### 1. CTEs (Common Table Expressions)

```sql
with orders as (
    select * from {{ ref('stg_sap__orders') }}
),

filtered as (
    select * from orders where margin > 0
)

select * from filtered
```

**Why CTEs?**
- Readability — each step has a name
- Debuggability — you can run each CTE independently
- Reusability — reference the same CTE multiple times

---

### 2. Window Functions

```sql
row_number() over (partition by order_id) as rn
```

**What this does:**
Assigns a sequential number to each row within each `order_id` group. First row gets `rn = 1`, second gets `rn = 2`, etc.

**Used in your project to:**
Take only the first line item per order (since your original view used `WHERE rn = 1`).

---

### 3. CASE Statements

```sql
case
    when extract(month from order_date) in (4,5,6) then 'Q1'
    when extract(month from order_date) in (7,8,9) then 'Q2'
    when extract(month from order_date) in (10,11,12) then 'Q3'
    when extract(month from order_date) in (1,2,3) then 'Q4'
end as financial_quarter
```

**What this does:**
Indian financial year mapping — April–June = Q1, etc.

---

### 4. UNION ALL

```sql
select * from not_sharing_orders
union all
select * from sharing_orders
```

**What this does:**
Combines two result sets with identical columns into one. `UNION ALL` keeps duplicates (faster than `UNION` which removes duplicates).

**Used in your project:**
Combining standard orders and margin-sharing orders into one dataset.

---

### 5. CROSS JOIN LATERAL with Array Unnesting

```sql
cross join lateral (
    select unnest(array[0,1,2,3]) as idx
) u
```

**What this does:**
For each row in the base table, generate 4 rows (one for each array index 0, 1, 2, 3).

**Used in your project:**
Expanding opportunities with 4 SBUs into 4 separate rows — one row per SBU.

---

### 6. Regular Expressions

```sql
regexp_replace("User_Logon"::text, '[^0-9]', '', 'g')
```

**What this does:**
Removes all non-numeric characters from a string. `[^0-9]` = "anything that's not a digit".

**Used in your project:**
Extracting employee IDs from CRM login names (e.g., "EMP123ABC" → "123").

---

### 7. COALESCE

```sql
coalesce(sbu."SBU Name", o.sbu_code) as sbu
```

**What this does:**
Returns the first non-null value. If lookup table has a standardized name, use it; otherwise fall back to the raw code.

---

## Git & GitHub

### Initialize Git

```bash
cd ~/frontier_analytics
git init
git config --global user.email "suraj.tiwari0722@gmail.com"
git config --global user.name "Suraj Tiwari"
```

### Add .gitignore

```bash
cat > .gitignore << 'EOF'
target/
dbt_packages/
logs/
profiles.yml
~/.dbt/profiles.yml
*.env
*.swp
EOF
```

**Why gitignore these?**
- `target/` — compiled SQL and logs (regenerated every run)
- `dbt_packages/` — third-party packages (like node_modules)
- `profiles.yml` — contains database credentials
- `*.env` — environment variables

### Commit

```bash
git add .
git commit -m "Initial commit: frontier_analytics dbt project

- 8 staging models (SAP + CRM + Excel_Data sources)
- 4 intermediate models (business logic, UNION ALL, array unnesting)
- 2 mart tables (sales orders + CRM pipeline)
- 8 data tests
- Auto-generated documentation with lineage graph"
```

### Push to GitHub

**Manual upload (if you don't want to reset password):**
1. Create new repo on github.com: `frontier-analytics-dbt`
2. Copy project folder to Windows Desktop
3. Upload files via GitHub web interface

**Proper git push (requires Personal Access Token):**
1. Generate token: GitHub → Settings → Developer Settings → Personal Access Tokens
2. Add remote: `git remote add origin https://github.com/SurajT100/frontier-analytics-dbt.git`
3. Push: `git push -u origin master`

---

## Resources

**Official dbt Documentation:**
- https://docs.getdbt.com/docs/introduction
- https://docs.getdbt.com/reference/dbt-jinja-functions/ref
- https://docs.getdbt.com/reference/dbt-jinja-functions/source

**Best Practices:**
- https://docs.getdbt.com/guides/best-practices

**dbt Discourse (Community Forum):**
- https://discourse.getdbt.com/

---

## Conclusion

You've built a complete dbt project from scratch using real production data. The project demonstrates:

✅ **Understanding of ELT architecture** — ETL loads raw data, dbt transforms it  
✅ **Proper layering** — staging, intermediate, marts with clear separation of concerns  
✅ **Source management** — declared 16 sources across 3 schemas  
✅ **Dependency management** — using `ref()` and `source()` to build lineage  
✅ **Data quality** — 8 automated tests catching issues before dashboards  
✅ **Documentation** — auto-generated with lineage graph  
✅ **Version control** — committed to GitHub with proper gitignore  

This is portfolio-ready and interview-ready. You can confidently say you know dbt in job applications and back it up with a real GitHub repository and tangible explanations of what you built and why.

When you're ready to add Airflow orchestration on top of this, start a new chat and we'll build a DAG that schedules your Python ETL + dbt pipeline end-to-end with proper failure handling and monitoring.
