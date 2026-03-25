# Cortex-Orchestrated Data Migration — Complete Prompt Reference

> **Purpose:** This document catalogs every LLM prompt used across the application, ordered by the architecture flow (Tab 1 → Tab 8). It is intended to give the review panel a clear picture of how Cortex AI powers each conversion stage.

---

## Table of Contents

1. [Prompt Architecture Overview](#1-prompt-architecture-overview)
2. [Shared Conversion Rules (Injected into All Prompts)](#2-shared-conversion-rules-injected-into-all-prompts)
3. [Prompt 1 — DDL Conversion (Tables & Views)](#3-prompt-1--ddl-conversion-tables--views)
4. [Prompt 2 — Stored Procedure Conversion](#4-prompt-2--stored-procedure-conversion)
5. [Prompt 3 — UDF Conversion](#5-prompt-3--udf-conversion)
6. [Prompt 4 — SP → dbt Medallion Conversion](#6-prompt-4--sp--dbt-medallion-conversion)
7. [Prompt 5 — Auto-Heal (Self-Correction)](#8-prompt-5--auto-heal-self-correction)
8. [Prompt 6 — Heal Knowledge Summarization](#9-prompt-6--heal-knowledge-summarization)
9. [Known Pitfalls Injection (Feedback Loop)](#10-known-pitfalls-injection-feedback-loop)
10. [Pre-Processing & Post-Processing Pipeline](#11-pre-processing--post-processing-pipeline)
11. [Prompt Flow Diagram (End-to-End)](#12-prompt-flow-diagram-end-to-end)

---

## 1. Prompt Architecture Overview

All prompts are executed via:

```sql
SELECT SNOWFLAKE.CORTEX.COMPLETE('<model>', '<escaped_prompt>') AS response
```

| # | Prompt Name | Model | Tabs Used | Input | Output Format |
|---|-------------|-------|-----------|-------|---------------|
| 1 | DDL Conversion | `claude-opus-4-5` | Tab 1 (Tables), Tab 2 (Views) | SQL Server DDL | Raw Snowflake DDL text |
| 2 | SP Conversion | `claude-opus-4-5` | Tab 4 (Procedures) | SQL Server SP body | Snowflake Python SP text |
| 3 | UDF Conversion | `claude-opus-4-5` | Tab 3 (Functions) | SQL Server UDF body | Snowflake SQL/Python UDF text |
| 4 | SP → dbt Medallion | `claude-opus-4-5` | Tab 5 (SP → dbt) | SP body + metadata | Structured JSON (models/sources/columns) | |
| 5 | Auto-Heal | `claude-opus-4-5` | Tab 1–4 (Deploy) | Failed DDL + error message | Corrected DDL text |
| 6 | Heal Summarization | `mistral-large2` | Internal (KB) | Before/After DDL + error | Bullet-point fix description |

### Prompt Execution Flow (per conversion)

```
┌──────────────────────────────────────────────────────────────────────┐
│ USER INPUT (DDL / SP / UDF / SSIS XML)                               │
│        │                                                             │
│        ▼                                                             │
│ ┌─────────────────────┐                                              │
│ │ Pre-Processing       │  _sanitize_sp_for_prompt() — neutralize $() │
│ │                      │  extract_main_ddl() — strip GO separators   │
│ │                      │  Truncate to char limit (6K SP / 8K SSIS)   │
│ └─────────┬───────────┘                                              │
│           ▼                                                          │
│ ┌─────────────────────┐                                              │
│ │ Prompt Assembly      │  Static rules (CONVERSION_RULES_ALL)        │
│ │                      │  + Known Pitfalls (_get_known_pitfalls)      │
│ │                      │  + Source code                               │
│ └─────────┬───────────┘                                              │
│           ▼                                                          │
│ ┌─────────────────────┐                                              │
│ │ Escape & Execute     │  .replace("'", "''") for SQL string safety  │
│ │                      │  SNOWFLAKE.CORTEX.COMPLETE(model, prompt)    │
│ └─────────┬───────────┘                                              │
│           ▼                                                          │
│ ┌─────────────────────┐                                              │
│ │ Post-Processing      │  clean_sql_response() — strip markdown      │
│ │                      │  _robust_json_parse() — 4-level fallback    │
│ │                      │  normalize_snowflake_identifiers()           │
│ │                      │  Ensure CREATE OR REPLACE                    │
│ └─────────┬───────────┘                                              │
│           ▼                                                          │
│    CONVERTED OUTPUT → Deploy or store in session_state               │
│           │                                                          │
│     (on deploy failure)                                              │
│           ▼                                                          │
│ ┌─────────────────────┐                                              │
│ │ Auto-Heal Loop       │  Up to 3 Cortex calls to self-correct       │
│ │                      │  _save_heal_knowledge() → feedback loop     │
│ └──────────────────────┘                                              │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 2. Shared Conversion Rules (Injected into All Prompts)

These constant rule blocks are **prepended to every conversion prompt** to ensure consistent SQL Server → Snowflake translation. They are defined once at the top of the app and reused across all prompt functions.

### 2.1 CONVERSION_RULES_NAMING
```
NAMING RULES (MANDATORY):
- ALL column names, table names, and identifiers MUST be UPPERCASE
- Remove ALL spaces from identifiers: "Created Date" → CREATEDDATE
- Strip bracket notation: [Created Date] → CREATEDDATE
- Strip [dbo]. prefix: [dbo].[TableName] → TABLENAME
- Preserve underscores: Order_Details → ORDER_DETAILS
- CamelCase to UPPERCASE: CreatedDate → CREATEDDATE
```

### 2.2 CONVERSION_RULES_TYPES
```
DATA TYPE MAPPINGS (SQL Server → Snowflake):
- VARCHAR(MAX) / NVARCHAR(MAX) → VARCHAR(16777216)
- NVARCHAR(n) → VARCHAR(n)
- NCHAR(n) → CHAR(n)
- NTEXT / TEXT → VARCHAR(16777216)
- BIT → BOOLEAN
- MONEY → NUMBER(19,4), SMALLMONEY → NUMBER(10,4)
- DATETIME / DATETIME2 → TIMESTAMP_NTZ
- SMALLDATETIME → TIMESTAMP_NTZ
- DATETIMEOFFSET → TIMESTAMP_TZ
- IMAGE / VARBINARY(MAX) → BINARY
- UNIQUEIDENTIFIER → VARCHAR(36)
- XML → VARIANT
- TINYINT → NUMBER(3,0)
- INT / INTEGER → NUMBER
- INT(10) / INTEGER(10) → NUMBER(10)
- NUMERIC(16,2) → NUMBER(16,2)
- BIGINT → NUMBER(19,0)
- REAL → FLOAT
- FLOAT → FLOAT (Snowflake native; NOT for monetary columns)

IMPORTANT: For monetary / amount / currency columns, ALWAYS use NUMBER(19,4) — never FLOAT.
FLOAT introduces floating-point rounding errors that are unacceptable for financial data.
```

### 2.3 CONVERSION_RULES_FUNCTIONS
```
FUNCTION MAPPINGS (SQL Server → Snowflake):
- GETDATE() → CURRENT_TIMESTAMP()
- SYSDATETIME() → CURRENT_TIMESTAMP()
- ISNULL(a, b) → COALESCE(a, b)
- CHARINDEX(sub, str) → POSITION(sub IN str)
- LEN(str) → LENGTH(str)
- DATALENGTH(str) → OCTET_LENGTH(str)
- SELECT TOP N → SELECT ... LIMIT N (move to end)
- NEWID() → UUID_STRING()
- ISNUMERIC(expr) → TRY_CAST(expr AS NUMBER) IS NOT NULL
- CONVERT(type, expr) → CAST(expr AS type)
- STRING_AGG(col, sep) → LISTAGG(col, sep)
- FORMAT(date, fmt) → TO_CHAR(date, fmt)
- DATEADD/DATEDIFF → same syntax (Snowflake supports it)
```

### 2.4 CONVERSION_RULES_SYNTAX
```
CONSTRAINT & SYNTAX RULES:
- IDENTITY(1,1) → AUTOINCREMENT
- Remove CLUSTERED / NONCLUSTERED index hints
- Remove WITH (NOLOCK) and all table hints
- Remove SET NOCOUNT ON
- Remove PRINT statements
- FOREIGN KEY REFERENCES → remove (Snowflake doesn't enforce)
- CHECK constraints (cross-table) → remove
- Keep PRIMARY KEY, UNIQUE, NOT NULL
- DEFAULT GETDATE() → DEFAULT CURRENT_TIMESTAMP()
- #TempTable / ##GlobalTemp → use CTE or TEMPORARY TABLE
- EXEC / EXECUTE → CALL
- @@ROWCOUNT → remove or use SQLROWCOUNT
- @@IDENTITY / SCOPE_IDENTITY() → use AUTOINCREMENT
- Strip [dbo]. prefix from all references
- Remove USE [database] statements
- Remove GO batch separators
- Always use CREATE OR REPLACE
```

### 2.5 Known Pitfalls Block (Dynamic — from Heal Knowledge Base)

This block is **queried from the `HEAL_KNOWLEDGE_BASE` table** at runtime and appended to conversion prompts. It contains patterns from previously-healed errors so the LLM learns to avoid them proactively.

```
KNOWN PITFALLS (these errors have occurred before in past conversions — AVOID them):
- [4x] SQL compilation error: invalid identifier '<value>' → Removed [brackets], used COALESCE instead of ISNULL
- [2x] SQL compilation error: unexpected '<value>' → Removed trailing comma before closing parenthesis
- [1x] SQL compilation error: invalid default value → Added ::DATE cast for date default literals
```

> **Feedback loop:** Every time auto-heal successfully fixes a DDL, the generalized error pattern and fix description are saved to `HEAL_KNOWLEDGE_BASE`. The top 15 most-frequent patterns are then injected into all future conversion prompts via `_get_known_pitfalls()`. This means the app **gets smarter over time** — errors fixed once are proactively avoided in subsequent conversions.

---

## 3. Prompt 1 — DDL Conversion (Tables & Views)

**Function:** `convert_ddl_to_snowflake(source_ddl, source_type="SQL Server")`  
**Used in:** Tab 1 (Tables), Tab 2 (Views)  
**Model:** `claude-opus-4-5`  
**Output:** Raw Snowflake DDL (text)

### Full Prompt Template

```
Convert this {source_type} DDL to Snowflake SQL.
IMPORTANT: Always use CREATE OR REPLACE syntax.

{CONVERSION_RULES_ALL}
    ↳ Includes: NAMING + TYPES + FUNCTIONS + SYNTAX rules (see Section 2)

{Known Pitfalls from Heal KB}
    ↳ Dynamic block from _get_known_pitfalls()

Return ONLY the converted DDL, no explanations, no markdown:

{source_ddl}
```

### Prompt Behavior
- **Simplest prompt** — injects all four rule blocks plus dynamic pitfalls, then the source DDL
- Instructs the LLM to return **only DDL text** — no explanations, no markdown
- Post-processing applies: `clean_sql_response()` → regex to ensure `CREATE OR REPLACE` → `normalize_snowflake_identifiers()`

### Example Input / Output

**Input (SQL Server):**
```sql
CREATE TABLE [dbo].[CustomerAddress] (
    [AddressID] INT IDENTITY(1,1) NOT NULL,
    [CustomerName] NVARCHAR(100),
    [CreatedDate] DATETIME DEFAULT GETDATE(),
    CONSTRAINT PK_Address PRIMARY KEY CLUSTERED ([AddressID])
);
```

**Output (Snowflake):**
```sql
CREATE OR REPLACE TABLE CUSTOMERADDRESS (
    ADDRESSID NUMBER AUTOINCREMENT NOT NULL,
    CUSTOMERNAME VARCHAR(100),
    CREATEDDATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (ADDRESSID)
);
```

---

## 4. Prompt 2 — Stored Procedure Conversion

**Function:** `convert_sp_to_snowflake(source_sp, source_type="SQL Server")`  
**Used in:** Tab 4 (Procedures)  
**Model:** `claude-opus-4-5`  
**Output:** Snowflake Python Stored Procedure (text)

### Full Prompt Template

```
Convert this {source_type} stored procedure to a Snowflake Python stored procedure.

Use this EXACT format:
CREATE OR REPLACE PROCEDURE procedure_name(param1 TYPE DEFAULT NULL)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
def main(session, param1):
    # Your Python code here
    
    # To run SQL queries:
    result = session.sql("SELECT ...").collect()
    
    # To get values:
    value = result[0][0] if result else None
    
    # Return a value
    return value
$$;

CRITICAL RULES:
1. Use LANGUAGE PYTHON
2. RUNTIME_VERSION = '3.11'
3. PACKAGES = ('snowflake-snowpark-python')
4. HANDLER = 'main'
5. First parameter of main() is always 'session' (Snowpark session)
6. Use session.sql("...").collect() to execute SQL
7. Use f-strings for dynamic SQL: session.sql(f"SELECT * FROM table WHERE col = '{var}'").collect()
8. For dates use: from datetime import datetime, timedelta
9. Use Python datetime operations, not SQL date functions in code
10. Return the final value

{CONVERSION_RULES_NAMING}
{CONVERSION_RULES_TYPES}
{CONVERSION_RULES_FUNCTIONS}
{Known Pitfalls from Heal KB}

SQL SERVER SYNTAX TO REMOVE/CONVERT:
- Remove SET NOCOUNT ON, PRINT statements
- Remove WITH (NOLOCK) and all table hints
- #TempTable → use Python variables or session.sql("CREATE TEMPORARY TABLE ...")
- @@ROWCOUNT → len(result) after .collect()
- @@IDENTITY / SCOPE_IDENTITY() → use AUTOINCREMENT or sequence
- EXEC/EXECUTE → session.sql("CALL ...")
- BEGIN...END → Python control flow
- WHILE loops → Python while loops
- IF/ELSE → Python if/else

Return ONLY the procedure code, no explanations, no markdown:

{source_sp}
```

### Prompt Behavior
- Converts T-SQL stored procedures to **Snowflake Python SPs** (Snowpark)
- Provides an explicit code template so the LLM follows the exact SP signature format
- Specific rules for converting T-SQL control flow (WHILE, IF/ELSE, temp tables) to Python equivalents
- Post-processing: `clean_sql_response()` → ensure `CREATE OR REPLACE PROCEDURE` → `normalize_snowflake_identifiers()`

### Key Design Decisions
- **Always Python SPs** (not Snowflake Scripting) — chosen for richer control flow and error handling parity with T-SQL
- `session` is always the first parameter — matches Snowpark convention
- Dynamic SQL via f-strings rather than EXECUTE IMMEDIATE

---

## 5. Prompt 3 — UDF Conversion

**Function:** `convert_udf_to_snowflake(source_udf, source_type="SQL Server")`  
**Used in:** Tab 3 (Functions)  
**Model:** `claude-opus-4-5`  
**Output:** Snowflake SQL or Python UDF (text)

### Full Prompt Template

```
Convert this {source_type} User Defined Function (UDF) to a Snowflake UDF.

MANDATORY: OUTPUT SQL UDF BY DEFAULT. Only use Python UDF as last resort.

CLASSIFICATION RULES (strict):
- DECLARE + SET + RETURN with simple logic → SQL UDF (flatten to CASE/IFF/COALESCE)
- IF/ELSE returning values → SQL UDF (convert to IFF() or CASE WHEN)
- String ops (LEN, SUBSTRING, CHARINDEX) → SQL UDF
- Date ops (DATEADD, DATEDIFF, GETDATE) → SQL UDF
- Math, ISNULL, COALESCE chains → SQL UDF
- Table-Valued Function (RETURNS TABLE) → SQL UDF with RETURNS TABLE(...)
- ONLY use Python UDF for: WHILE loops, cursors, TRY/CATCH, dynamic SQL (EXEC), external APIs

CONVERSION STRATEGY:
1. Flatten multi-statement T-SQL (DECLARE/SET/IF/ELSE) into a SINGLE SQL expression
2. Example: DECLARE @r; IF @x=1 SET @r='A' ELSE SET @r='B'; RETURN @r
   → CASE WHEN X=1 THEN 'A' ELSE 'B' END

SQL UDF FORMAT (default — use this):
CREATE OR REPLACE FUNCTION FUNCTION_NAME(PARAM1 DATA_TYPE)
RETURNS RETURN_TYPE
AS
$$
    CASE WHEN ... THEN ... ELSE ... END
$$;

PYTHON UDF FORMAT (only if SQL is impossible):
CREATE OR REPLACE FUNCTION FUNCTION_NAME(PARAM1 DATA_TYPE)
RETURNS RETURN_TYPE
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'udf_handler'
AS
$$
def udf_handler(param1):
    return result
$$;

{CONVERSION_RULES_NAMING}
{CONVERSION_RULES_TYPES}
{CONVERSION_RULES_FUNCTIONS}
{Known Pitfalls from Heal KB}

ADDITIONAL RULES:
1. UPPERCASE the function name and all identifiers
2. Strip [dbo]. prefix and bracket notation
3. Remove WITH SCHEMABINDING
4. For Table-Valued Functions: RETURNS TABLE(COL1 TYPE, COL2 TYPE) with SQL body
5. Ensure all string literals use single quotes
6. Return ONLY the function DDL — no explanations, no markdown

{source_type} UDF to convert:
{source_udf}
```

### Prompt Behavior
- **SQL-first philosophy**: The prompt enforces SQL UDF as the default output; Python UDF is a last resort
- Provides an explicit classification decision tree so the LLM doesn't over-escalate to Python
- Shows the "flatten" strategy for converting procedural T-SQL (DECLARE/SET/IF) to a single SQL expression
- Post-processing: `clean_sql_response()` → ensure `CREATE OR REPLACE FUNCTION` → `normalize_snowflake_identifiers()`

### Classification Logic
| T-SQL Pattern | → Snowflake Output |
|---|---|
| DECLARE + SET + RETURN (simple) | SQL UDF with CASE/IFF |
| IF/ELSE with simple returns | SQL UDF with IFF() or CASE WHEN |
| String / Date / Math operations | SQL UDF (native Snowflake functions) |
| Table-Valued Function | SQL UDF with RETURNS TABLE(...) |
| WHILE loops, cursors, TRY/CATCH | Python UDF (only these cases) |

---

## 6. Prompt 4 — SP → dbt Medallion Conversion

**Function:** `convert_sp_to_dbt_medallion(source_sp, sp_name, project_name, source_database, source_schema)`  
**Used in:** Tab 5 (SP → dbt)  
**Model:** `claude-opus-4-5`  
**Output:** Structured JSON with bronze/silver/gold models, sources, columns  
**Char Limit:** SP body truncated to 6,000 characters

### Full Prompt Template

```
[STRICT JSON OUTPUT ONLY - NO PREAMBLE - NO MARKDOWN]

TASK: Convert the attached SQL Server stored procedure to modular dbt models.
Break into multiple models following dbt best practices with 
staging → intermediate → marts architecture.

RULES:
REPLICATE EXACT SP LOGIC INTO DBT MODELS.

CRITICAL - TARGET TABLE IDENTIFICATION:
The TARGET TABLE is the table being INSERT INTO, MERGE INTO, or UPDATE in the SP.
This is the FINAL OUTPUT table.
CRITICAL - DO NOT CREATE ANY SOURCE OR STAGING MODEL FOR THE TARGET TABLE!
The target table does not exist yet - it will be created by dbt.
Never use source('raw', '<target_table>') or create stg_<target_table> models.

USE FUNCTIONS LOGIC CORRECTLY, USE FUNCTION CALLS AT RIGHT PLACES,
For FUNCTION CALLS USE Fully qualify the FUNCTIONS in the model SQL using
SOURCE DATABASE: {src_db} AND SOURCE SCHEMA: {src_schema}
for eg. {src_db}.{src_schema}.<FUNCTION_NAME>

IMPORTANT: ALL source objects (tables, views, functions) exist in database '{src_db}'
and schema '{src_schema}'. Use these exact values for all source references 
and function calls.

USE single quotes for string literals eg. 'N/A', not double quotes.
Double quotes in Snowflake mean identifiers (column/table names).

1. REPLICATE THE EXACT LOGIC FROM STORED PROCEDURE INTO THE DBT MODELS 
   WHICH WILL MATCH THE OUTPUT OF SP TO THE FINAL DBT MODEL!!!

2. Identify ALL SOURCE tables (tables being READ FROM via SELECT/JOIN).
   Map them to dbt source() references. 
   EXCLUDE the target table from sources!

3. CONVERSION APPROACH TO BE FOLLOWED STRICTLY:
    
    LAYER 1 - STAGING (stg_*.sql):
    - Create one model per SOURCE table only (tables being SELECT FROM, not INSERT INTO)
    - NEVER create staging model for the target/output table
    - Materialized as VIEW (always — lightweight, no storage cost, always fresh from source)
    - Light transformations only (rename, cast, filter)
    
    LAYER 2 - INTERMEDIATE (int_*.sql):
    - One model per business logic operation
    - Choose materialization based on complexity:
      * VIEW — if simple transforms, referenced by 1-2 downstream models
      * TABLE — if heavy joins, window functions, or referenced by 3+ downstream models
      * EPHEMERAL — if only used once as a CTE in another model
    - Convert WHILE loops → CTEs with ROW_NUMBER()/window functions
    - Convert temp tables → CTEs or separate models
    - Convert UPDATE statements → SELECT with CASE/transformations
    
    LAYER 3 - MARTS (fct_*.sql, dim_*.sql):
    - Choose materialization based on the SP logic and downstream usage:
      * TABLE — default for marts; best for BI tool queries, large result sets
      * VIEW — only if the mart is a simple passthrough or rarely queried
      * INCREMENTAL — if SP uses INSERT with date filters, MERGE, or processes 
        only new/changed rows
    - UNION ALL of all intermediate classifications

5. Every model SQL must start with a dbt config block:
   {{ config(materialized='view', schema='bronze') }}        ← bronze: always view
   {{ config(materialized='table', schema='silver') }}       ← silver: table if heavy
   {{ config(materialized='view', schema='silver') }}        ← silver: view if light
   {{ config(materialized='table', schema='gold') }}         ← gold: table for BI-facing
   {{ config(materialized='incremental', schema='gold') }}   ← gold: incremental if date-based

6. For schema.yml: List every column with name, description, and at least 1 test 
   (not_null or unique where applicable).

7. For sources.yml: List ONLY source tables (tables being read from), 
   NEVER include the target table.

8. Use Snowflake-compatible SQL only (no TOP, use LIMIT; no ISNULL, use COALESCE).

{CONVERSION_RULES_NAMING}
{CONVERSION_RULES_TYPES}
{CONVERSION_RULES_FUNCTIONS}

9. DO NOT use single quotes inside SQL strings in JSON — use double quotes or escape them.
10. Escape all newlines as \n in JSON strings.
11. All SQL Server UDFs are converted and already exist in Snowflake in same schema.
    Replace SQL Server UDFs with Snowflake UDF calls.
12. The FINAL GOLD MODEL name must be the TARGET TABLE name from the SP 
    (e.g. SP inserts into DIM_ADDRESS → gold model = dim_address).
13. ALWAYS convert WHILE loops to set-based CTEs/recursive CTEs.
14. Explicit CASTING required in all models (no dbt contracts).
15. DBT MODEL NAMING (CRITICAL):
    - ALL model names MUST use lowercase snake_case
    - CamelCase split: DimAddress → dim_address
    - Bronze: stg_<snake_case>.sql
    - Silver: int_<snake_case>.sql
    - Gold: <snake_case_target>.sql

REQUIRED OUTPUT FILES:
1. All SQL model files organized in staging/intermediate/marts folders
2. sources.yml defining all source tables
3. schema.yml for each layer with descriptions

Return ONLY this exact JSON structure (no markdown, no backticks, no explanation):
{
    "bronze_models": [
        {
            "name": "stg_<table_name>",
            "description": "Raw ingestion of <table_name> from source",
            "sql": "{{ config(materialized=\"view\", schema=\"bronze\") }}\n\n
                    SELECT ...\nFROM {{ source(\"raw\", \"<table_name>\") }}"
        }
    ],
    "silver_models": [
        {
            "name": "int_<model_name>",
            "description": "Cleaned and transformed <model_name>",
            "sql": "{{ config(materialized=\"table\", schema=\"silver\") }}\n\n
                    SELECT ...\nFROM {{ ref(\"bronze_<table_name>\") }}"
        }
    ],
    "gold_models": [
        {
            "name": "<model_name>",
            "description": "Business-ready aggregation of <model_name>",
            "sql": "{{ config(materialized=\"table\", schema=\"gold\") }}\n\n
                    SELECT ...\nFROM {{ ref(\"silver_<model_name>\") }}"
        }
    ],
    "sources": [
        {
            "name": "<source_table_name>",
            "database": "{src_db}",
            "schema": "{src_schema}",
            "description": "Source table from raw layer"
        }
    ],
    "columns": {
        "<model_name>": [
            {"name": "<col_name>", "description": "<description>", "tests": ["not_null"]}
        ]
    }
}

Stored Procedure Name: {sp_name}
Stored Procedure:
{source_sp (first 6000 chars)}
```

### Prompt Behavior
- **Most complex prompt** — produces a complete dbt project structure in one LLM call
- Pre-processing: `_sanitize_sp_for_prompt()` neutralizes SQLCMD `$()` variables before prompt assembly
- Post-processing: `_robust_json_parse()` (4-level fallback) handles LLM JSON inconsistencies
- The LLM must produce **valid JSON** with all model SQL, descriptions, and column metadata
- Source database/schema are injected so the LLM can produce correct `source()` and function references

### Pre-Conversion Validation (Before This Prompt Runs)
- `validate_sp_dependencies()` extracts table/function references via regex
- Cross-references against Snowflake `INFORMATION_SCHEMA` catalog
- Blocks conversion if dependencies are missing — prompt is never sent for invalid SPs

### JSON Output Structure

| Key | Contents |
|-----|----------|
| `bronze_models[]` | Staging views — one per source table, `{{ source() }}` refs |
| `silver_models[]` | Intermediate transforms — business logic, joins, window functions |
| `gold_models[]` | Final mart — matches SP target table name |
| `sources[]` | Source table definitions for `sources.yml` |
| `columns{}` | Per-model column metadata with tests for `schema.yml` |

---



## 7. Prompt 5 — Auto-Heal (Self-Correction)

**Function:** `_cortex_auto_heal(ddl, error_msg, obj_type, obj_name, target_db, target_schema)`  
**Used in:** Tab 1–4 (Deploy actions)  
**Model:** `claude-opus-4-5`  
**Max Attempts:** 3 per object  
**Output:** Corrected DDL text

### Full Prompt Template (per attempt)

```
You are a Snowflake SQL expert. The following DDL failed to execute on Snowflake.

FAILED DDL:
{current_ddl}

SNOWFLAKE ERROR:
{error_msg}

Fix ONLY the specific error. Return ONLY the corrected DDL — no explanation, 
no markdown fences, no commentary.
Keep CREATE OR REPLACE and all column definitions intact. 
Use valid Snowflake syntax only.
```

### Prompt Behavior
- **Minimalist prompt** — only the failed DDL and error message; no conversion rules injected
- Called **only when `_is_healable_error()` returns True** — skips permission/infrastructure errors entirely
- Each attempt feeds the **latest DDL** (not the original) — if attempt 1 partially fixes an issue, attempt 2 sees the partially-fixed DDL with the new error
- After successful heal, the FQ name (`DB.SCHEMA.OBJECT`) is **always re-stamped** — never trusts the LLM to preserve it
- On success: saves healed DDL to `CONVERTED_OBJECTS` with `WAS_HEALED=TRUE`

### Retry Flow

```
Attempt 1: Send original_ddl + error₁ → Cortex returns healed_ddl₁
    ├── Deploy healed_ddl₁ → SUCCESS → return (True, healed_ddl₁, 1 attempt)
    └── Deploy fails with error₂ → is error₂ healable?
           ├── NO  → return failure
           └── YES →
Attempt 2: Send healed_ddl₁ + error₂ → Cortex returns healed_ddl₂
    ├── Deploy healed_ddl₂ → SUCCESS → return (True, healed_ddl₂, 2 attempts)
    └── Deploy fails with error₃ →
Attempt 3: Send healed_ddl₂ + error₃ → Cortex returns healed_ddl₃
    ├── Deploy healed_ddl₃ → SUCCESS → return (True, healed_ddl₃, 3 attempts)
    └── Deploy fails → return (False, healed_ddl₃, 3 attempts) — EXHAUSTED
```

### Error Classification (Gate Before Prompt)

**Healable (prompt sent):**
- `SQL compilation error`, `invalid identifier`, `syntax error`, `unexpected`
- `invalid value`, `invalid default`, `not recognized`, `unsupported`
- `unknown function`, `missing column`, `duplicate column`
- Snowflake error codes: `001003`, `001007`, `002262`, `002043`, `000904`, `001044`

**Non-Healable (prompt skipped, immediate failure):**
- `insufficient privileges`, `permission denied`, `access denied`
- `warehouse.*not.*found`, `no active warehouse`
- `ACCOUNTADMIN`, `quota exceeded`, `session does not have a current database`
- `network`, `timeout`, `authentication`, `role .* does not exist`

---

## 8. Prompt 6 — Heal Knowledge Summarization

**Function:** `_save_heal_knowledge()` (internal)  
**Used in:** After successful auto-heal  
**Model:** `mistral-large2` (lighter/faster model — only summarization, no conversion)  
**Output:** 2–3 bullet points describing what changed

### Full Prompt Template

```
Compare these two Snowflake DDLs and describe ONLY what changed to fix the error. 
Be concise (2-3 bullet points max).

ERROR: {error_msg (first 300 chars)}

BEFORE (failed):
{original_ddl (first 1500 chars)}

AFTER (fixed):
{healed_ddl (first 1500 chars)}

Return ONLY the bullet points describing the fix, no preamble:
```

### Prompt Behavior
- Uses **`mistral-large2`** (not Claude) — this is a quick summarization task, not a conversion
- Called **after** a successful heal to generate a human-readable description of what was fixed
- The summary is stored in the `HEAL_KNOWLEDGE_BASE` table (`FIX_DESCRIPTION` column)
- Later retrieved by `_get_known_pitfalls()` and injected into future conversion prompts
- This creates the **self-improving feedback loop**: heal → summarize → teach → prevent

### Example Output
```
- Removed FOREIGN KEY constraint referencing non-existent table DimClient
- Changed BINARY(8000) to BINARY (Snowflake does not support size for BINARY)
- Added ::DATE cast to DEFAULT '1900-01-01' for DATE column
```

---

## 9. Known Pitfalls Injection (Feedback Loop)

**Function:** `_get_known_pitfalls()`  
**Cached:** `@st.cache_data(ttl=120)` — refreshes every 2 minutes  
**Source:** `HEAL_KNOWLEDGE_BASE` table (top 15 by occurrence count)

### How It Works

```
┌──────────────────────────────────────────────────────────────────┐
│                    SELF-IMPROVING FEEDBACK LOOP                   │
│                                                                  │
│  1. User converts DDL ─────────────────────────────────┐         │
│                                                        │         │
│  2. Deploy fails (SQL compilation error)               │         │
│        │                                               │         │
│  3. Auto-heal fixes it (Prompt 6)                      │         │
│        │                                               │         │
│  4. Heal summarized (Prompt 7, mistral-large2)         │         │
│        │                                               │         │
│  5. Pattern + fix stored in HEAL_KNOWLEDGE_BASE        │         │
│        │                                               │         │
│  6. _get_known_pitfalls() reads top 15 patterns        │         │
│        │                                               │         │
│  7. Injected into Prompts 1, 2, 3 for next conversion ─┘         │
│                                                                  │
│  Result: Same error class is AVOIDED in future conversions       │
└──────────────────────────────────────────────────────────────────┘
```

### Query Behind the Injection

```sql
SELECT ERROR_PATTERN, FIX_DESCRIPTION, OCCURRENCE_COUNT
FROM HEAL_KNOWLEDGE_BASE
ORDER BY OCCURRENCE_COUNT DESC
LIMIT 15
```

### Injected Text Format

```
KNOWN PITFALLS (these errors have occurred before in past conversions — AVOID them):
- [4x] SQL compilation error: invalid identifier '<value>' → Removed brackets, used COALESCE
- [2x] SQL compilation error: unexpected '<value>' → Removed trailing comma
...
```

---

## 10. Pre-Processing & Post-Processing Pipeline

### Pre-Processing (Before Prompt)

| Step | Function | Purpose |
|------|----------|---------|
| 1 | `extract_main_ddl(sql)` | Splits on `\nGO\n`, extracts first `CREATE` statement |
| 2 | `_sanitize_sp_for_prompt(sp, db)` | Replaces `[$(VarName)]` and `$(VarName)` with actual DB name |
| 3 | Char truncation | SP body → 6,000 chars; SSIS XML → 8,000 chars |
| 4 | `_get_known_pitfalls()` | Fetches learned error patterns from KB |
| 5 | `validate_sp_dependencies()` | (Tab 5 only) Checks all referenced objects exist in Snowflake |
| 6 | `.replace("'", "''")` | Escapes single quotes for SQL string interpolation |

### Post-Processing (After Prompt)

| Step | Function | Purpose |
|------|----------|---------|
| 1 | `clean_sql_response(response)` | Strips `` ```sql ``, `` ``` `` markdown fences from LLM output |
| 2 | `_robust_json_parse(text)` | (JSON prompts only) 4-level fallback: direct → newline escape → quote fix → ast.literal_eval |
| 3 | Regex: ensure `CREATE OR REPLACE` | Patches the header if LLM omitted `OR REPLACE` |
| 4 | `normalize_snowflake_identifiers(ddl)` | Uppercases all identifiers in the DDL header (preserves string literals and body) |
| 5 | `_strip_unsupported_constraints(ddl)` | (Tables only, pre-deploy) Removes FOREIGN KEY and CHECK constraints |
| 6 | `_deploy_ddl()` FQ re-stamp | (Deploy only) Replaces object name with `DB.SCHEMA.OBJECT` — never trusts LLM naming |

### `_robust_json_parse()` — 4-Level Fallback Chain

```
Level 1: json.loads(extracted_json_block)
    ↓ fails
Level 2: Replace \n/\r with \\n/\\r → json.loads()
    ↓ fails
Level 3: Regex single→double quote conversion → json.loads()
    ↓ fails
Level 4: Python ast.literal_eval() (handles True/False/None)
    ↓ fails
Return None → show error + raw output for debugging
```

---

## 11. Prompt Flow Diagram (End-to-End)

### Tab 1 & 2: Tables / Views

```
Source DDL → extract_main_ddl() → Prompt 1 (DDL Conversion) → clean + normalize
    → User reviews → Deploy → _strip_unsupported_constraints() → _deploy_ddl()
        ├── Success → save_conversion(STATUS='CONVERTED')
        └── Fail → _is_healable_error()?
              ├── No  → show error
              └── Yes → Prompt 6 (Auto-Heal, up to 3×) → re-deploy
                    ├── Success → Prompt 7 (Summarize fix) → save to KB
                    │            → save_conversion(STATUS='DEPLOYED_HEALED')
                    └── Fail    → show error after 3 attempts
```

### Tab 3: Functions

```
Source UDF → Prompt 3 (UDF Conversion) → clean + normalize
    → User reviews → Deploy → _deploy_ddl()
        ├── Success → save_conversion()
        └── Fail → Auto-Heal loop (same as above)
```

### Tab 4: Procedures

```
Source SP → Prompt 2 (SP Conversion) → clean + normalize
    → User reviews → Deploy → _deploy_ddl()
        ├── Success → save_conversion()
        └── Fail → Auto-Heal loop (same as above)
```

### Tab 5: SP → dbt

```
Source SP → _sanitize_sp_for_prompt() → validate_sp_dependencies()
    ├── Missing deps → BLOCKED (red panel, no LLM call)
    └── All valid → Prompt 4 (SP → dbt Medallion) → _robust_json_parse()
        → generate_dbt_project_files() → merge_dbt_project_files()
        → Files stored in session_state.dbt_project_files
```

### Tab 6: Execute (dbt Deploy)

```
session_state.dbt_project_files → zip → upload to @DBT_PROJECT_STAGE
    → Create/Execute DBT_EXECUTION_TASK with dbt build/run/test/docs
    → Poll task status → display results / render docs HTML
    (No LLM prompts in this tab — pure deployment infrastructure)
```

### Tab 8: Dashboard

```
Queries CONVERTED_OBJECTS, TASK_HISTORY, INFORMATION_SCHEMA
Parses {{ ref() }} / {{ source() }} from stage files for lineage DAG
Surfaces auto-healed objects with diff viewer
(No LLM prompts in this tab — pure analytics / visualization)
```

### Tab 9: RoadMap

---

## Summary: All 6 Prompts at a Glance

| # | Name | Model | When | Input Size | Output | Post-Process |
|---|------|-------|------|-----------|--------|-------------|
| 1 | DDL Conversion | claude-opus-4-5 | Tab 1–2: Convert | Full DDL | DDL text | clean → normalize |
| 2 | SP Conversion | claude-opus-4-5 | Tab 4: Convert | Full SP | Python SP text | clean → normalize |
| 3 | UDF Conversion | claude-opus-4-5 | Tab 3: Convert | Full UDF | SQL/Python UDF text | clean → normalize |
| 4 | SP → dbt | claude-opus-4-5 | Tab 5: Convert | 6K chars max | JSON (models) | _robust_json_parse |
| 5 | Auto-Heal | claude-opus-4-5 | Tab 1–4: Deploy fail | DDL + error | DDL text | clean + FQ re-stamp |
| 6 | Heal Summarize | mistral-large2 | After heal success | Before/After DDL | Bullet points | Store to KB |

---

