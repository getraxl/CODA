# CODA — MVP Setup Guide
Cortex-Orchestrated Data Automation

> **Built with Snowflake Cortex Code** — AI-Powered Development Assistant  
> **Runtime**: Streamlit in Snowflake (single-file app, ~4200+ lines)  
> **Primary AI Model**: `claude-opus-4-5` via `SNOWFLAKE.CORTEX.COMPLETE`

---

## Table of Contents

1. [Overview](#1-overview)
2. [Prerequisites](#2-prerequisites)
3. [Snowflake Account Setup](#3-snowflake-account-setup)
4. [Deploying the App](#4-deploying-the-app)
5. [First Launch Checklist](#5-first-launch-checklist)
6. [Codebase Structure](#6-codebase-structure)
7. [AI Models & Prompts](#7-ai-models--prompts)
8. [Conversion Rules Reference](#8-conversion-rules-reference)
9. [Auto-Heal System](#9-auto-heal-system)
10. [Self-Learning Knowledge Base](#10-self-learning-knowledge-base)
11. [Tab-by-Tab Usage Guide](#11-tab-by-tab-usage-guide)
12. [Dashboard Panels](#12-dashboard-panels)
13. [Snowflake Objects Created at Runtime](#13-snowflake-objects-created-at-runtime)
14. [Session State Reference](#14-session-state-reference)
15. [Caching Strategy](#15-caching-strategy)
16. [Reset System](#16-reset-system)
17. [Skills & Agents Needed](#17-skills--agents-needed)
18. [Troubleshooting](#18-troubleshooting)
19. [Known Limitations](#19-known-limitations)

---

## 1. Overview

**Cortex-Orchestrated Data Automation**  is a Streamlit-in-Snowflake application that automates migration of SQL Server database objects to Snowflake-native equivalents and dbt projects.

### Key Capabilities

| Capability | Detail |
|---|---|
| AI-powered DDL conversion | Tables, Views, Functions, Stored Procedures |
| SP → dbt medallion project | Bronze / Silver / Gold model generation with `schema.yml`, `sources.yml` |
| One-click deploy | Direct deployment to any Snowflake `DATABASE.SCHEMA` |
| Cortex Auto-Heal | Self-correcting deploy errors (up to 3 AI-driven retries) |
| Self-Learning KB | Error patterns captured and injected into future conversions |
| Batch upload | `.sql`, `.txt`, `.zip` — zip auto-extracted |
| Live dashboard | 9 panels: models, run history, tests, lineage, file explorer, trends, performance, KB, reset |

---

## 2. Prerequisites

### Snowflake Account

| Requirement | Minimum |
|---|---|
| Snowflake edition | Enterprise or higher (for Cortex AI access) |
| Cortex AI enabled | `SNOWFLAKE.CORTEX.COMPLETE` must be callable |
| Models available | `claude-opus-4-5` (primary), `mistral-large2` (secondary) |
| Streamlit in Snowflake | Enabled on the account |
| Warehouse | `X-SMALL` or larger (for Cortex calls + dbt execution) |
| Role | Must have `CREATE TABLE`, `CREATE STAGE`, `CREATE SCHEMA`, `CREATE TASK`, `EXECUTE TASK` privileges |

### Python Packages (Snowflake-managed — no pip install needed)

```
streamlit              (built-in with Streamlit in Snowflake)
snowflake-snowpark-python  (built-in)
pyyaml                 (built-in)
```

**No external packages or API keys required.** Everything runs inside the Snowflake runtime.

---

## 3. Snowflake Account Setup

### 3.1 Create a Database & Schema


## 4. Deploying the App

### 4.1 Create Streamlit App

1. Open **Snowsight** → **Streamlit** → **+ Streamlit App**
2. Choose:
   - Database: `MIGRATION_DB`
   - Schema: `COCO_APP`
   - Warehouse: `COCO_WH`
3. Replace the default code with the **entire contents** of `coco_streamlit.py` (~4200 lines)
4. Click **Run**

> **Important**: This is a single-file application. Paste the full `coco_streamlit.py` into the Streamlit editor. Do not split it.

### 4.2 Add Required Package

In the Streamlit app editor, click the **Packages** icon (📦) in the left sidebar and add:

```
pyyaml
```

`streamlit` and `snowflake-snowpark-python` are pre-installed.

### 4.3 Verify Launch

On successful launch you should see:
1. **Splash screen** with kipi.ai branding and a green "🚀 Get Started" button
2. After clicking, the **8-tab UI** renders:
   - Tables | Views | Functions | Procedures | SP → dbt | Execute  | Dashboard | RoadMap

---
---

## 6. Codebase Structure

### 6.1 File Inventory

```
📄 coco_streamlit.py                 — The entire app (~4272 lines, single file)
📄 sol_doc.txt                       — Solution document with full architecture docs
📄 generate_coco_ppt.py              — Executive presentation generator (python-pptx)
📄 README.md                         — This file
📄 CustomerAddress.sql               — Sample SQL Server DDL for testing
📄 Territory_lu.sql                  — Sample SQL Server DDL for testing
📄 TerritoryRegion_lu.sql            — Sample SQL Server DDL for testing
📄 usp_PopulateDimAddress.sql        — Sample stored procedure for testing
```

### 6.2 App Code Sections (coco_streamlit.py)

| Lines (approx.) | Section | Purpose |
|---|---|---|
| 1–10 | **Imports** | `streamlit`, `snowpark`, `json`, `re`, `datetime`, `zipfile`, `io`, `os`, `time`, `yaml` |
| 14–99 | **Conversion Rules** | `CONVERSION_RULES_NAMING`, `_TYPES`, `_FUNCTIONS`, `_SYNTAX`, `_ALL` |
| 100–170 | **Identifier Normalizer** | `normalize_snowflake_identifiers()`, `_normalize_header()`, `_uppercase_identifiers_in_line()` |
| 147–175 | **Utility Functions** | `get_databases()`, `get_schemas()`, `clean_sql_response()`, `extract_main_ddl()` |
| 200–340 | **AI Conversion Functions** | `convert_sp_to_snowflake()`, `convert_udf_to_snowflake()`, `convert_ddl_to_snowflake()` |
| 340–420 | **dbt Stage & File Helpers** | `init_dbt_stage()`, `read_file_from_stage()`, `merge_yaml_with_stage()` |
| 424–750 | **CSS Theme** | Full kipi.ai Light Theme (White/Green palette) |
| 754–836 | **Splash Screen** | Animated intro with "Get Started" button |
| 838–850 | **Header** | App title + Cortex AI badge |
| 850–886 | **Configuration** | `TARGET_SCHEMA`, `CONVERSION_TABLE`, `HEAL_KB_TABLE`, `init` calls |
| 886–1000 | **Heal Knowledge Base** | `_generalize_error()`, `_save_heal_knowledge()`, `_get_known_pitfalls()` |
| 1000–1240 | **Advanced Conversions** | `convert_sp_to_dbt_medallion()`, `generate_dbt_project_files()` |
| 1240–1520 | **dbt Project Builder** | File generation: `dbt_project.yml`, `profiles.yml`, `schema.yml`, `sources.yml`, macros |
| 1520–1700 | **Deploy & Auto-Heal** | `_deploy_ddl()`, `_is_healable_error()`, `_cortex_auto_heal()` |
| 1700–2000 | **Reusable Conversion Tab** | `render_conversion_tab()` — shared by Tables, Views, Functions, Procedures tabs |
| 2000–2070 | **Tab Definitions** | 8 tabs: Tables, Views, Functions, Procedures, SP → dbt, Execute, Dashboard |
| 2070–2400 | **SP → dbt Tab** | Upload SP, generate medallion models, download ZIP |
| 2400–3100 | **Execute Tab** | Upload to stage, deploy project, dbt run/test/docs |
| 3200–4200 | **Dashboard Tab** | 9 panels: Models, Run History, Tests, Lineage, File Explorer, Trends, Performance, KB, Reset |
| 4200–4272 | **Reset System** | Full reset: session state + tables + schemas + stages + tasks |

---

## 7. AI Models & Prompts

### 7.1 Models Used

| Model | Snowflake Call | Used For | Cost Profile |
|---|---|---|---|
| `claude-opus-4-5` | `SNOWFLAKE.CORTEX.COMPLETE('claude-opus-4-5', ...)` | All 6 conversion tasks + Auto-Heal | Higher per-token, high fidelity |
| `mistral-large2` | `SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', ...)` | KB fix summarization only | Lower per-token, sufficient for diffs |

### 7.2 Cortex Call Map (per object lifecycle)

```
┌─────────────────────────────────────────────────────────────────┐
│  CONVERSION (claude-opus-4-5)                                    │
│  ┌─────────────────┐ ┌─────────────────┐ ┌──────────────────┐  │
│  │ DDL → Snowflake │ │ SP → Snowflake  │ │ UDF → Snowflake  │  │
│  │ convert_ddl_to_ │ │ convert_sp_to_  │ │ convert_udf_to_  │  │
│  │ snowflake()     │ │ snowflake()     │ │ snowflake()      │  │
│  └─────────────────┘ └─────────────────┘ └──────────────────┘  │
│  ┌─────────────────┐ ┌─────────────────┐                       │
│  │ SP → dbt Models │ │ Auto-Heal (×3)   │                      │
│  │ convert_sp_to_  │ │ _cortex_auto_    │                      │
│  │ dbt_medallion() │ │ heal()           │                      │
│  └─────────────────┘ └──────────────────┘                      │
├─────────────────────────────────────────────────────────────────┤
│  SUMMARIZATION (mistral-large2)                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ _save_heal_knowledge() — before/after DDL diff → bullets   │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### 7.3 Prompt Architecture

Every AI conversion prompt follows this structure:

```
┌──────────────────────────────────┐
│ CONVERSION_RULES_NAMING          │  Uppercase, strip brackets, CamelCase
│ CONVERSION_RULES_TYPES           │  SQL Server → Snowflake type maps
│ CONVERSION_RULES_FUNCTIONS       │  GETDATE→CURRENT_TIMESTAMP, etc.
│ CONVERSION_RULES_SYNTAX          │  Remove NOLOCK, GO, CLUSTERED, etc.
├──────────────────────────────────┤
│ KNOWN PITFALLS (from KB)         │  Top 15 error patterns auto-injected
│ _get_known_pitfalls()            │  from HEAL_KNOWLEDGE_BASE table
├──────────────────────────────────┤
│ OBJECT-SPECIFIC INSTRUCTIONS     │  SP: Python handler format
│                                  │  UDF: SQL UDF preferred, Python fallback
│                                  │  dbt: medallion JSON schema
├──────────────────────────────────┤
│ SOURCE CODE                      │  Original SQL/T-SQL to convert
└──────────────────────────────────┘
│
▼
SNOWFLAKE.CORTEX.COMPLETE('claude-opus-4-5', <prompt>)
│
▼
┌──────────────────────────────────┐
│ POST-PROCESSING                   │
│ • clean_sql_response() — strip markdown fences
│ • Ensure CREATE OR REPLACE       │
│ • Deduplicate OR REPLACE         │
│ • normalize_snowflake_identifiers()
└──────────────────────────────────┘
```

### 7.4 Prompt Examples

#### DDL Conversion Prompt (`convert_ddl_to_snowflake`)

```python
prompt = f"""Convert this {source_type} DDL to Snowflake SQL.
IMPORTANT: Always use CREATE OR REPLACE syntax.

{CONVERSION_RULES_ALL}
{pitfalls}                             # ← auto-injected from KB
Return ONLY the converted DDL, no explanations, no markdown:

{source_ddl}"""
```

#### Stored Procedure Prompt (`convert_sp_to_snowflake`)

```python
prompt = f"""Convert this {source_type} stored procedure to a Snowflake Python stored procedure.

TARGET FORMAT:
CREATE OR REPLACE PROCEDURE PROCEDURE_NAME(params)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS $$
def main(session, param1):
    ...
$$;

{CONVERSION_RULES_NAMING}
{CONVERSION_RULES_TYPES}
{CONVERSION_RULES_FUNCTIONS}
{pitfalls}
Return ONLY the procedure code, no explanations, no markdown:

{source_sp}"""
```

#### UDF Conversion Prompt (`convert_udf_to_snowflake`)

```python
prompt = f"""Convert this {source_type} User Defined Function (UDF) to a Snowflake UDF.

MANDATORY: OUTPUT SQL UDF BY DEFAULT. Only use Python UDF as last resort.

CLASSIFICATION RULES (strict):
- DECLARE + SET + RETURN with simple logic → SQL UDF
- IF/ELSE returning values → SQL UDF (convert to IFF() or CASE WHEN)
- String ops, Date ops, Math → SQL UDF
- Table-Valued Function → SQL UDF with RETURNS TABLE(...)
- ONLY use Python UDF for: WHILE loops, cursors, TRY/CATCH, dynamic SQL

{CONVERSION_RULES_NAMING}
{CONVERSION_RULES_TYPES}
{CONVERSION_RULES_FUNCTIONS}
{pitfalls}
Return ONLY the function DDL:

{source_udf}"""
```

#### SP → dbt Medallion Prompt (`convert_sp_to_dbt_medallion`)

```python
prompt = f"""TASK: Convert the attached SQL Server stored procedure to modular
dbt models. Break into multiple models following dbt best practices with
staging → intermediate → marts architecture.

REPLICATE EXACT SP LOGIC INTO DBT MODELS.

Source Database: {src_db}
Source Schema: {src_schema}

Return JSON:
{{
  "bronze_models": [{{"name": "stg_...", "sql": "..."}}],
  "silver_models": [{{"name": "int_...", "sql": "..."}}],
  "gold_models":   [{{"name": "dim_/fact_...", "sql": "..."}}],
  "sources": [{{"table_name": "...", "database": "...", "schema": "..."}}],
  "columns": {{...}}
}}

Stored Procedure: {source_sp}"""
```

#### Auto-Heal Prompt (`_cortex_auto_heal`)

```python
heal_prompt = f"""You are a Snowflake SQL expert. The following DDL failed to execute.

FAILED DDL:
{current_ddl}

SNOWFLAKE ERROR:
{error_msg}

Fix ONLY the specific error. Return ONLY the corrected DDL —
no explanation, no markdown fences, no commentary.
Keep CREATE OR REPLACE and all column definitions intact.
Use valid Snowflake syntax only."""
```

#### KB Summarization Prompt (`_save_heal_knowledge`)

```python
summarize_prompt = f"""Compare these two Snowflake DDLs and describe ONLY what
changed to fix the error. Be concise (2-3 bullet points max).

ERROR: {error_msg[:300]}

BEFORE (failed):
{original_ddl[:1500]}

AFTER (fixed):
{healed_ddl[:1500]}

Return ONLY the bullet points describing the fix, no preamble:"""
```

---

## 8. Conversion Rules Reference

### 8.1 Naming Rules

| Source | Target | Rule |
|---|---|---|
| `CamelCase` (e.g. `CreatedDate`) | `CREATEDDATE` | Uppercase, no separators |
| `[Created Date]` | `CREATEDDATE` | Strip brackets + spaces |
| `[dbo].[TableName]` | `TABLENAME` | Strip schema prefix |
| `Order_Details` | `ORDER_DETAILS` | Preserve underscores |
| `200A_PreviousYear` | `"200A_PREVIOUSYEAR"` | Double-quoted (digit-start) |

### 8.2 Data Type Mappings

| SQL Server | Snowflake |
|---|---|
| `VARCHAR(MAX)` / `NVARCHAR(MAX)` | `VARCHAR(16777216)` |
| `NVARCHAR(n)` | `VARCHAR(n)` |
| `BIT` | `BOOLEAN` |
| `MONEY` | `NUMBER(19,4)` |
| `DATETIME` / `DATETIME2` | `TIMESTAMP_NTZ` |
| `DATETIMEOFFSET` | `TIMESTAMP_TZ` |
| `UNIQUEIDENTIFIER` | `VARCHAR(36)` |
| `XML` | `VARIANT` |
| `IMAGE` / `VARBINARY(MAX)` | `BINARY` |
| `INT` / `INTEGER` | `NUMBER` |
| `BIGINT` | `NUMBER(19,0)` |
| `TINYINT` | `NUMBER(3,0)` |
| `REAL` | `FLOAT` |

### 8.3 Function Mappings

| SQL Server | Snowflake |
|---|---|
| `GETDATE()` | `CURRENT_TIMESTAMP()` |
| `ISNULL(a, b)` | `COALESCE(a, b)` |
| `CHARINDEX(sub, str)` | `POSITION(sub IN str)` |
| `LEN(str)` | `LENGTH(str)` |
| `TOP N` | `LIMIT N` |
| `NEWID()` | `UUID_STRING()` |
| `STRING_AGG(col, sep)` | `LISTAGG(col, sep)` |
| `CONVERT(type, expr)` | `CAST(expr AS type)` |

### 8.4 Syntax Rules

| Remove / Convert | Action |
|---|---|
| `IDENTITY(1,1)` | → `AUTOINCREMENT` |
| `CLUSTERED` / `NONCLUSTERED` | Remove |
| `WITH (NOLOCK)` | Remove |
| `SET NOCOUNT ON` | Remove |
| `FOREIGN KEY REFERENCES` | Remove |
| `#TempTable` | → CTE or `TEMPORARY TABLE` |
| `EXEC` / `EXECUTE` | → `CALL` |
| `GO` batch separators | Remove |
| `USE [database]` | Remove |
| `CREATE OR REPLACE` | Always enforced |

---

## 9. Auto-Heal System

### 9.1 How It Works

When a converted DDL fails during deployment, the app automatically:

1. **Classifies** the error as healable or non-healable
2. If healable: sends the failed DDL + error to `claude-opus-4-5` for correction
3. **Re-stamps** the fully qualified name (defense against LLM dropping `DB.SCHEMA.OBJECT`)
4. **Re-deploys** the corrected DDL
5. Repeats up to **3 attempts** if new errors arise
6. On success: stores the fix in `HEAL_KNOWLEDGE_BASE`

### 9.2 Error Classification

**Healable errors** (will attempt fix):
- SQL compilation errors (syntax, type mismatch)
- Object already exists errors
- Invalid identifier errors
- Default value type mismatches

**Non-healable errors** (skip immediately → no wasted Cortex calls):
- `insufficient privileges`
- `permission denied`
- `warehouse.*not.*found`
- `quota exceeded`

### 9.3 What Gets Auto-Fixed

| Error Type | Example | Fix Applied |
|---|---|---|
| Date default mismatch | `DATE DEFAULT '1900-01-01'` | `::DATE` cast added |
| FK reference failure | `REFERENCES DimClient` doesn't exist | FK removed |
| LLM syntax residue | Trailing comma, unexpected token | Corrected |
| Type size issues | `BINARY(8000)` invalid | Changed to `BINARY` |
| SQL Server residue | `ISNULL`, `TOP n`, brackets | Snowflake equivalents |
| Digit-starting name | `200A_PreviousYear` unquoted | Double-quoted |

---

## 10. Self-Learning Knowledge Base

### 10.1 The Automated 4-Phase Loop

```
PHASE 1 — PREVENT  (every conversion)
    _get_known_pitfalls() → queries HEAL_KNOWLEDGE_BASE
    → Returns top 15 patterns sorted by occurrence count
    → Injected into every LLM prompt as "KNOWN PITFALLS"
    → Cortex avoids these errors during initial conversion

PHASE 2 — HEAL  (during deploy, if error occurs)
    _cortex_auto_heal() fixes the error (claude-opus-4-5, up to 3 attempts)

PHASE 3 — LEARN  (after successful heal)
    _save_heal_knowledge() automatically:
    ├── Generalizes the error (strips query IDs, line numbers)
    ├── Calls mistral-large2 for a human-readable fix summary
    ├── Checks if pattern already exists:
    │   ├── EXISTS → INCREMENT occurrence_count
    │   └── NEW → INSERT with full details
    └── Stores before/after DDL for traceability

PHASE 4 — DISPLAY  (Dashboard Panel 8)
    Shows all learned patterns with KPIs and fix descriptions
```

### 10.2 Error Generalization

Raw errors are made reusable by stripping specific values:

```
Before: (1304): 01c3289f-0108-2621... syntax error line 11 at position 4
After:  (<code>): <query_id>: ... syntax error line <N> at position <N>
```

---

## 11. Tab-by-Tab Usage Guide

### Tab 1–4: Tables / Views / Functions / Procedures

All four share the reusable `render_conversion_tab()` function.

**Workflow:**

1. **Select Source Platform** — SQL Server, Oracle, MySQL, PostgreSQL
2. **Upload files** — `.sql`, `.txt`, `.zip` (zip auto-extracted, skips `__MACOSX`)
3. **Click "Convert All"** — Each file:
   - Strips BOM, splits on `GO`
   - Injects known pitfalls from KB
   - Calls `SNOWFLAKE.CORTEX.COMPLETE('claude-opus-4-5', ...)`
   - Cleans response, normalizes identifiers
   - Saves to `CONVERTED_OBJECTS` with duration
   - Shows progress bar per file
4. **Review** — Side-by-side: converted DDL + source preview
5. **Select Target DB/Schema** → Click **"Deploy All"**
   - On error: Auto-Heal activates (up to 3 retries)
   - Success badge shows ✅; healed objects show ⚡

### Tab 5: SP → dbt Medallion

1. Enter **Project Name** (e.g., `address_migration`)
2. Select **Source Platform**, **Source Database**, **Source Schema**
3. Upload the stored procedure `.sql` file
4. Click **"Generate Models"**
5. Review models in **Bronze / Silver / Gold** sub-tabs
6. **Download as ZIP** — complete dbt project

**Materialization strategy:**
- Bronze (staging): always `VIEW`
- Silver (intermediate): `TABLE`, `VIEW`, or `EPHEMERAL`
- Gold (marts): `TABLE` or `INCREMENTAL`

### Tab 6: Execute

1. Verify **Active Stage** banner: `@DB.DBT_PROJECTS.DBT_PROJECT_STAGE`
2. Click **"Upload All Files to Stage"**
   - `.sql` files → always overwrite
   - `schema.yml` / `sources.yml` → smart merge with existing
   - `dbt_project.yml` / `profiles.yml` → skip if exists
3. Click **"Deploy Project"** → creates dbt project object
4. Click **"dbt run"** → executes models
5. Click **"dbt test"** → runs tests from `schema.yml`
6. Click **"dbt docs"** → generates and renders docs inline

### Tab 7: Dashboard

### Tab 8: RoadMap

See [Section 12](#12-dashboard-panels) for all 9 panels.

---

## 12. Dashboard Panels

| # | Panel | Data Source | What It Shows |
|---|---|---|---|
| 1 | Models Created & Deployed | `LIST @stage` + `INFORMATION_SCHEMA.TABLES` | Stage file counts + deployed object counts per layer |
| 2 | dbt Run History | `INFORMATION_SCHEMA.TASK_HISTORY` | Total runs, success/fail, avg duration, per-run detail + full log |
| 3 | Latest dbt Test Results | `INFORMATION_SCHEMA.TASK_HISTORY` | Pass/fail counts, test names, model names |
| 4 | Model Lineage | Stage files (persistent) | Graphviz DAG: source → bronze → silver → gold |
| 5 | Stage File Explorer | `LIST @stage` | Tree view with syntax-highlighted file viewer |
| 6 | Conversion Trends | `CONVERTED_OBJECTS` table | Bar chart by type + total/latest KPIs |
| 7 | Conversion Performance | `CONVERTED_OBJECTS` (DURATION_SECS) | Per-type stats: count, total, avg, min, max |
| 8 | Auto-Heal Knowledge Base | `HEAL_KNOWLEDGE_BASE` table | Learned patterns, occurrence counts, fix descriptions, before/after DDL |
| 9 | Reset App | Session state + all Snowflake objects | Full cleanup (see [Section 16](#16-reset-system)) |

---

## 13. Snowflake Objects Created at Runtime

The app auto-creates these objects — no manual DDL needed.

| Object | Fully Qualified Name | Created By | Purpose |
|---|---|---|---|
| `CONVERTED_OBJECTS` | `<db>.<schema>.CONVERTED_OBJECTS` | `init_conversion_table()` | Audit trail of all conversions |
| `HEAL_KNOWLEDGE_BASE` | `<db>.<schema>.HEAL_KNOWLEDGE_BASE` | `init_heal_kb()` | Auto-learned error patterns |
| `DBT_PROJECTS` schema | `<db>.DBT_PROJECTS` | `init_dbt_stage()` | Container for dbt infra |
| `DBT_PROJECT_STAGE` | `@<db>.DBT_PROJECTS.DBT_PROJECT_STAGE` | `init_dbt_stage()` | dbt project files |
| `DBT_DOCS_STAGE` | `@<db>.DBT_PROJECTS.DBT_DOCS_STAGE` | dbt docs flow | Docs HTML artifacts |
| `DBT_EXECUTION_TASK` | `<db>.DBT_PROJECTS.DBT_EXECUTION_TASK` | Execute tab | Runs dbt commands |
| `DBT_DOCS_FETCH_TASK` | `<db>.DBT_PROJECTS.DBT_DOCS_FETCH_TASK` | dbt docs | Fetches docs ZIP |
| `SP_FETCH_DBT_DOCS` | `<db>.DBT_PROJECTS.SP_FETCH_DBT_DOCS` | dbt docs | Python SP to extract docs |
| `SP_READ_STAGE_FILE` | `<db>.DBT_PROJECTS.SP_READ_STAGE_FILE` | dbt docs | Python SP to read stage files |
| `BRONZE` schema | `<db>.BRONZE` | `dbt run` | Bronze layer models |
| `SILVER` schema | `<db>.SILVER` | `dbt run` | Silver layer models |
| `GOLD` schema | `<db>.GOLD` | `dbt run` | Gold layer models |

### Table Schemas

**CONVERTED_OBJECTS:**

```sql
CREATE TABLE CONVERTED_OBJECTS (
    ID                NUMBER AUTOINCREMENT,
    CONVERSION_TYPE   VARCHAR(50),       -- TABLE_DDL, VIEW_DDL, FUNCTION, PROCEDURE, etc.
    SOURCE_NAME       VARCHAR(255),
    SOURCE_CODE       TEXT,
    TARGET_NAME       VARCHAR(255),
    TARGET_CODE       TEXT,
    STATUS            VARCHAR(50),       -- CONVERTED
    DURATION_SECS     FLOAT DEFAULT 0,
    CREATED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

**HEAL_KNOWLEDGE_BASE:**

```sql
CREATE TABLE HEAL_KNOWLEDGE_BASE (
    ID                NUMBER AUTOINCREMENT,
    ERROR_PATTERN     VARCHAR(500),      -- Generalized error text
    ERROR_CODE        VARCHAR(20),       -- Snowflake error code (e.g. 001003)
    SOURCE_PATTERN    TEXT,              -- Source DDL snippet
    FIX_DESCRIPTION   TEXT,              -- AI-generated fix summary
    ORIGINAL_DDL      TEXT,              -- DDL that failed
    HEALED_DDL        TEXT,              -- DDL that succeeded
    OBJECT_TYPE       VARCHAR(50),
    OCCURRENCE_COUNT  NUMBER DEFAULT 1,
    CREATED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    LAST_SEEN_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

---

## 14. Session State Reference

| Key | Type | Set By | Used By |
|---|---|---|---|
| `splash_dismissed` | `bool` | Splash screen | App load gate |
| `converted_objects` | `dict` | Conversion tabs | Dashboard |
| `deployed_models` | `dict` | Deploy handlers | Dashboard |
| `dbt_project_files` | `dict` | SP → dbt | Execute tab |
| `medallion_models` | `dict` | SP → dbt | Display |
| `current_project_name` | `str` | SP → dbt | Execute tab |
| `dbt_stage` | `str` | `init_dbt_stage()` | Execute + Dashboard |
| `dbt_stage_ready` | `bool` | `init_dbt_stage()` | Execute tab |
| `stage_upload_done` | `bool` | Upload handler | Execute buttons |
| `project_deployed` | `bool` | Deploy Project | Execute tab |
| `dbt_docs_html` | `str` | dbt docs | Docs viewer |
| `multi_conversions_{tab}` | `list` | Tabs 1–4 | Results display |
| `{state}_heal_results` | `list` | Deploy All | Heal display |
| `reset_timestamp` | `str` | Reset button | Task history filter |
| `source_database` | `str` | SP → dbt | dbt generation |
| `source_schema` | `str` | SP → dbt | dbt generation |

---

## 15. Caching Strategy

| Cache | TTL | What | Why |
|---|---|---|---|
| `get_databases()` | 300s | Database list | Rarely changes |
| `get_schemas()` | 300s | Schema list per DB | Rarely changes |
| `_list_stage_files()` | 60s | Files on stage | Updates after uploads |
| `_get_deployed_objects_from_stage()` | 60s | Deployed model counts | Updates after dbt run |
| `get_all_dbt_stages()` | 30s | Stage discovery | Quick refresh |
| `_load_lineage_from_stage()` | 60s | Lineage data | Updates after uploads |
| `_get_known_pitfalls()` | 120s | KB patterns for prompt injection | Updates after heals |

---

## 16. Reset System - For Testing Purpose

Dashboard Panel 9 provides a full "Reset Everything" button:

```
Step 1: Clear Session State
    All conversion results, heal results, dbt files, SSIS content, flags

Step 2: Truncate Tables
    TRUNCATE CONVERTED_OBJECTS
    TRUNCATE HEAL_KNOWLEDGE_BASE

Step 3: Drop Deployed Objects
    DROP SCHEMA IF EXISTS BRONZE CASCADE
    DROP SCHEMA IF EXISTS SILVER CASCADE
    DROP SCHEMA IF EXISTS GOLD CASCADE

Step 4: Drop Tasks and Procedures
    DROP TASK IF EXISTS DBT_EXECUTION_TASK
    DROP TASK IF EXISTS DBT_DOCS_FETCH_TASK
    DROP PROCEDURE IF EXISTS SP_FETCH_DBT_DOCS
    DROP PROCEDURE IF EXISTS SP_READ_STAGE_FILE
    DROP DBT PROJECT IF EXISTS (all)

Step 5: Purge Stage Files
    DROP + recreate DBT_PROJECT_STAGE (empty)
    DROP + recreate DBT_DOCS_STAGE (empty)

Step 6: Store reset_timestamp (filters out old task history)

Step 7: Clear caches + rerun
```

**Post-reset state:** All tables exist but are empty. Schemas recreated by next `dbt run`.

---

## 17. Skills & Agents Needed

### 17.1 Cortex Code (Development Agent)

**What it is:** Snowflake's AI-powered coding assistant used to build and iterate all features.

**How to use it:**

1. Open the Streamlit app in Snowsight code editor
2. Cortex Code appears as an inline assistant
3. Provide clear prompts referencing the app's patterns

**Key skills Cortex Code uses:**
- Snowflake SQL DDL/DML
- Python Snowpark API (`session.sql()`, `session.file.put_stream()`)
- Streamlit components (`st.tabs`, `st.columns`, `st.metric`, `st.expander`)
- Cortex AI integration (`SNOWFLAKE.CORTEX.COMPLETE`)
- YAML generation (dbt `schema.yml`, `sources.yml`)
- Regular expressions for SQL parsing
- Error handling patterns (try/except with `st.error`)

**Effective prompt patterns for Cortex Code:**

```
# Adding a new feature
"Add a new tab that converts Oracle PL/SQL packages to Snowflake.
Follow the same render_conversion_tab() pattern used by Tables tab.
Use the same theme colors (#7AB929 accent) and card layouts."

# Fixing a bug
"The 'merged' label shows incorrectly on first upload when no file exists
on stage yet. Fix the label logic in the upload handler to only show 'merged'
when read_file_from_stage() returns non-None content."

# Extending Auto-Heal
"Add a new healable pattern for SEQUENCE errors. Update _is_healable_error()
and add the pattern to HEALABLE_PATTERNS. Follow the existing regex style."
```

### 17.2 Knowledge File (AGENTS.md)

If using AGENTS.md as context for Cortex Code, include these sections:

```markdown
# AGENTS.md — CoCo Migration Studio

## Tech Stack
- Streamlit in Snowflake (single file: coco_streamlit.py)
- SNOWFLAKE.CORTEX.COMPLETE for AI
- get_active_session() for DB connection
- st.session_state for state management

## Key Patterns
- All SQL: session.sql("...").collect()
- All errors: try/except → st.error()
- All caching: @st.cache_data(ttl=N)
- Theme colors: #7AB929 (accent), #272727 (text), #F7F8F6 (bg)

## Tables
- CONVERTED_OBJECTS (audit trail)
- HEAL_KNOWLEDGE_BASE (learned patterns)

## Main Functions
- render_conversion_tab() → shared by tabs 1-4
- convert_ddl_to_snowflake() → DDL conversion
- convert_sp_to_snowflake() → SP conversion
- convert_udf_to_snowflake() → UDF conversion
- convert_sp_to_dbt_medallion() → dbt project generation
- _cortex_auto_heal() → self-correcting deploy
- _save_heal_knowledge() → KB learning
- _get_known_pitfalls() → prompt injection

## UI Conventions
- Section headers: <div class="section-header">
- KPI row: st.columns() + st.metric()
- Detail: st.expander()
- All buttons: type="primary"
```

### 17.3 Required Team Skills

| Skill | Where Needed |
|---|---|
| Snowflake SQL | Account setup, privilege grants, debugging deploys |
| Python (basic) | Understanding Snowpark patterns, extending conversion functions |
| Streamlit | UI customization, adding new tabs/panels |
| dbt fundamentals | Understanding medallion models, YAML configs, dbt run/test |
| T-SQL / SQL Server | Understanding source objects being migrated |
| Prompt engineering | Tuning conversion prompts, adding new conversion rules |
| Regex | Extending identifier normalization, error generalization |

---

## 18. Troubleshooting

### Common Issues

| Problem | Cause | Fix |
|---|---|---|
| `SNOWFLAKE.CORTEX.COMPLETE` fails | Cortex AI not enabled | Ask admin to enable Cortex on account |
| `claude-opus-4-5` model not found | Model not available in region | Check Snowflake region's model availability |
| `PermissionError` on deploy | Missing CREATE privileges | Grant CREATE TABLE/VIEW/PROCEDURE to role |
| `Stage not found` | `init_dbt_stage()` failed | Check CREATE STAGE + CREATE SCHEMA privileges |
| `Task not starting` | Missing EXECUTE TASK | `GRANT EXECUTE TASK ON ACCOUNT TO ROLE <ROLE>` |
| Slow conversions | Large SP body | SP body is truncated to 6000 chars for prompts |
| dbt run fails | Wrong source DB/schema | Verify source database and schema in SP → dbt tab |
| Reset doesn't clear task history | By design | Task history uses timestamp filter, not delete |
| Heal KB not injecting | Cache TTL | Wait 120s or clear caches in Dashboard |

### Verification Queries

```sql
-- Check conversion audit trail
SELECT CONVERSION_TYPE, COUNT(*), AVG(DURATION_SECS)
FROM CONVERTED_OBJECTS
GROUP BY CONVERSION_TYPE;

-- Check learned heal patterns
SELECT ERROR_PATTERN, OCCURRENCE_COUNT, FIX_DESCRIPTION
FROM HEAL_KNOWLEDGE_BASE
ORDER BY OCCURRENCE_COUNT DESC;

-- Check stage files
LIST @MIGRATION_DB.DBT_PROJECTS.DBT_PROJECT_STAGE;

-- Check deployed models
SELECT TABLE_NAME, TABLE_TYPE
FROM MIGRATION_DB.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA IN ('BRONZE', 'SILVER', 'GOLD');
```

---

## 19. Known Limitations

1. **SQL Server dialect coupling** — Conversion rules are hardcoded for T-SQL. Oracle/PostgreSQL support requires updating 5 conversion functions.
2. **LLM prompt size** — SP body truncated to 6000 chars. Very large procedures may lose logic.
3. **Single file** — Entire app is one Python file (~4200+ lines). Modularization requires Streamlit-in-Snowflake multi-file support.
4. **Session state is ephemeral** — Per-user, per-session. Mitigated by stage-persistent lineage and table-persistent KB.
5. **Top 15 KB entries** — Only top 15 pitfalls injected per prompt to avoid prompt bloat.
6. **Task history retention** — `INFORMATION_SCHEMA.TASK_HISTORY` retains records for dropped tasks; reset uses timestamp filtering.
7. **Trial account limits** — Task scheduling, execution history, and AI model quotas may be restricted.

---

## Quick Start Checklist

```
□  Snowflake Enterprise+ account with Cortex AI enabled
□  Warehouse created (X-SMALL+)
□  Role has CREATE TABLE/STAGE/SCHEMA/TASK + EXECUTE TASK
□  Verify: SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-opus-4-5', 'hello')
□  Create Streamlit app in Snowsight
□  Paste full coco_streamlit.py into editor
□  Add pyyaml package
□  Click Run → see splash screen → click "Get Started"
□  Upload a .sql file in Tables tab → click "Convert All"
□  Select target DB/Schema → click "Deploy All"
□  Check Dashboard tab for metrics
```

---

*Generated for Cortex-Orchestrated Data Automation MVP — kipi.ai*

