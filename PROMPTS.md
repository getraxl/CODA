# CODA — 7 Cortex AI Prompt Templates

> **2 LLM Models** · **7 Prompt Templates** · All executed via `SNOWFLAKE.CORTEX.COMPLETE()` inside Snowflake

---

## Prompt Index

| # | Prompt | Model | Function | Line | Purpose |
|---|--------|-------|----------|------|---------|
| ❶ | [DDL Conversion](#-ddl-conversion) | `claude-opus-4-5` | `convert_ddl_to_snowflake()` | ~1639 | Table/View DDL → Snowflake DDL |
| ❷ | [SP Conversion](#-sp-conversion) | `claude-opus-4-5` | `convert_sp_to_snowflake()` | ~190 | Stored Procedure → Snowflake Python SP |
| ❸ | [UDF Conversion](#-udf-conversion) | `claude-opus-4-5` | `convert_udf_to_snowflake()` | ~243 | UDF → SQL-first Snowflake UDF |
| ❹ | [SP → dbt Medallion](#-sp--dbt-medallion) | `claude-opus-4-5` | `convert_sp_to_dbt_medallion()` | ~2069 | SP → Bronze/Silver/Gold dbt models (JSON) |
| ❺ | [View → dbt Model](#-view--dbt-model) | `claude-opus-4-5` | `convert_view_to_dbt_model()` | ~1726 | View DDL → dbt gold-layer model with ref() |
| ❻ | [Auto-Heal](#-auto-heal) | `claude-opus-4-5` | `_cortex_auto_heal()` + `_dbt_auto_heal_model()` | ~2733, ~2791 | Fix failed DDL/dbt model SQL errors |
| ❼ | [Fix Summary](#-fix-summary) | `mistral-large2` | `_save_heal_knowledge()` | ~1553 | Summarize heal fix for Knowledge Base |

**Sub-function** (not counted): `_regenerate_single_model()` (~2282) uses `claude-opus-4-5` with layer-specific instructions.

---

## Common Injections

All conversion prompts (❶–❺) inject these dynamically:

### Known Pitfalls (`_get_known_pitfalls()`)
```
KNOWN PITFALLS (these errors have occurred before in past conversions — AVOID them):
- Pattern: SQL compilation error ... → Fix: ...
- Pattern: invalid identifier ... → Fix: ...
... (top 15 from HEAL_KNOWLEDGE_BASE, ordered by occurrence_count, TTL=120s)
```

### Conversion Rule Constants (lines 17–87)
Four constant blocks concatenated as `CONVERSION_RULES_ALL`:

**`CONVERSION_RULES_NAMING`**
```
NAMING RULES (MANDATORY):
- ALL column names, table names, and identifiers MUST be UPPERCASE
- Remove ALL spaces from identifiers: "Created Date" → CREATEDDATE
- Strip bracket notation: [Created Date] → CREATEDDATE
- Strip [dbo]. prefix: [dbo].[TableName] → TABLENAME
- Preserve underscores: Order_Details → ORDER_DETAILS
- CamelCase to UPPERCASE: CreatedDate → CREATEDDATE
```

**`CONVERSION_RULES_TYPES`**
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
- Hash / encrypted / checksum / HASHKEY columns → VARCHAR(16777216)
- When CAST-ing to VARCHAR without explicit length, always use VARCHAR(16777216)
- HASHBYTES('SHA2_512', ...) → SHA2(... , 512) → VARCHAR(16777216)
- XML → VARIANT
- TINYINT → NUMBER(3,0)
- INT / INTEGER → NUMBER
- INT(10) → NUMBER(10)
- NUMERIC(16,2) → NUMBER(16,2)
- BIGINT → NUMBER(19,0)
- REAL → FLOAT
- FLOAT → FLOAT (NOT for monetary columns)

IMPORTANT: For monetary / amount / currency columns, ALWAYS use NUMBER(19,4) — never FLOAT.
```

**`CONVERSION_RULES_FUNCTIONS`**
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

**`CONVERSION_RULES_SYNTAX`**
```
CONSTRAINT & SYNTAX RULES:
- IDENTITY(1,1) → AUTOINCREMENT
- Remove CLUSTERED / NONCLUSTERED index hints
- Remove WITH (NOLOCK) and all table hints
- Remove SET NOCOUNT ON / PRINT statements
- FOREIGN KEY REFERENCES → remove
- CHECK constraints (cross-table) → remove
- Keep PRIMARY KEY, UNIQUE, NOT NULL
- DEFAULT GETDATE() → DEFAULT CURRENT_TIMESTAMP()
- #TempTable / ##GlobalTemp → use CTE or TEMPORARY TABLE
- EXEC / EXECUTE → CALL
- @@ROWCOUNT → remove or use SQLROWCOUNT
- @@IDENTITY / SCOPE_IDENTITY() → use AUTOINCREMENT
- Strip [dbo]. prefix from all references
- Remove USE [database] statements / GO batch separators
```

### Cross-Reference Model Registry (`_format_registry_for_prompt()`)
Injected into ❹ and ❺ only:
```
ALREADY CONVERTED MODELS (use ref() instead of source() for these):
If a source table in this SP matches one of the following already-converted gold models,
use {{ ref('<model_name>') }} instead of {{ source('raw', '<TABLE>') }} and do NOT
create a staging model for it.
  - DIMADDRESS → already exists as dbt model: dim_address
  - PIPELINEDEAL → already exists as dbt model: pipeline_deal
  ...
```

---

## ❶ DDL Conversion

**Function:** `convert_ddl_to_snowflake(source_ddl, source_type="SQL Server")`
**Model:** `claude-opus-4-5`
**Used in:** Tab 1 (Tables), Tab 2 (Views — DDL mode)

### Prompt Template
```
Convert this {source_type} DDL to Snowflake SQL.
IMPORTANT: Always use CREATE OR REPLACE syntax.

{CONVERSION_RULES_ALL}
{pitfalls}
Return ONLY the converted DDL, no explanations, no markdown:

{source_ddl}
```

### Rules Injected
- `CONVERSION_RULES_ALL` (all 4 blocks: naming + types + functions + syntax)
- Known Pitfalls from `_get_known_pitfalls()`

### Post-Processing
1. `clean_sql_response()` — strips markdown fences
2. Regex ensures `CREATE OR REPLACE TABLE` / `CREATE OR REPLACE VIEW`
3. `normalize_snowflake_identifiers()` — uppercases all identifiers

---

## ❷ SP Conversion

**Function:** `convert_sp_to_snowflake(source_sp, source_type="SQL Server")`
**Model:** `claude-opus-4-5`
**Used in:** Tab 4 (Procedures)

### Prompt Template
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
    result = session.sql("SELECT ...").collect()
    value = result[0][0] if result else None
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
{pitfalls}

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

### Rules Injected
- `CONVERSION_RULES_NAMING`, `CONVERSION_RULES_TYPES`, `CONVERSION_RULES_FUNCTIONS`
- Known Pitfalls
- T-SQL → Python removal/conversion list

### Post-Processing
1. `clean_sql_response()`
2. Regex ensures `CREATE OR REPLACE PROCEDURE`
3. `normalize_snowflake_identifiers()`

---

## ❸ UDF Conversion

**Function:** `convert_udf_to_snowflake(source_udf, source_type="SQL Server")`
**Model:** `claude-opus-4-5`
**Used in:** Tab 3 (Functions)

### Prompt Template
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
2. Example: DECLARE @r; IF @x=1 SET @r='A' ELSE SET @r='B'; RETURN @r → CASE WHEN X=1 THEN 'A' ELSE 'B' END

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
{pitfalls}

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

### Rules Injected
- `CONVERSION_RULES_NAMING`, `CONVERSION_RULES_TYPES`, `CONVERSION_RULES_FUNCTIONS`
- Known Pitfalls
- SQL-first classification decision tree
- 7 additional strictness rules

### Post-Processing
1. `clean_sql_response()`
2. Regex ensures `CREATE OR REPLACE FUNCTION`
3. `normalize_snowflake_identifiers()`

---

## ❹ SP → dbt Medallion

**Function:** `convert_sp_to_dbt_medallion(source_sp, sp_name, project_name, source_database, source_schema, existing_models_registry)`
**Model:** `claude-opus-4-5`
**Used in:** Tab 5 (SP → dbt)

### Pre-Processing
1. `_sanitize_sp_for_prompt(source_sp, src_db)` — neutralizes SQLCMD `$()` variables
2. `fetch_schema_catalog()` — gets referenced tables from INFORMATION_SCHEMA
3. `GET_DDL('TABLE', ...)` — fetches source table DDLs for bronze model completeness

### Prompt Template (abbreviated — full prompt is ~200 lines)
```
[STRICT JSON OUTPUT ONLY - NO PREAMBLE - NO MARKDOWN]
TASK: Convert the attached SQL Server stored procedure to modular dbt models.
Break into multiple models following dbt best practices with staging → intermediate → marts.

RULES:
- REPLICATE EXACT SP LOGIC INTO DBT MODELS
- TARGET TABLE: The table being INSERT INTO / MERGE INTO. DO NOT create source/staging for it.
- USE FUNCTIONS with FQ: {src_db}.{src_schema}.<FUNCTION_NAME>
- All source objects exist in '{src_db}' and '{src_schema}'
- USE single quotes for string literals

{cross_reference_registry}    ← from _format_registry_for_prompt()

LAYER 1 - STAGING (stg_*.sql):
- One per SOURCE table (not target), materialized as VIEW
- SELECT ALL COLUMNS — 1:1 mirror of source table
- REFERENCE SOURCE TABLE DDLs to know exact columns
- Apply only: CAST, RENAME to UPPERCASE, COALESCE nulls

LAYER 2 - INTERMEDIATE (int_*.sql):
- One per business logic operation
- VIEW (simple) / TABLE (heavy joins, 3+ downstream) / EPHEMERAL (used once)
- Convert WHILE loops → CTEs with ROW_NUMBER()/window functions
- Convert temp tables → CTEs or separate models
- Convert UPDATE statements → SELECT with CASE/transformations

LAYER 3 - MARTS (fct_*.sql, dim_*.sql):
- TABLE (default for BI), VIEW (simple passthrough), INCREMENTAL (date-based inserts/merges)
- Gold model name = TARGET TABLE name from the SP

{CONVERSION_RULES_NAMING}
{CONVERSION_RULES_TYPES}
{CONVERSION_RULES_FUNCTIONS}

DBT MODEL NAMING (CRITICAL):
- ALL model names lowercase snake_case
- CamelCase → snake_case: DimAddress → dim_address
- Bronze: stg_<table>.sql, Silver: int_<logic>.sql, Gold: <target>.sql

Return ONLY JSON:
{
    "bronze_models": [{"name": "...", "description": "...", "sql": "..."}],
    "silver_models": [{"name": "...", "description": "...", "sql": "..."}],
    "gold_models": [{"name": "...", "description": "...", "sql": "..."}],
    "sources": [{"name": "...", "database": "...", "schema": "...", "description": "..."}],
    "columns": {"<model>": [{"name": "...", "description": "...", "tests": ["not_null"]}]}
}

Stored Procedure Name: {sp_name}

SOURCE TABLE DDLs:
{source_table_ddls}

Stored Procedure:
{source_sp}
```

### Rules Injected
- `CONVERSION_RULES_NAMING`, `CONVERSION_RULES_TYPES`, `CONVERSION_RULES_FUNCTIONS`
- Cross-Reference Model Registry (for `ref()` usage)
- Source table DDLs (fetched live via `GET_DDL()`)
- 15 numbered medallion architecture rules
- Quality checklist (WHILE loops, temp tables, UPDATE logic)

### Post-Processing
1. `_robust_json_parse()` — 4-level JSON fallback
2. `content` → `sql` key migration (if LLM used wrong key name)
3. `_normalize_model_names()` — lowercase snake_case + ref() updates

---

## ❺ View → dbt Model

**Function:** `convert_view_to_dbt_model(source_ddl, source_type="SQL Server")`
**Model:** `claude-opus-4-5`
**Used in:** Tab 2 (Views — dbt mode)

### Prompt Template
```
[STRICT JSON OUTPUT ONLY - NO PREAMBLE - NO MARKDOWN]
TASK: Convert this {source_type} view definition into a dbt gold-layer model.

RULES:
1. The output model should be materialized as 'view' in the 'gold' schema.
2. Convert all SQL Server syntax to Snowflake SQL (types, functions, identifiers).
3. ALL identifiers must be UPPERCASE.
4. The model name must be lowercase_snake_case derived from the view name.

{CONVERSION_RULES_NAMING}
{CONVERSION_RULES_TYPES}
{CONVERSION_RULES_FUNCTIONS}

{registry_prompt}    ← from _format_registry_for_prompt()

5. CRITICAL: If the view references a table that already exists as a dbt model,
   use {{ ref('<model_name>') }} instead of a direct table reference.
   For tables NOT in the list above, use {{ source('raw', '<TABLE_NAME>') }}.
6. Preserve all WHERE clauses, column aliases, and logic exactly.
7. Strip [dbo]. prefixes, bracket notation, and SQL Server-specific hints.

{pitfalls}

Return ONLY this exact JSON (no markdown, no backticks):
{
    "model_name": "<lowercase_snake_case_name>",
    "description": "<one-line description>",
    "sql": "{{ config(materialized='view', schema='gold') }}\n\nSELECT ...\nFROM {{ ref('...') }}",
    "base_tables": ["<table_name_1>", "<table_name_2>"]
}

View DDL:
{source_ddl}
```

### Rules Injected
- `CONVERSION_RULES_NAMING`, `CONVERSION_RULES_TYPES`, `CONVERSION_RULES_FUNCTIONS`
- Known Pitfalls
- Cross-Reference Model Registry

### Post-Processing
1. `_robust_json_parse()` — 4-level JSON fallback
2. Returns `{model_name, description, sql, base_tables}` or `None`

---

## ❻ Auto-Heal

Two sub-prompts under one category — both fix errors using Cortex AI.

### ❻a DDL Auto-Heal

**Function:** `_cortex_auto_heal(ddl, error_msg, obj_type, obj_name, target_db, target_schema, max_attempts=3)`
**Model:** `claude-opus-4-5`
**Used in:** Tabs 1–4 (Deploy All + per-object Deploy)

#### Prompt Template (per attempt)
```
You are a Snowflake SQL expert. The following DDL failed to execute on Snowflake.

FAILED DDL:
{current_ddl}

SNOWFLAKE ERROR:
{error_msg}

Fix ONLY the specific error. Return ONLY the corrected DDL — no explanation, no markdown fences, no commentary.
Keep CREATE OR REPLACE and all column definitions intact. Use valid Snowflake syntax only.
```

#### Flow
1. Check `_is_healable_error()` — skip non-healable errors (permissions, infra)
2. Loop up to `max_attempts` (1–3, configurable via slider)
3. Per attempt: send DDL+error → clean response → re-deploy via `_deploy_ddl()`
4. If deploy succeeds → return `(True, healed_ddl, attempts)`
5. If new error is healable → continue loop with updated DDL+error
6. If new error is non-healable → abort immediately

#### Human-in-the-Loop
- Healed DDL is **DROPped immediately** after successful heal
- Object set to `pending_review` — user sees original vs healed DDL
- **✅ Approve & Deploy** or **❌ Reject**
- On approve: saves to audit as `DEPLOYED_HEALED`, saves pattern to KB

---

### ❻b dbt Model Auto-Heal

**Function:** `_dbt_auto_heal_model(model_sql, error_msg, model_name)`
**Model:** `claude-opus-4-5`
**Used in:** Tab 6 (Execute — after `dbt run` failure)

#### Prompt Template
```
You are a Snowflake dbt SQL expert. The following dbt model failed during `dbt run`.

MODEL NAME: {model_name}

FAILED MODEL SQL:
{model_sql}

SNOWFLAKE ERROR:
{error_msg}

{pitfalls}

Fix ONLY the specific error. Common fixes:
- String truncation: widen VARCHAR casts (use VARCHAR or VARCHAR(16777216))
- Type mismatch: fix CAST expressions
- Missing columns: check source references
- Ambiguous columns: add table aliases

Return ONLY the corrected SQL — no explanation, no markdown fences, no commentary.
Keep all {{ ref() }} and {{ source() }} macros intact. Do not change model logic beyond fixing the error.
```

#### Flow
1. `_parse_dbt_run_errors()` extracts per-model errors from dbt output
2. For each failed model: call `_dbt_auto_heal_model()` → returns fixed SQL or None
3. **Preventive fix propagation:** VARCHAR widening applied to sibling models sharing healed columns
4. Heal suggestions stored in `st.session_state.dbt_heal_suggestions` → user reviews
5. **✅ Approve & Apply** each → saves to `dbt_project_files` + KB
6. **🚀 Apply & Re-run** → uploads fixes → redeploys → `dbt run --full-refresh`

---

## ❼ Fix Summary

**Function:** `_save_heal_knowledge(error_msg, original_ddl, healed_ddl, obj_type, attempts)`
**Model:** `mistral-large2` (lighter model for summarization)
**Used in:** After every successful auto-heal (DDL or dbt)

### Prompt Template
```
Compare these two Snowflake DDLs and describe ONLY what changed to fix the error.
Be concise (2-3 bullet points max).

ERROR: {error_msg[:300]}

BEFORE (failed):
{original_ddl[:1500]}

AFTER (fixed):
{healed_ddl[:1500]}

Return ONLY the bullet points describing the fix, no preamble:
```

### Post-Processing
1. Fix description stored as `FIX_DESCRIPTION` in `HEAL_KNOWLEDGE_BASE` table
2. Error pattern generalized via `_generalize_error()` (strips literals, query IDs, line numbers)
3. Error code extracted via `_extract_error_code()` (6-digit Snowflake codes)
4. Upserted into KB: if same pattern exists → increment `OCCURRENCE_COUNT`; else insert new row
5. Top 15 patterns queried by `_get_known_pitfalls()` (TTL=120s) and injected into all future conversion prompts

### Self-Learning Loop
```
Deploy fails → Auto-Heal fixes → Fix Summary describes change → KB stores pattern
                                                                        ↓
Future conversion → _get_known_pitfalls() injects pattern → LLM avoids error
```

---

## Sub-Function: Regenerate Single Model

**Function:** `_regenerate_single_model(model_name, layer, current_sql, source_db, source_schema)`
**Model:** `claude-opus-4-5`
**Used in:** Tab 5 per-model "Regenerate" button

### Layer-Specific Instructions

**Bronze:**
```
Regenerate this dbt STAGING model. It must be a COMPLETE 1:1 mirror of the source table.
- Materialized as VIEW in schema 'bronze'
- SELECT ALL columns from the source table — do NOT skip any
- Apply CAST to Snowflake types, UPPERCASE all identifiers
- Use {{ source('raw', '{TABLE_NAME}') }} as the FROM clause

SOURCE TABLE DDL (include EVERY column from this):
{table_ddl}    ← fetched via GET_DDL('TABLE', ...)
```

**Silver:**
```
Regenerate this dbt INTERMEDIATE model.
- Keep the existing logic and transformations intact
- Fix any syntax issues for Snowflake compatibility
- Use ref() for upstream models
- UPPERCASE all identifiers
```

**Gold:**
```
Regenerate this dbt GOLD/MART model.
- Keep the existing business logic intact
- Fix any syntax issues for Snowflake compatibility
- Use ref() for upstream models
- UPPERCASE all identifiers
```

### Wrapper Prompt
```
[STRICT SQL OUTPUT ONLY - NO MARKDOWN - NO EXPLANATION]
You are a Snowflake dbt expert. {layer_instructions}

Current model SQL (improve/fix this):
{current_sql}

{CONVERSION_RULES_NAMING}
{CONVERSION_RULES_TYPES}

Return ONLY the complete model SQL starting with {{ config(...) }}. No markdown fences, no explanation.
```
