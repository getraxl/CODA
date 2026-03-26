# Skill: SQL Server → Snowflake dbt Migration App

## Purpose
This Streamlit-in-Snowflake app automates migration of SQL Server objects (DDLs, stored procedures, UDFs, SSIS packages) to Snowflake and dbt projects following a bronze/silver/gold medallion architecture.

## Architecture

### Tech Stack
- **Frontend**: Streamlit in Snowflake (single file: `streamlit_app.py`, ~3200 lines)
- **LLM Engine**: `SNOWFLAKE.CORTEX.COMPLETE('claude-opus-4-5', ...)` for all conversions
- **Session**: `snowflake.snowpark.context.get_active_session()`
- **State**: `st.session_state` for converted objects, dbt project files, deployed models
- **Persistence**: `CONVERTED_OBJECTS` table in current schema for audit trail
- **Dependencies**: `streamlit`, `snowflake-snowpark-python`, `pyyaml` (no external packages)

### 8-Tab Layout
| Tab | Purpose | Key Functions |
|-----|---------|---------------|
| 1. Tables | Browse & convert DDLs | `convert_ddl_to_snowflake()` |
| 2. Views | Browse & convert view DDLs | `convert_ddl_to_snowflake()` |
| 3. Functions | Browse & convert UDFs | `convert_udf_to_snowflake()` |
| 4. Procedures | Browse & convert SPs | `convert_sp_to_snowflake()` |
| 5. SP → dbt | Convert SP to full dbt project | `convert_sp_to_dbt_medallion()` |
| 6. Execute | Deploy dbt project to stage, run/test/docs | `generate_dbt_project_files()`, `_run_dbt_task()` |
| 7. SSIS | Convert SSIS .dtsx packages | `convert_ssis_to_snowflake()` |
| 8. Dashboard | Run history, lineage, test results, file explorer | Multiple panels |


## Core Functions

### Conversion Functions (LLM-powered)
| Function | Line | Input | Output |
|----------|------|-------|--------|
| `convert_ddl_to_snowflake(ddl, source_type)` | :545 | DDL string | Snowflake DDL string |
| `convert_sp_to_snowflake(sp, source_type)` | :70 | SP body | Snowflake SP string |
| `convert_udf_to_snowflake(udf, source_type)` | :123 | UDF body | Snowflake UDF string |
| `convert_sp_to_dbt_medallion(sp, sp_name, ...)` | :567 | SP body + metadata | JSON with bronze/silver/gold models |
| `convert_ssis_to_snowflake(xml)` | :727 | SSIS XML | JSON with tasks/streams/procedures |

### Project Generation
| Function | Line | Purpose |
|----------|------|---------|
| `generate_dbt_project_files(name, models, ...)` | :750 | Produces dict of file_path → content for full dbt project |
| `merge_dbt_project_files(new_files)` | :910 | Merges new conversion into cumulative project (deduplicates schema.yml, sources.yml) |

### Auto-Heal Functions (Deploy Self-Correction)
| Function | Purpose |
|----------|---------|
| `_is_healable_error(error_msg)` | Returns `True` if the error is a SQL compilation/syntax issue that Cortex can fix; `False` for permission/infra errors |
| `_cortex_auto_heal(ddl, error_msg, obj_type, obj_name, target_db, target_schema)` | Sends failed DDL + error to Cortex AI, retries deploy up to 3 times, returns `(success, final_ddl, attempts)` |

### Utilities
| Function | Line | Purpose |
|----------|------|---------|
| `clean_sql_response(response)` | :34 | Strips markdown code fences from LLM output |
| `extract_main_ddl(sql_content)` | :47 | Extracts CREATE statement, splits on `GO` batch separator |
| `save_conversion(...)` | :536 | Inserts conversion record into audit table (supports heal metadata) |
| `init_conversion_table()` | :520 | Creates `CONVERTED_OBJECTS` table if not exists (includes WAS_HEALED, HEAL_ATTEMPTS, ORIGINAL_DDL columns) |
| `_extract_sql_from_zip(zip_bytes)` | — | Extracts `.sql`/`.txt` files from a zip archive, returns list of `{name, content}` dicts. Skips `__MACOSX` and directory entries. |
| `_sanitize_sp_for_prompt(sp_body, source_db)` | — | Neutralizes SQLCMD `$()` variables and escapes backslashes before LLM prompt interpolation |
| `_robust_json_parse(raw_text)` | — | 4-level JSON parsing fallback: direct parse, newline escape, single-to-double quotes, `ast.literal_eval` |
| `_extract_sp_references(sp_body)` | — | Regex-based extraction of table/function/procedure references from SP body |
| `validate_sp_dependencies(sp_body, db, schema)` | — | Checks all SP references exist in Snowflake; returns `{valid, missing, refs}` |

## dbt Project Structure Generated
```
models/
  staging/
    sources.yml          # source() definitions pointing to raw tables
  bronze/
    stg_<table>.sql      # One per source table, materialized as view
    schema.yml
  silver/
    int_<logic>.sql      # Business logic models, materialized as view
    schema.yml
  gold/
    <target_table>.sql   # Final output matching SP target table name
    schema.yml
macros/
  generate_schema_name.sql
dbt_project.yml
profiles.yml
```

## Deployment Flow (Tab 6: Execute)
1. User clicks **Deploy** → files are zipped and uploaded to `@<DB>.DBT_PROJECTS.DBT_PROJECT_STAGE`
2. A Snowflake **TASK** (`DBT_EXECUTION_TASK`) is created with `dbt build` command
3. **dbt run** / **dbt test** / **dbt docs** buttons execute the task with different commands
4. `_run_dbt_task(task_name, project, warehouse, command)` handles task execution + polling
5. dbt docs artifacts are fetched via `SYSTEM$LOCATE_DBT_ARTIFACTS()` and rendered inline

## Conventions & Patterns

### Code Style
- Dark theme CSS: background `#0d1117`, accent `#29b5e8`, success `#3fb950`, warning `#d29922`, error `#f85149`
- All UI sections use `section-header` div pattern with icon + title
- KPI metrics displayed via `st.columns()` + `st.metric()`
- Expandable detail sections via `st.expander()`
- Status badges via `<span class='status-badge status-{ok|error}'>` HTML

### Data Access Patterns
- All Snowflake queries via `session.sql(...)`.collect()`
- Caching: `@st.cache_data(ttl=300)` for metadata, `ttl=60` for dashboard queries, `ttl=30` for stage listings
- Object browsing: `SHOW TABLES/VIEWS/PROCEDURES/USER FUNCTIONS IN SCHEMA ...`
- Object DDL retrieval: `SELECT GET_DDL('{type}', '{fqn}')`
- File upload: `st.file_uploader` accepts `.sql`, `.txt`, `.zip` — zip files are auto-extracted via `_extract_sql_from_zip()`, only `.sql`/`.txt` entries are kept, `__MACOSX` artifacts are skipped

### LLM Prompt Patterns
- All prompts use `SNOWFLAKE.CORTEX.COMPLETE('claude-opus-4-5', '...')` via SQL
- Single quotes escaped via `.replace("'", "''")`
- **CRITICAL:** Always call `_sanitize_sp_for_prompt()` on SP/DDL body before prompt interpolation — neutralizes SQLCMD `$()` variables that corrupt escaping
- SP body truncated to 6000 chars, SSIS XML to 8000 chars
- JSON output parsing: use `_robust_json_parse()` (4-level fallback chain) — never bare `json.loads()`
- See "Known LLM Input/Output Pitfalls" section at end of this file for full details

### Session State Keys
| Key | Type | Purpose |
|-----|------|---------|
| `converted_objects` | dict | All conversions grouped by type |
| `dbt_project_files` | dict | Cumulative file_path → content for dbt project |
| `deployed_models` | dict | Models deployed to Snowflake by layer |
| `dbt_docs_html` | str | Rendered dbt docs HTML |
| `dbt_docs_cache` | dict | Version → HTML cache for docs viewer |
| `dbt_docs_stage` | str | Fully qualified stage for docs artifacts |
| `ssis_content` | str | Uploaded SSIS XML content |
| `converted_ssis` | dict | Converted SSIS objects |
| `{tab_key}_heal_results` | list[dict] | Per-object auto-heal results from last Deploy All (keys: name, status, attempts, final_ddl, error) |

## Known Dialect Coupling (SQL Server-specific)

These areas are hardcoded for SQL Server and need changes for Oracle/PostgreSQL support:

1. **`extract_main_ddl()` :47** — Splits on `\nGO\n` (T-SQL batch separator)
2. **`convert_sp_to_snowflake()` :70** — T-SQL function mappings (ISNULL, GETDATE, CHARINDEX)
3. **`convert_udf_to_snowflake()` :123** — SQL Server type mappings (VARCHAR(MAX), NVARCHAR, BIT, MONEY)
4. **`convert_sp_to_dbt_medallion()` :567** — Entire prompt assumes T-SQL: #TempTables, WHILE loops, TOP N, T-SQL SP syntax
5. **`_extract_object_name()` :1035** — Strips `[dbo].[Name]` bracket notation

### To Add Oracle Support
- Add PL/SQL batch separator (`/`)
- Map: NVL→COALESCE, SYSDATE→CURRENT_TIMESTAMP, DECODE→CASE, INSTR→POSITION, ROWNUM→ROW_NUMBER()
- Map types: VARCHAR2→VARCHAR, NUMBER→NUMBER, CLOB→VARCHAR, RAW→BINARY
- Handle: DECLARE...BEGIN...EXCEPTION...END, cursors, %TYPE/%ROWTYPE, GLOBAL TEMPORARY TABLEs

## Snowflake Objects Created by the App
| Object | Location | Purpose |
|--------|----------|---------|
| `CONVERTED_OBJECTS` table | Current schema | Audit trail of all conversions |
| `DBT_PROJECT_STAGE` stage | `<DB>.DBT_PROJECTS` | Stores dbt project files |
| `DBT_EXECUTION_TASK` task | `<DB>.DBT_PROJECTS` | Executes dbt commands |
| `DBT_DOCS_STAGE` stage | `<DB>.DBT_PROJECTS` | Stores dbt docs artifacts |
| `DBT_DOCS_FETCH_TASK` task | `<DB>.DBT_PROJECTS` | Fetches docs from artifact location |
| `SP_FETCH_DBT_DOCS` procedure | `<DB>.DBT_PROJECTS` | Python SP to extract docs from zip |
| `SP_READ_STAGE_FILE` procedure | `<DB>.DBT_PROJECTS` | Python SP to read stage files via SnowflakeFile |

## Dashboard Panels (Tab 8)
1. **Models Created & Deployed** — Counts models per layer from stage, cross-references with `INFORMATION_SCHEMA.TABLES`
2. **DBT Run History** — Last 7 days from `INFORMATION_SCHEMA.TASK_HISTORY`, with per-run detail panel
3. **Latest DBT Test Results** — Parses error messages for failed test/model names
4. **Model Lineage** — Graphviz DAG built by parsing `{{ ref() }}` and `{{ source() }}` from SQL files
5. **Stage File Explorer** — Recursive tree view of all files in `DBT_PROJECT_STAGE`

## Guidelines for Modifications
- When adding features, follow the existing tab pattern: section header → KPI metrics → detail content → divider
- All SQL must go through `session.sql()` with try/except
- Never use ACCOUNTADMIN in deployed app — use dedicated role
- Keep all LLM prompts requesting JSON output with regex-based parsing
- When adding a new source dialect, update: `extract_main_ddl()`, `convert_sp_to_snowflake()`, `convert_udf_to_snowflake()`, `convert_sp_to_dbt_medallion()`, and `_extract_object_name()`
- Test any dbt project changes by deploying to stage and running `dbt build` via the Execute tab

## ⚡ Cortex Auto-Heal (DDL Deploy Self-Correction)

### Overview
When deploying converted DDLs (Tables, Views, Functions, Stored Procedures) to Snowflake, if a SQL compilation error occurs, the app automatically sends the failed DDL and the error back to Cortex AI for self-correction — up to 3 attempts — without any human intervention. Users only see a subtle ⚡ badge for healed objects. Errors only surface when they truly cannot be fixed automatically.

### Scope
- **Tabs affected**: 1 (Tables), 2 (Views), 3 (Functions), 4 (Procedures)
- **Triggers**: Both "Deploy All" button and per-object "Deploy" buttons
- **NOT affected**: dbt project deploy (Tab 6), SSIS deploy (Tab 7)

### Auto-Heal Functions
| Function | Purpose |
|----------|---------|
| `_is_healable_error(error_msg)` | Classifies a Snowflake SQL error as healable vs non-healable using regex on error codes/patterns |
| `_cortex_auto_heal(ddl, error_msg, obj_type, obj_name, target_db, target_schema)` | Sends failed DDL + error to Cortex AI, retries deploy up to 3 times, returns `(success, final_ddl, attempts)` |

### Healable vs Non-Healable Error Classification

**Healable** (auto-fix attempted):
| Pattern | Example |
|---------|---------|
| SQL compilation error (syntax) | Unexpected token, trailing comma, missing keyword |
| Type mismatch in DEFAULT | `DATE DEFAULT '1900-01-01'` → needs `::DATE` cast |
| Invalid type/size | `BINARY(8000)` not valid in Snowflake |
| FK reference failure | `REFERENCES DimClient` — table doesn't exist yet |
| Unsupported SQL Server syntax residue | `ISNULL`, `TOP N`, `[brackets]`, `WITH (NOLOCK)` |
| Invalid identifier | Column/table name not recognized |
| Invalid expression | Bad function call, wrong argument count |

Regex patterns checked (case-insensitive):
- `SQL compilation error`
- `invalid identifier`
- `invalid expression`
- `unexpected`
- `syntax error`
- `does not exist or not authorized` (only for object references in DDL, not role/permission)
- `invalid value` / `invalid default`
- `not recognized` / `unsupported`
- Snowflake error codes: `001003`, `001007`, `002262`, `002043`, `000904`, `001044`

**Non-Healable** (fail immediately, no Cortex call):
| Pattern | Why |
|---------|-----|
| `Insufficient privileges` | Role/permission issue — Cortex can't fix RBAC |
| `permission denied` | Same as above |
| `warehouse` + `not` (missing warehouse) | Infrastructure issue |
| `ACCOUNTADMIN` errors | Role escalation — not a DDL problem |
| `quota exceeded` | Resource limits |
| `session does not have a current database` | Session config issue |
| Network/timeout errors | Infrastructure, not SQL |

### Auto-Heal Flow
```
_deploy_ddl() fails
       │
       ▼
_is_healable_error(error_msg)?
       │
  YES  │  NO → return failure immediately
       ▼
_cortex_auto_heal(ddl, error, obj_type, obj_name, db, schema)
       │
       ├── Attempt 1: Send DDL + error to Cortex → clean response → re-stamp FQ name → deploy
       │       │
       │   SUCCESS → return (True, healed_ddl, [attempt1])
       │       │
       │   FAIL → is new error healable?
       │              │
       │         YES  │  NO → return (False, ddl, [attempt1])
       │              ▼
       ├── Attempt 2: Send healed DDL + NEW error to Cortex → clean → re-stamp → deploy
       │       │
       │   SUCCESS / FAIL (same logic)
       │              ▼
       └── Attempt 3: Final attempt
               │
          SUCCESS → return (True, final_ddl, [attempt1, attempt2, attempt3])
          FAIL    → return (False, last_ddl, [all_attempts])
```

### Cortex Heal Prompt Pattern
```
The prompt sent to SNOWFLAKE.CORTEX.COMPLETE('claude-opus-4-5', ...) includes:
1. The failed Snowflake DDL (full text)
2. The exact Snowflake error message
3. Instruction to fix ONLY the specific error
4. Instruction to return ONLY the corrected DDL (no explanation, no markdown)
5. Reminder to keep CREATE OR REPLACE and all column definitions intact
```

**Critical rule**: After Cortex returns the healed DDL, the FQ name (`DB.SCHEMA.OBJECT`) is **always re-stamped** using the same `_deploy_ddl()` header-replacement logic. Never trust the LLM to preserve the fully qualified name.

### Deploy All Results UI
The deploy summary replaces the old flat markdown list with a structured panel:

**KPI bar:**
```
Deploy All Complete  ·  [22 deployed]  ·  [3 auto-healed]  ·  [1 failed]
Target: DB.SCHEMA
```

**Per-object rows:**
```
✅  ObjectName     →  DB.SCHEMA.OBJECTNAME                          (direct success)
✅  ObjectName     →  DB.SCHEMA.OBJECTNAME  ⚡ Fixed by Cortex      (auto-healed)
    └─ 🔍 What Cortex fixed  ▼                                      (expandable)
         • Attempt 1: <error message>
         [corrected DDL preview]
❌  ObjectName  —  <error message>                                   (non-healable / exhausted)
```

**Styling:**
- Success rows: green text `#3fb950`
- Auto-healed badge: `⚡` with accent color `#29b5e8`
- Failed rows: red text `#f85149`
- Heal detail expander uses existing `st.expander()` pattern

### Session State Keys (Auto-Heal)
| Key | Type | Purpose |
|-----|------|---------|
| `{state_key}_heal_results` | list[dict] | Per-object heal results for the last Deploy All run |

Each heal result dict:
```python
{
    "name":       str,     # Object name
    "status":     str,     # "deployed" | "healed" | "failed"
    "attempts":   list,    # List of {error, healed_ddl} per attempt
    "final_ddl":  str,     # The DDL that was actually deployed (original or healed)
    "error":      str,     # Final error message (only if status == "failed")
}
```

### Audit Trail
- **Healed DDLs** are saved to `CONVERTED_OBJECTS` table with status `DEPLOYED`
- The saved `TARGET_DDL` is the **healed version**, not the original broken one
- This ensures the audit table reflects what's actually running in Snowflake

### Design Constraints
- **Max 3 heal attempts** per object — prevents infinite loops and token waste
- **Non-healable errors skip Cortex entirely** — no wasted LLM calls on permission/infra issues
- **FQ name is always re-stamped** after heal — LLM may drop `DB.SCHEMA.` prefix
- **Heal prompt is small** — only the DDL + error, not a full re-conversion prompt
- **Both Deploy All and per-object Deploy** use the same heal logic
- **Pre-deploy FK stripping** (`_strip_unsupported_constraints`) still runs as first pass before any deploy attempt

## DDL Conversion Rules (SQL Server → Snowflake)

### Naming Conventions
| Source (SQL Server) | Target (Snowflake) | Rule |
|---------------------|-------------------|------|
| `CamelCase` identifiers (e.g. `CreatedDate`) | `CREATEDDATE` | Uppercase, no separators |
| Quoted names with spaces (e.g. `"Created Date"`) | `CREATEDDATE` | Remove spaces, uppercase |
| Bracket notation (e.g. `[Created Date]`) | `CREATEDDATE` | Strip brackets, remove spaces, uppercase |
| `[dbo].[TableName]` | `TABLENAME` | Strip schema prefix + brackets, uppercase |
| `CamelCase_Mixed` (e.g. `Order_Details`) | `ORDER_DETAILS` | Uppercase only, preserve underscores |

### Data Type Mappings
| SQL Server | Snowflake | Notes |
|------------|-----------|-------|
| `VARCHAR(MAX)` / `NVARCHAR(MAX)` | `VARCHAR(16777216)` | Snowflake max VARCHAR |
| `NVARCHAR(n)` | `VARCHAR(n)` | No Unicode-specific type in Snowflake |
| `NCHAR(n)` | `CHAR(n)` | |
| `NTEXT` / `TEXT` | `VARCHAR(16777216)` | Deprecated types → VARCHAR |
| `BIT` | `BOOLEAN` | |
| `MONEY` / `SMALLMONEY` | `NUMBER(19,4)` / `NUMBER(10,4)` | |
| `DATETIME` / `DATETIME2` | `TIMESTAMP_NTZ` | |
| `SMALLDATETIME` | `TIMESTAMP_NTZ` | |
| `DATETIMEOFFSET` | `TIMESTAMP_TZ` | |
| `IMAGE` / `VARBINARY(MAX)` | `BINARY` | |
| `UNIQUEIDENTIFIER` | `VARCHAR(36)` | GUID as string |
| Hash / encrypted / checksum / HASHKEY columns (SHA, MD5, HASHBYTES output) | `VARCHAR(16777216)` | Use max VARCHAR to prevent truncation |
| `HASHBYTES('SHA2_512', ...)` | `SHA2(... , 512)` stored in `VARCHAR(16777216)` | Snowflake equivalent |
| `CAST(... AS VARCHAR)` without explicit length | `VARCHAR(16777216)` | Prevent truncation errors |
| `XML` | `VARIANT` | |
| `TINYINT` | `NUMBER(3,0)` | |
| `INT` / `INTEGER` | `NUMBER` | |
| `INT(10` / `INTEGER(10)` | `NUMBER(10,0)` | |
| `NUMERIC(10)` | `NUMBER(10,0)` | |
| `BIGINT` | `NUMBER(19,0)` | |
| `REAL` | `FLOAT` | |

### Function Mappings
| SQL Server | Snowflake | Notes |
|------------|-----------|-------|
| `GETDATE()` | `CURRENT_TIMESTAMP()` | |
| `SYSDATETIME()` | `CURRENT_TIMESTAMP()` | |
| `ISNULL(a, b)` | `COALESCE(a, b)` / `IFNULL(a, b)` | |
| `CHARINDEX(sub, str)` | `POSITION(sub IN str)` | Argument order changes |
| `LEN(str)` | `LENGTH(str)` | |
| `DATALENGTH(str)` | `OCTET_LENGTH(str)` | Byte-length |
| `TOP N` | `LIMIT N` | Move from SELECT to end |
| `NEWID()` | `UUID_STRING()` | |
| `ISNUMERIC(expr)` | `TRY_CAST(expr AS NUMBER) IS NOT NULL` | |
| `CONVERT(type, expr, style)` | `CAST(expr AS type)` / `TO_CHAR()` | Style codes need manual mapping |
| `DATEADD(part, n, date)` | `DATEADD(part, n, date)` | Same syntax, Snowflake supports it |
| `DATEDIFF(part, a, b)` | `DATEDIFF(part, a, b)` | Same syntax |
| `STRING_AGG(col, sep)` | `LISTAGG(col, sep)` | |
| `FORMAT(date, fmt)` | `TO_CHAR(date, fmt)` | Format strings differ |

### Constraint & Syntax Rules
| SQL Server | Snowflake | Action |
|------------|-----------|--------|
| `IDENTITY(1,1)` | `AUTOINCREMENT` or `IDENTITY` | Supported natively |
| `CLUSTERED / NONCLUSTERED` index hints | Remove | Not applicable |
| `WITH (NOLOCK)` / table hints | Remove | Not applicable |
| `SET NOCOUNT ON` | Remove | T-SQL specific |
| `FOREIGN KEY REFERENCES` | Remove or keep as comment | Snowflake parses but doesn't enforce |
| `CHECK` constraints (cross-table) | Remove | Not enforced |
| `PRIMARY KEY` / `UNIQUE` / `NOT NULL` | Keep | Supported |
| `DEFAULT GETDATE()` | `DEFAULT CURRENT_TIMESTAMP()` | Map function |
| `PRINT` statements | Remove | T-SQL debug, no equivalent |
| `BEGIN...END` blocks | Restructure | Use Snowflake Scripting or Snowpark |
| `#TempTable` / `##GlobalTemp` | `TEMPORARY TABLE` or CTE | No `#` prefix |
| `EXEC` / `EXECUTE` | `CALL` | For stored procedures |
| `@@ROWCOUNT` | Remove or use `SQLROWCOUNT` | In Snowflake Scripting |
| `@@IDENTITY` / `SCOPE_IDENTITY()` | Sequence or `AUTOINCREMENT` | Different pattern |

### Schema & Object Rules
| Rule | Description |
|------|-------------|
| Strip `[dbo].` prefix | SQL Server default schema not needed |
| Remove `USE [database]` | Not valid in DDL context |
| Remove `GO` batch separator | T-SQL only, split before conversion |
| `CREATE OR REPLACE` | Always use instead of `CREATE` for idempotency |
| Fully qualify objects | Use `DB.SCHEMA.OBJECT` format on deploy |

## UDF Conversion Rules (SQL Server → Snowflake)

### Classification: SQL UDF First (Mandatory)

**DEFAULT OUTPUT: SQL UDF.** Only use Python UDF when SQL is literally impossible.

| SQL Server Pattern | Snowflake Output | Rationale |
|-------------------|-----------------|-----------|
| Scalar function with CASE/WHEN | **SQL UDF** | Pure expression, no procedural logic |
| DECLARE + SET + RETURN | **SQL UDF** | Flatten to single SQL expression using CASE/COALESCE/IFF |
| IF/ELSE with simple returns | **SQL UDF** | Convert to `IFF()` or `CASE WHEN` |
| String operations (LEN, SUBSTRING, CHARINDEX) | **SQL UDF** | All have Snowflake SQL equivalents |
| Date operations (DATEADD, DATEDIFF, GETDATE) | **SQL UDF** | Snowflake supports natively |
| Math operations | **SQL UDF** | Pure SQL |
| ISNULL/COALESCE chains | **SQL UDF** | Direct mapping |
| Multiple DECLARE + SET + nested IF | **SQL UDF** | Use nested CASE/IFF expressions |
| Table-Valued Function (RETURNS TABLE) | **SQL UDF** | Use `RETURNS TABLE(...)` with SQL body |
| WHILE loops, cursors | **Python UDF** | No SQL equivalent |
| TRY/CATCH blocks | **Python UDF** | Requires exception handling |
| Dynamic SQL (EXEC) | **Python UDF** | Cannot do in SQL UDF |
| External API calls | **Python UDF** | Requires Python libraries |

### Conversion Strategy

**Step 1: Flatten T-SQL to SQL expression**
```
-- SQL Server (procedural-looking but actually simple):
DECLARE @result VARCHAR(50)
IF @input = 1 SET @result = 'Active'
ELSE IF @input = 0 SET @result = 'Inactive'
ELSE SET @result = 'Unknown'
RETURN @result

-- Snowflake SQL UDF (correct conversion):
CREATE OR REPLACE FUNCTION STATUS_LABEL(INPUT NUMBER)
RETURNS VARCHAR
AS $$
    CASE WHEN INPUT = 1 THEN 'Active'
         WHEN INPUT = 0 THEN 'Inactive'
         ELSE 'Unknown'
    END
$$;
```

**Step 2: Only escalate to Python when SQL cannot express the logic**
```
-- Python UDF (only for loops, recursion, or external libs):
CREATE OR REPLACE FUNCTION COMPLEX_CALC(INPUT VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'calc'
AS $$
def calc(input):
    # Logic that genuinely requires iteration
    return result
$$;
```

### Rules for the LLM Prompt
1. **ALWAYS attempt SQL UDF first** — even if the source uses DECLARE/SET/IF/ELSE
2. Flatten multi-statement T-SQL to a single SQL expression using CASE/IFF/COALESCE
3. Only use Python UDF if the function has: WHILE loops, cursors, TRY/CATCH, dynamic SQL, or external dependencies
4. ALL identifiers MUST be UPPERCASE (same naming rules as DDL)
5. Apply the same data type and function mappings from DDL Conversion Rules
6. Remove `WITH SCHEMABINDING`, `[dbo].` prefixes
7. For Table-Valued Functions, use `RETURNS TABLE(col1 TYPE, col2 TYPE)` with SQL body

---

## Concrete Example: Before/After Agent Behavior

**Task:** "Add a conversion count chart to the Dashboard tab"

### WITHOUT `AGENTS.md` — Agent guesses blindly

```
❌ Agent's actions:
1. Creates a new file "dashboard_chart.py"          — WRONG: app is a single file
2. Uses `import plotly.express as px`                — WRONG: not in dependencies
3. Writes `conn = snowflake.connector.connect()`     — WRONG: must use session.sql()
4. Queries a table called "conversions"              — WRONG: table is CONVERTED_OBJECTS
5. Adds chart with white background                  — WRONG: app uses dark theme #0d1117
6. Puts the chart at the bottom of the file          — WRONG: must go inside `with tab8:` block
7. No error handling                                 — WRONG: all SQL needs try/except
8. No caching                                        — WRONG: dashboard queries use @st.cache_data(ttl=60)
```

**Result:** Broken app. Agent spent tokens on 3+ retry cycles.

### WITH `AGENTS.md` — Agent reads conventions and nails it first try

```
✅ Agent reads AGENTS.md and knows:
1. Single file: edit coco_converter.py               — from "Tech Stack" section
2. Only streamlit + snowpark + pyyaml available       — from "Dependencies" line
3. Use session.sql(...).collect() with try/except     — from "Data Access Patterns"
4. Table is CONVERTED_OBJECTS in current schema       — from "Snowflake Objects Created"
5. Dark theme: background #0d1117, accent #29b5e8    — from "Code Style"
6. Insert inside `with tab8:` after existing panels   — from "8-Tab Layout" + "Dashboard Panels"
7. Use @st.cache_data(ttl=60) for dashboard queries   — from "Caching" pattern
8. Follow: section header → KPI metrics → content     — from "Guidelines for Modifications"
```

**Agent produces (first attempt):**
```python
# Inside `with tab8:` block, after existing panels
st.divider()
st.markdown('<div class="section-header">📊 Conversion Trends</div>', unsafe_allow_html=True)

@st.cache_data(ttl=60)
def _get_conversion_counts():
    try:
        rows = session.sql("""
            SELECT CONVERSION_TYPE, COUNT(*) AS CNT
            FROM CONVERTED_OBJECTS
            GROUP BY CONVERSION_TYPE
            ORDER BY CNT DESC
        """).collect()
        return rows
    except Exception:
        return []

_counts = _get_conversion_counts()
if _counts:
    c1, c2, c3 = st.columns(3)
    for i, row in enumerate(_counts[:3]):
        [c1, c2, c3][i].metric(row["CONVERSION_TYPE"], row["CNT"])
    st.bar_chart({r["CONVERSION_TYPE"]: r["CNT"] for r in _counts})
else:
    st.info("No conversions recorded yet.")
```

**Result:** Works on first deploy. Matches app style, uses correct table, correct session pattern, correct caching, correct theme.

### Key Takeaway

| Aspect | Without AGENTS.md | With AGENTS.md |
|--------|-------------------|----------------|
| File structure | Created new file | Edited correct single file |
| Dependencies | Used unavailable library | Used built-in st.bar_chart |
| DB connection | Wrong connector | `session.sql().collect()` |
| Table name | Guessed wrong | `CONVERTED_OBJECTS` |
| Theme | Default white | Dark theme colors |
| Placement | Random location | Inside `with tab8:` |
| Error handling | None | try/except pattern |
| Caching | None | `@st.cache_data(ttl=60)` |
| Attempts to succeed | 3+ retries | 1 (first try) |

## Known LLM Input/Output Pitfalls & Mitigations

### Pitfall 1: SQLCMD Variables Corrupt Prompts

**Symptom:** `JSON Parse Error: Expecting property name enclosed in double quotes: line 1 column 2 (char 1)` when converting SPs that reference SQLCMD variables.

**Root Cause:** SQL Server SPs from SSDT projects contain SQLCMD variable syntax like `[$(MediaDMStaging)].[dbo].TableName` or `$(EnvironmentDB)`. When this is interpolated into the Cortex AI prompt via f-string and `.replace("'", "''")`:
1. The `$()` syntax creates escaping chaos in the SQL string sent to `SNOWFLAKE.CORTEX.COMPLETE()`
2. The LLM receives corrupted input and returns malformed JSON (single-quoted keys, broken structure, or plain text instead of JSON)
3. `json.loads()` fails because the response is not valid JSON

**Example of problematic SP patterns:**
```sql
-- These SQLCMD patterns WILL break the prompt:
FROM [$(MediaDMStaging)].[dbo].OrderDetailDelivery
INNER JOIN [$(StagingDB)].dbo.Product ON ...
WHERE col IN (SELECT * FROM $(ReportDB).dbo.Lookup)
```

**Mitigation (implemented):**
- `_sanitize_sp_for_prompt(sp_body, source_db)` — Pre-processes SP body BEFORE sending to LLM:
  - Replaces `[$(VariableName)]` with the source database name from the UI selector
  - Replaces `$(VariableName)` with the source database name from the UI selector
  - Escapes backslashes to prevent double-escaping
- Called at the top of `convert_sp_to_dbt_medallion()` before prompt construction

**When modifying conversion functions:** Always call `_sanitize_sp_for_prompt()` on any SP/DDL body before interpolating it into a Cortex AI prompt. Never send raw SP body containing `$()` patterns to the LLM.

### Pitfall 2: LLM Returns Non-Standard JSON

**Symptom:** `JSON Parse Error` even after SQLCMD sanitization — the LLM returns syntactically invalid JSON.

**Common LLM JSON failures:**
| LLM Output | Why It Fails | Fallback That Fixes It |
|---|---|---|
| `{'key': 'value'}` | Single quotes instead of double | Single-to-double quote regex |
| `{"key": "val",}` | Trailing comma before `}` or `]` | Trailing comma regex cleanup |
| `{"sql": "SELECT\nFROM"}` | Unescaped newlines inside JSON string values | `\n` to `\\n` replacement |
| `Here is the JSON: {"key":...}` | Preamble text before the JSON object | Regex `\{[\s\S]*\}` extraction |
| Python dict literal `{'key': True}` | Python booleans `True/False` vs JSON `true/false` | `ast.literal_eval` fallback |

**Mitigation (implemented):**
- `_robust_json_parse(raw_text)` — 4-level fallback chain:
  1. Direct `json.loads()` on extracted JSON block
  2. Newline/carriage-return escaping + retry
  3. Single-quote to double-quote regex conversion + retry
  4. Python `ast.literal_eval()` as last resort
- Returns `None` only if ALL 4 levels fail (then shows error + raw output for debugging)
- Called by `convert_sp_to_dbt_medallion()` instead of inline `json.loads()`

**When adding new LLM-to-JSON functions:** Always use `_robust_json_parse()` instead of bare `json.loads()`. Never assume the LLM will return perfectly formatted JSON.

### Pitfall 3: SP Body Exceeds Prompt Token Budget

**Current limits:**
- SP body: truncated to **6,000 chars** (`source_sp[:6000]`)
- SSIS XML: truncated to **8,000 chars** (`ssis_xml[:8000]`)

**Risk:** Large SPs (>6000 chars) lose trailing logic — MERGE statements, cleanup blocks, RETURN statements, and GRANT/PERMISSION blocks at the end get silently dropped. The LLM converts only what it sees.

**When modifying prompts:** If increasing the char limit, remember that the entire prompt (rules + SP body) must fit within the Cortex AI context window. The prompt template itself is ~2000 chars, so effective SP budget is `context_limit - 2000`.

## SP to dbt Dependency Validation

### Overview
Before converting an SP to dbt, the app validates that all referenced objects (tables, views, functions, procedures) exist in the specified Snowflake source database/schema. Missing dependencies block conversion with a detailed error — other SPs in the same batch continue independently.

### Functions
| Function | Purpose |
|----------|---------|
| `_extract_sp_references(sp_body)` | Parses SP body with regex to extract referenced table names, function calls, and EXEC'd procedures. Strips comments and string literals first. Filters out 100+ SQL keywords and built-in functions. Returns `{"tables": [...], "functions": [...], "procedures": [...]}` |
| `_check_objects_exist(names, db, schema, type)` | Runs `SHOW TABLES/VIEWS/FUNCTIONS/PROCEDURES IN SCHEMA` and returns names that don't exist |
| `validate_sp_dependencies(sp_body, db, schema)` | Orchestrates validation: checks tables (with VIEW fallback), functions, procedures. Returns `{"valid": bool, "missing": [{"name": str, "type": str}], "refs": {...}}` |

### Behavior
- **Tab 5 (SP to dbt)** supports multi-file + ZIP upload
- Each SP is validated independently — one blocked SP does NOT stop others
- Blocked SPs show red panel with expandable detail listing each missing object name + type
- Successfully validated SPs proceed to LLM conversion
- Summary line: `"3 converted (27 models) . 2 blocked (missing dependencies)"`

### Reference Extraction Patterns
| SQL Pattern | Extracted As |
|---|---|
| `FROM TableName`, `JOIN TableName` | TABLE |
| `INTO TableName`, `UPDATE TableName` | TABLE |
| `MERGE INTO TableName` | TABLE |
| `dbo.FunctionName(...)` | FUNCTION |
| `EXEC ProcedureName` / `EXECUTE ProcedureName` | PROCEDURE |

Temp tables (`#TempTable`), variables (`@var`), SQL keywords, and 100+ built-in functions (ISNULL, GETDATE, COALESCE, ROW_NUMBER, etc.) are excluded from validation.

## Auto-Heal Audit Trail (Semantic Drift Detection)

### Overview
When auto-heal modifies a DDL to fix a deployment error, the change is now tracked in the `CONVERTED_OBJECTS` audit table. The dashboard surfaces healed objects with a side-by-side diff so users can verify no semantic drift occurred.

### Schema Additions to CONVERTED_OBJECTS
| Column | Type | Default | Purpose |
|---|---|---|---|
| `WAS_HEALED` | BOOLEAN | FALSE | Flag indicating object was modified by auto-heal |
| `HEAL_ATTEMPTS` | NUMBER | 0 | Number of heal retry attempts used |
| `ORIGINAL_DDL` | TEXT | NULL | The DDL before auto-heal modified it (enables diff) |

### Behavior
- Direct deploys: `WAS_HEALED=FALSE`, `STATUS='CONVERTED'`
- Healed deploys: `WAS_HEALED=TRUE`, `STATUS='DEPLOYED_HEALED'`, `ORIGINAL_DDL` stores pre-heal DDL
- Dashboard Panel 1B: Amber warning banner when healed objects exist, per-object rows with attempt count, side-by-side diff expander (original vs healed DDL)
- Only appears when `SELECT ... WHERE WAS_HEALED = TRUE` returns rows

### Session State Keys (Validation)
| Key | Type | Purpose |
|-----|------|---------|
| `dbt_sp_validation_results` | list[dict] | Per-SP validation results |
| `dbt_sp_convert_results` | list[dict] | Per-SP conversion results (ok/blocked/error) |

## ⚡ dbt Auto-Heal (Model Run Self-Correction)

### Overview
When `dbt run` fails with model errors, the app automatically parses the error, reads the failed model SQL, sends it to Cortex AI for correction, and presents a side-by-side diff for user review and approval. Same as DDL auto-heal, dbt auto-heal requires explicit user approval before applying fixes.

### Scope
- **Tab affected**: 6 (Execute dbt) — triggered on `dbt run` failure
- **NOT auto-applied**: Fixes are queued for review, not applied automatically

### Functions
| Function | Purpose |
|----------|---------|
| `_parse_dbt_run_errors(error_msg)` | Parses dbt task error message to extract model name, file path, and error detail for each failed model |
| `_dbt_auto_heal_model(model_sql, error_msg, model_name)` | Sends failed model SQL + error to Cortex AI, returns corrected SQL preserving `{{ ref() }}` and `{{ source() }}` macros |

### Flow
```
dbt run FAILS
       │
       ▼
_parse_dbt_run_errors(error_msg)
       │
       ▼ (for each failed model)
Lookup model SQL from st.session_state.dbt_project_files
       │
       ▼
_dbt_auto_heal_model(sql, error, model_name)
       │
       ▼
Store suggestion in st.session_state["dbt_heal_suggestions"]
       │
       ▼
Auto-Heal Review Panel (user reviews)
       │
  APPROVE → update dbt_project_files[file] = fixed_sql
  REJECT  → remove suggestion from list
       │
       ▼
User re-deploys and runs dbt run again
```

### Review Panel UI
- Side-by-side diff: original SQL (left) vs Cortex-fixed SQL (right)
- Per-model error message shown
- Approve button applies fix to `dbt_project_files` in session state
- Reject button removes the suggestion
- After approving all fixes, user must re-Deploy and re-Run to apply

### Session State Keys (dbt Auto-Heal)
| Key | Type | Purpose |
|-----|------|---------|
| `dbt_heal_suggestions` | list[dict] | Pending heal suggestions from last failed dbt run |

Each suggestion dict:
```python
{
    "model":        str,   # Model name (e.g. "int_address_combined")
    "file":         str,   # File path in dbt_project_files (e.g. "models/silver/int_address_combined.sql")
    "error":        str,   # Snowflake error detail
    "original_sql": str,   # Original model SQL
    "fixed_sql":    str,   # Cortex-suggested fix
    "approved":     bool,  # User approval status
}
```

### Common Healable dbt Errors
| Error | Typical Fix |
|-------|-------------|
| String truncation (100078) | Widen `CAST(... AS VARCHAR(n))` to `VARCHAR(16777216)` |
| Type mismatch | Fix CAST expressions or column types |
| Missing column | Correct column reference or add alias |
| Ambiguous column | Add table alias qualification |
| Invalid function | Map to Snowflake equivalent |
