# Skill: SQL Server → Snowflake dbt Migration App

## Purpose
This Streamlit-in-Snowflake app (CODA — **C**ortex-**O**rchestrated **D**ata **A**utomation) automates migration of SQL Server objects (DDLs, stored procedures, UDFs, views) to Snowflake and dbt projects following a bronze/silver/gold medallion architecture.

## Architecture

### Tech Stack
- **Frontend**: Streamlit in Snowflake (single file: `coco_streamlit.py`, ~6300 lines)
- **LLM Engine**: `SNOWFLAKE.CORTEX.COMPLETE('claude-opus-4-5', ...)` for 6 conversion/heal prompts; `SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', ...)` for fix summarization
- **Session**: `snowflake.snowpark.context.get_active_session()`
- **State**: `st.session_state` for converted objects, dbt project files, deployed models, heal suggestions
- **Persistence**: `CONVERTED_OBJECTS` table for audit trail; `HEAL_KNOWLEDGE_BASE` table for self-learning error patterns
- **Dependencies**: `streamlit`, `snowflake-snowpark-python`, `pyyaml` (no external packages)

### 8-Tab Layout
| Tab | Variable | Purpose | Key Functions |
|-----|----------|---------|---------------|
| 1. Tables | `tab1` | Browse & convert table DDLs | `render_conversion_tab()`, `convert_ddl_to_snowflake()` |
| 2. Views | `tab2` | Convert view DDLs or dbt models (dual mode) | `convert_ddl_to_snowflake()`, `convert_view_to_dbt_model()` |
| 3. Functions | `tab3` | Convert UDFs (SQL-first, Python fallback) | `render_conversion_tab()`, `convert_udf_to_snowflake()` |
| 4. Procedures | `tab4` | Convert SPs to Snowflake Python SPs | `render_conversion_tab()`, `convert_sp_to_snowflake()` |
| 5. SP → dbt | `tab5` | Convert SP to full dbt medallion project | `validate_sp_dependencies()`, `convert_sp_to_dbt_medallion()`, `generate_dbt_project_files()` |
| 6. Execute | `tab6` | Deploy dbt project to stage, run/test/docs | `_run_dbt_task()`, `_dbt_auto_heal_model()`, test data generator |
| 7. Dashboard | `tab8` | 9 live panels: models, run history, lineage, KB, reset | Multiple cached query functions |
| 8. Roadmap | `tab9` | Coming soon: SSIS, RAG chatbot, multi-platform, CI/CD | Static HTML cards |

**Note**: Tab variable naming skips `tab7` — goes from `tab6` to `tab8` (Dashboard) and `tab9` (Roadmap). There are 8 tabs total in the UI.


## 7 Cortex AI Calls (2 LLM Models, 7 Prompt Templates)

| # | Function | Model | Purpose |
|---|----------|-------|---------|
| ❶ | `convert_ddl_to_snowflake()` | `claude-opus-4-5` | DDL → Snowflake DDL (55+ rules + Known Pitfalls) |
| ❷ | `convert_sp_to_snowflake()` | `claude-opus-4-5` | SP → Snowflake Python SP (session.sql(), HANDLER='main') |
| ❸ | `convert_udf_to_snowflake()` | `claude-opus-4-5` | UDF → SQL-first decision tree (Python only as last resort) |
| ❹ | `convert_sp_to_dbt_medallion()` | `claude-opus-4-5` | SP → dbt medallion JSON (bronze/silver/gold models) |
| ❺ | `convert_view_to_dbt_model()` | `claude-opus-4-5` | View → dbt gold-layer model with ref() cross-references |
| ❻ | `_cortex_auto_heal()` + `_dbt_auto_heal_model()` | `claude-opus-4-5` | Auto-heal failed DDL & dbt models (up to 3 retries) |
| ❼ | `_save_heal_knowledge()` | `mistral-large2` | Summarizes heal fix for Knowledge Base (2-3 bullet points) |

**Sub-function** (not counted separately): `_regenerate_single_model()` uses `claude-opus-4-5` to regenerate a single dbt model — invoked from Tab 5 per-model "Regenerate" button.


## Core Functions

### Conversion Functions (LLM-powered)
| Function | Line | Input | Output |
|----------|------|-------|--------|
| `convert_ddl_to_snowflake(ddl, source_type)` | ~1639 | DDL string | Snowflake DDL string |
| `convert_sp_to_snowflake(sp, source_type)` | ~190 | SP body | Snowflake Python SP string |
| `convert_udf_to_snowflake(udf, source_type)` | ~243 | UDF body | Snowflake SQL/Python UDF string |
| `convert_sp_to_dbt_medallion(sp, sp_name, ...)` | ~2069 | SP body + metadata | JSON with bronze/silver/gold models |
| `convert_view_to_dbt_model(ddl, source_type)` | ~1726 | View DDL | JSON with model_name, sql, description, base_tables |
| `_regenerate_single_model(name, layer, sql, ...)` | ~2282 | Model name + current SQL | Regenerated model SQL |

### Project Generation
| Function | Line | Purpose |
|----------|------|---------|
| `generate_dbt_project_files(name, models, ...)` | ~2363 | Produces dict of file_path → content for full dbt project |
| `merge_dbt_project_files(new_files)` | ~2519 | Merges new conversion into cumulative project (deduplicates schema.yml, sources.yml) |
| `_merge_schema_yml(existing, new)` | ~2573 | Merges model blocks by name (new wins on collision) |
| `_merge_sources_yml(existing, new)` | ~2596 | Merges table entries under same source blocks |

### Auto-Heal Functions (Deploy Self-Correction)
| Function | Line | Purpose |
|----------|------|---------|
| `_is_healable_error(error_msg)` | ~2726 | Returns `True` if error is SQL compilation/syntax; `False` for permission/infra |
| `_cortex_auto_heal(ddl, error_msg, obj_type, obj_name, target_db, target_schema, max_attempts)` | ~2733 | Sends failed DDL + error to Cortex AI, retries deploy up to 3 times, returns `(success, final_ddl, attempts)` |
| `_dbt_auto_heal_model(model_sql, error_msg, model_name)` | ~2791 | Single LLM call to fix a failed dbt model SQL, returns corrected SQL or None |
| `_parse_dbt_run_errors(error_msg)` | ~2773 | Regex parses `error in model 'name'` patterns from dbt run output |

### Self-Learning Knowledge Base
| Function | Line | Purpose |
|----------|------|---------|
| `init_heal_kb()` | ~1523 | Creates `HEAL_KNOWLEDGE_BASE` table if not exists |
| `_generalize_error(error_msg)` | ~1541 | Anonymizes error (strips literals, line numbers, query IDs) for pattern dedup |
| `_extract_error_code(error_msg)` | ~1548 | Regex extracts 6-digit Snowflake error code |
| `_save_heal_knowledge(error_msg, original_ddl, healed_ddl, obj_type, attempts)` | ~1553 | Summarizes fix via `mistral-large2`, upserts pattern into KB table |
| `_get_known_pitfalls()` | ~1607 | `@st.cache_data(ttl=120)` — Fetches top 15 patterns from KB, formats for prompt injection |

### Cross-Reference Model Registry
| Function | Line | Purpose |
|----------|------|---------|
| `_build_model_registry()` | ~1669 | Scans session state + converted_objects for existing dbt model names → dict |
| `_format_registry_for_prompt(registry)` | ~1701 | Formats registry as LLM prompt section: "ALREADY CONVERTED MODELS (use ref()...)" |

### Dependency Validation (Tab 5)
| Function | Line | Purpose |
|----------|------|---------|
| `fetch_schema_catalog(source_db, source_schema)` | ~1822 | 2 INFORMATION_SCHEMA queries → `{tables: {}, functions: set(), error}` |
| `validate_sp_dependencies(sp_source, source_db, source_schema, catalog)` | ~1853 | Regex-based extraction of table/function/procedure refs, cross-ref against catalog, returns `{valid, found, missing, target}` |

### Utilities
| Function | Line | Purpose |
|----------|------|---------|
| `clean_sql_response(response)` | ~155 | Strips markdown code fences from LLM output |
| `extract_main_ddl(sql_content)` | ~167 | Extracts CREATE statement, splits on `GO` batch separator, strips BOM |
| `normalize_snowflake_identifiers(ddl)` | ~89 | Uppercases all identifiers, preserves body after `AS $$` |
| `_sanitize_sp_for_prompt(sp_body, source_db)` | ~1660 | Neutralizes SQLCMD `$()` variables before prompt interpolation |
| `_robust_json_parse(raw_text)` | ~1783 | 4-level JSON parsing fallback: direct → newline escape → single/double quotes → `ast.literal_eval` |
| `_normalize_model_names(models, source_sp)` | ~1988 | Normalizes dbt model names to lowercase snake_case, updates ref() calls |
| `_extract_sql_from_zip(zip_bytes)` | ~2666 | Extracts `.sql`/`.txt` files from a zip archive, skips `__MACOSX` |
| `_strip_unsupported_constraints(table_ddl)` | ~2682 | Removes FOREIGN KEY / CHECK constraint lines before deploy |
| `_extract_object_name(ddl, obj_type)` | ~2647 | Regex extract bare object name from DDL header |
| `_deploy_ddl(ddl, obj_type, obj_name, target_db, target_schema)` | ~2824 | Stamps FQ name, strips FK/CHECK for tables, executes via `session.sql()` |
| `save_conversion(...)` | ~1627 | Inserts conversion record into `CONVERTED_OBJECTS` audit table |
| `init_conversion_table()` | ~1359 | Creates `CONVERTED_OBJECTS` table if not exists (includes WAS_HEALED, HEAL_ATTEMPTS, ORIGINAL_DDL columns) |


## dbt Project Structure Generated
```
models/
  staging/
    sources.yml          # source() definitions pointing to raw tables
  bronze/
    stg_<table>.sql      # One per source table, materialized as view
    schema.yml
  silver/
    int_<logic>.sql      # Business logic models (view/table/ephemeral)
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
1. User clicks **Upload All Files to Stage** → smart diff upload (SQL overwrite, YAML merge, config skip-if-exists)
2. **Deploy Project** → `CREATE OR REPLACE DBT PROJECT ... FROM '@stage'`
3. **dbt run** / **dbt test** / **dbt docs** buttons execute via Snowflake TASK + polling
4. `_run_dbt_task(task_name, project, warehouse, args)` handles task creation, execution, and polling (120s max)
5. On `dbt run` failure + auto-heal ON → `_parse_dbt_run_errors()` → `_dbt_auto_heal_model()` per failed model → heal suggestions for review
6. **Cortex Test Data Generator** — creates synthetic INSERT statements via LLM in 3 modes: All Pass / All Fail / Mixed
7. dbt docs artifacts fetched via `SYSTEM$LOCATE_DBT_ARTIFACTS()` and rendered inline


## Conventions & Patterns

### Code Style (CSS Theme — Light)
- Background: `#FFFFFF` → `#F7F8F6` (gradient)
- Primary accent: `#7AB929` (Kipi green)
- Dark green: `#5C9A1E`
- Body text: `#374151`, headings: `#272727`
- Warning amber: `#B8860B` / `#D29922`
- Error red: `#DC2626`
- All UI sections use `section-header` div pattern with icon + title
- KPI metrics displayed via `st.columns()` + `st.metric()`
- Status badges: `.status-badge .status-success` / `.status-pending` / `.status-error`
- Toast notifications: `show_toast()` with animated slide-in (success/error/info/heal types)
- Loading: `show_skeleton()` shimmer animation during Cortex API calls
- Font: Inter (system fallback)

### Data Access Patterns
- All Snowflake queries via `session.sql(...)`.collect()`
- Caching: `@st.cache_data(ttl=300)` for metadata (databases/schemas), `ttl=120` for known pitfalls, `ttl=60` for dashboard/stage queries, `ttl=30` for stage listings
- Object DDL retrieval: `SELECT GET_DDL('{type}', '{fqn}')`
- File upload: `st.file_uploader` accepts `.sql`, `.txt`, `.zip` — zip files auto-extracted via `_extract_sql_from_zip()`, only `.sql`/`.txt` entries are kept, `__MACOSX` artifacts are skipped

### LLM Prompt Patterns
- All prompts use `SNOWFLAKE.CORTEX.COMPLETE('{model}', '...')` via SQL
- Single quotes escaped via `.replace("'", "''")`
- **CRITICAL:** Always call `_sanitize_sp_for_prompt()` on SP/DDL body before prompt interpolation — neutralizes SQLCMD `$()` variables that corrupt escaping
- JSON output parsing: always use `_robust_json_parse()` (4-level fallback chain) — never bare `json.loads()`
- Known Pitfalls injected into all conversion prompts via `_get_known_pitfalls()` (top 15, TTL=120s)
- Cross-reference model registry injected via `_format_registry_for_prompt()` (for SP→dbt and View→dbt)

### Conversion Rule Constants (lines 17–87)
| Constant | Purpose |
|----------|---------|
| `CONVERSION_RULES_NAMING` | Uppercase identifiers, strip brackets/[dbo]., preserve underscores |
| `CONVERSION_RULES_TYPES` | 20+ SQL Server → Snowflake type mappings |
| `CONVERSION_RULES_FUNCTIONS` | 13+ function mappings (GETDATE→CURRENT_TIMESTAMP, ISNULL→COALESCE, etc.) |
| `CONVERSION_RULES_SYNTAX` | Constraint/syntax rules (IDENTITY→AUTOINCREMENT, remove FK/CHECK, etc.) |
| `CONVERSION_RULES_ALL` | All 4 blocks concatenated |


## Session State Keys
| Key | Type | Purpose |
|-----|------|---------|
| `splash_dismissed` | bool | Whether splash screen was dismissed |
| `converted_objects` | dict | All conversions grouped by type |
| `dbt_project_files` | dict | Cumulative file_path → content for dbt project |
| `deployed_models` | dict | Models deployed to Snowflake by layer |
| `dbt_stage` | str | Fully qualified stage path |
| `dbt_stage_ready` | bool | Whether stage was provisioned |
| `dbt_stage_error` | str\|None | Stage provisioning error |
| `stage_upload_done` | bool | Whether files uploaded to stage |
| `_uploaded_file_fingerprint` | dict | Hash-per-file for change detection |
| `project_deployed` | bool | Whether dbt project deployed |
| `current_project_name` | str | Current dbt project name |
| `source_database` | str | Selected source database |
| `source_schema` | str | Selected source schema |
| `dbt_docs_html` | str | Rendered dbt docs HTML |
| `dbt_docs_cache` | dict | Version → HTML cache for docs viewer |
| `dbt_heal_suggestions` | list[dict] | Pending dbt model heal suggestions |
| `dbt_heal_rerun_result` | dict | Result of heal re-run `{state, error}` |
| `multi_conversions_{tab_key}` | list[dict] | Per-tab multi-file conversion results |
| `multi_conversions_{tab_key}_heal_results` | list[dict] | Per-object auto-heal results from Deploy All |
| `{tab_key}_ind_heal_{idx}` | dict | Individual object heal review data |
| `dbt_sp_results` | list[dict] | Per-SP validation + conversion results (Tab 5) |
| `dbt_batch_timing` | float | SP→dbt batch total time |
| `view_dbt_results` | list[dict] | View→dbt conversion results |
| `reset_timestamp` | str | ISO timestamp of last reset |


## Snowflake Objects Created by the App
| Object | Type | Location | Purpose |
|--------|------|----------|---------|
| `CONVERTED_OBJECTS` | TABLE | `{current_schema}` | Audit trail of all conversions (12 columns incl. heal metadata) |
| `HEAL_KNOWLEDGE_BASE` | TABLE | `{current_schema}` | Self-learned error patterns + fix descriptions |
| `DBT_PROJECTS` | SCHEMA | `{current_db}` | Schema for all dbt-related objects |
| `DBT_PROJECT_STAGE` | STAGE | `{db}.DBT_PROJECTS` | Stores dbt project files |
| `DBT_DOCS_STAGE` | STAGE | `{db}.{schema}` | Stores dbt docs artifacts (versioned) |
| `AUTO_MIGRATION_PROJECT` | DBT PROJECT | `{db}.{schema}` | `CREATE OR REPLACE DBT PROJECT ... FROM '@stage'` |
| `DBT_EXECUTION_TASK` | TASK | `{db}.{schema}` | Executes dbt run/test/docs commands |
| `DBT_DOCS_FETCH_TASK` | TASK | `{db}.{schema}` | Fetches docs from artifact location |
| `SP_FETCH_DBT_DOCS` | PROCEDURE | `{db}.{schema}` | Python SP to extract docs from dbt_artifacts.zip |
| `SP_READ_STAGE_FILE` | PROCEDURE | `{db}.{schema}` | Python SP to read stage files via SnowflakeFile |


## Dashboard Panels (Tab 7)
1. **Models Created & Deployed** — Counts models per layer from stage, cross-references with `INFORMATION_SCHEMA.TABLES`
2. **DBT Run History** — Last 7 days from `INFORMATION_SCHEMA.TASK_HISTORY`, with per-run detail panel (status, duration, query ID, logs, errors)
3. **Latest DBT Test Results** — Parses error messages for failed test/model names; test counts from schema.yml
4. **Model Lineage** — Graphviz DAG built by parsing `{{ ref() }}` and `{{ source() }}` from SQL files; clustered by Source/Bronze/Silver/Gold layers
5. **Stage File Explorer** — Recursive tree view of all files in `DBT_PROJECT_STAGE` with inline file viewer
6. **Conversion Trends** — From `CONVERTED_OBJECTS`, grouped by type with bar chart
7. **CoCo Conversion Performance** — Duration stats per conversion type (count, avg, min, max)
8. **Auto-Heal Knowledge Base** — Learned patterns table with occurrence counts and before/after DDL diffs
9. **Reset App** — Full cleanup (session state, audit table, stages, schemas); preserves `HEAL_KNOWLEDGE_BASE`


## Auto-Heal Error Classification

### Healable Patterns (regex, case-insensitive)
```
SQL compilation error, invalid identifier, invalid expression, unexpected,
syntax error, invalid value, invalid default, not recognized, unsupported,
unknown function, missing column, duplicate column,
001003, 001007, 002262, 002043, 000904, 001044
```

### Non-Healable Patterns (fail immediately, no Cortex call)
```
insufficient privileges, permission denied, access denied,
warehouse.*not.*found, no active warehouse, ACCOUNTADMIN,
quota exceeded, session does not have a current database,
network, timeout, authentication, role .* does not exist
```

### Auto-Heal Flow (DDL Deploy)
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
       │         YES  │  NO → return failure
       │              ▼
       ├── Attempt 2–3: Same flow with updated error/DDL
       │
       └── All attempts exhausted → return (False, last_ddl, all_attempts)
```

### Human-in-the-Loop Deploy
After auto-heal succeeds:
1. Healed DDL is **immediately DROPped** (not persisted)
2. Object set to `pending_review` status
3. User sees original DDL + healed DDL side-by-side + heal log
4. **✅ Approve & Deploy** → re-deploys healed DDL, saves to audit as `DEPLOYED_HEALED`, saves pattern to KB
5. **❌ Reject** → object marked "rejected", nothing deployed
6. Heal pattern saved to `HEAL_KNOWLEDGE_BASE` for future prevention

### Auto-Heal Flow (dbt run)
```
dbt run fails
       │
       ▼
_parse_dbt_run_errors(error_msg) → list of {model, file, error}
       │
       ▼
For each failed model:
  _dbt_auto_heal_model(model_sql, error, model_name) → fixed SQL
       │
       ▼
Preventive fix propagation: VARCHAR widening applied to sibling models
       │
       ▼
Heal suggestions stored in session_state → user reviews
       │
       ▼
✅ Approve & Apply each → saves to dbt_project_files + KB
🚀 Apply & Re-run → uploads fixes → redeploys → dbt run --full-refresh
```


## DDL Conversion Rules (SQL Server → Snowflake)

### Naming Conventions
| Source (SQL Server) | Target (Snowflake) | Rule |
|---------------------|-------------------|------|
| `CamelCase` identifiers | `CREATEDDATE` | Uppercase, no separators |
| Quoted `"Created Date"` | `CREATEDDATE` | Remove spaces, uppercase |
| Bracket `[Created Date]` | `CREATEDDATE` | Strip brackets, remove spaces, uppercase |
| `[dbo].[TableName]` | `TABLENAME` | Strip schema prefix + brackets, uppercase |
| `CamelCase_Mixed` | `ORDER_DETAILS` | Uppercase only, preserve underscores |

### Data Type Mappings
| SQL Server | Snowflake | Notes |
|------------|-----------|-------|
| `VARCHAR(MAX)` / `NVARCHAR(MAX)` | `VARCHAR(16777216)` | Snowflake max VARCHAR |
| `NVARCHAR(n)` | `VARCHAR(n)` | No Unicode-specific type |
| `NCHAR(n)` | `CHAR(n)` | |
| `NTEXT` / `TEXT` | `VARCHAR(16777216)` | Deprecated types → VARCHAR |
| `BIT` | `BOOLEAN` | |
| `MONEY` | `NUMBER(19,4)` | Never use FLOAT for monetary |
| `SMALLMONEY` | `NUMBER(10,4)` | |
| `DATETIME` / `DATETIME2` | `TIMESTAMP_NTZ` | |
| `SMALLDATETIME` | `TIMESTAMP_NTZ` | |
| `DATETIMEOFFSET` | `TIMESTAMP_TZ` | |
| `IMAGE` / `VARBINARY(MAX)` | `BINARY` | |
| `UNIQUEIDENTIFIER` | `VARCHAR(36)` | GUID as string |
| `XML` | `VARIANT` | |
| `TINYINT` | `NUMBER(3,0)` | |
| `INT` / `INTEGER` | `NUMBER` | |
| `INT(10)` | `NUMBER(10)` | |
| `BIGINT` | `NUMBER(19,0)` | |
| `REAL` | `FLOAT` | |
| Hash/HASHBYTES columns | `VARCHAR(16777216)` | SHA2(...,512) for HASHBYTES |

### Function Mappings
| SQL Server | Snowflake |
|------------|-----------|
| `GETDATE()` | `CURRENT_TIMESTAMP()` |
| `SYSDATETIME()` | `CURRENT_TIMESTAMP()` |
| `ISNULL(a, b)` | `COALESCE(a, b)` |
| `CHARINDEX(sub, str)` | `POSITION(sub IN str)` |
| `LEN(str)` | `LENGTH(str)` |
| `DATALENGTH(str)` | `OCTET_LENGTH(str)` |
| `TOP N` | `LIMIT N` (move to end) |
| `NEWID()` | `UUID_STRING()` |
| `ISNUMERIC(expr)` | `TRY_CAST(expr AS NUMBER) IS NOT NULL` |
| `CONVERT(type, expr)` | `CAST(expr AS type)` |
| `STRING_AGG(col, sep)` | `LISTAGG(col, sep)` |
| `FORMAT(date, fmt)` | `TO_CHAR(date, fmt)` |
| `DATEADD/DATEDIFF` | Same syntax (Snowflake supports it) |

### Constraint & Syntax Rules
| SQL Server | Action |
|------------|--------|
| `IDENTITY(1,1)` | → `AUTOINCREMENT` |
| `CLUSTERED / NONCLUSTERED` | Remove |
| `WITH (NOLOCK)` / table hints | Remove |
| `SET NOCOUNT ON` | Remove |
| `FOREIGN KEY REFERENCES` | Remove (Snowflake doesn't enforce) |
| `CHECK` constraints (cross-table) | Remove |
| `PRIMARY KEY` / `UNIQUE` / `NOT NULL` | Keep |
| `DEFAULT GETDATE()` | → `DEFAULT CURRENT_TIMESTAMP()` |
| `PRINT` statements | Remove |
| `#TempTable` / `##GlobalTemp` | → CTE or `TEMPORARY TABLE` |
| `EXEC` / `EXECUTE` | → `CALL` |
| `@@ROWCOUNT` / `@@IDENTITY` | Remove / use AUTOINCREMENT |
| `USE [database]` / `GO` | Remove |
| Always use `CREATE OR REPLACE` | For idempotency |
| Fully qualify on deploy | `DB.SCHEMA.OBJECT` format |


## UDF Conversion Rules (SQL Server → Snowflake)

### Classification: SQL UDF First (Mandatory)

| SQL Server Pattern | Snowflake Output | Rationale |
|-------------------|-----------------|-----------|
| DECLARE + SET + RETURN | **SQL UDF** | Flatten to CASE/COALESCE/IFF |
| IF/ELSE with simple returns | **SQL UDF** | Convert to IFF() or CASE WHEN |
| String/Date/Math operations | **SQL UDF** | All have SQL equivalents |
| Table-Valued Function | **SQL UDF** | Use `RETURNS TABLE(...)` |
| WHILE loops, cursors | **Python UDF** | No SQL equivalent |
| TRY/CATCH, Dynamic SQL | **Python UDF** | Requires Python |


## SP to dbt Dependency Validation

### Overview
Before converting an SP to dbt (Tab 5), the app validates that all referenced objects exist in the specified Snowflake source database/schema. Missing dependencies block conversion with detailed error — other SPs in the same batch continue independently.

### Validation Flow
1. `fetch_schema_catalog()` issues 2 queries (INFORMATION_SCHEMA.TABLES + FUNCTIONS) — cached per batch
2. `validate_sp_dependencies()` does regex-based extraction of FROM/JOIN/UPDATE/MERGE/EXEC references
3. Filters out 100+ SQL keywords, CTEs, temp tables, `@variables`, built-in functions
4. Cross-references against catalog (pure CPU, no SQL queries per reference)
5. Returns `{valid: bool, found: [], missing: [{name, type}], target: str}`

### Per-SP Behavior
- **Valid SPs** → proceed to LLM conversion
- **Blocked SPs** → red panel with validation dataframe (✅ Found / ❌ Missing / ⏭ Skipped)
- **Other SPs in batch continue independently** — one blocked SP does NOT stop the rest
- **Compression ratio warning** — shown if output lines < 20% of source lines


## Cross-Reference Model Registry

### Purpose
Views and SPs referencing already-converted tables should use `{{ ref() }}` instead of `{{ source() }}`.

### Functions
1. `_build_model_registry()` — Scans `dbt_project_files` + `converted_objects` for existing gold model names → dict mapping UPPERCASED SQL Server names to dbt model names
2. `_format_registry_for_prompt()` — Formats registry as LLM prompt section: "ALREADY CONVERTED MODELS (use ref() instead of source() for these):"

### Where Used
- **Tab 2 dbt mode** — Registry injected into `convert_view_to_dbt_model()` prompt
- **Tab 5 SP → dbt** — Registry injected into `convert_sp_to_dbt_medallion()` prompt; rebuilt after each SP so subsequent SPs see earlier gold models


## Known LLM Input/Output Pitfalls & Mitigations

### Pitfall 1: SQLCMD Variables Corrupt Prompts
**Root Cause:** SPs from SSDT projects contain `[$(MediaDMStaging)].[dbo].TableName` — the `$()` syntax corrupts the SQL string sent to `SNOWFLAKE.CORTEX.COMPLETE()`.
**Mitigation:** `_sanitize_sp_for_prompt(sp_body, source_db)` replaces `[$(var)]` and `$(var)` with the source database name before prompt interpolation.

### Pitfall 2: LLM Returns Non-Standard JSON
**Common failures:** Single quotes, trailing commas, unescaped newlines, preamble text, Python True/False
**Mitigation:** `_robust_json_parse(raw_text)` — 4-level fallback: direct → newline escape → single-to-double quotes → `ast.literal_eval`. Returns `None` only if all 4 fail.

### Pitfall 3: SP Body Exceeds Context Window
**Risk:** Large SPs lose trailing logic (MERGE, cleanup, RETURN blocks) after truncation.
**Mitigation:** Source table DDLs are fetched and appended to prompt for bronze model completeness. Prompt template itself is ~2000 chars.


## Cortex Test Data Generator (Tab 6)

### Overview
Generates synthetic INSERT statements for source tables via Cortex AI, with 3 modes:

| Mode | Behavior |
|------|----------|
| **All Pass ✅** | Data respects all dbt test constraints (not_null, unique, accepted_values) |
| **All Fail ❌** | Data deliberately violates constraints (nulls in NOT NULL, duplicates in UNIQUE) |
| **Mixed ⚠️** | Mix of valid and invalid data |

### Flow
1. Collects sources.yml, source table DDLs, schema.yml constraints, bronze model SQL
2. Sends to Cortex AI with mode instructions
3. For All Fail / Mixed: app auto-drops NOT NULL constraints before executing INSERTs
4. Executes each INSERT statement individually with progress bar


## Guidelines for Modifications
- When adding features, follow the existing tab pattern: section header → KPI metrics → detail content → divider
- All SQL must go through `session.sql()` with try/except
- Never use ACCOUNTADMIN in deployed app — use dedicated role
- Keep all LLM prompts requesting JSON output with `_robust_json_parse()` parsing
- All conversion prompts must inject `_get_known_pitfalls()` for self-learning
- Always call `_sanitize_sp_for_prompt()` on SP/DDL body before prompt interpolation
- Light theme: primary green `#7AB929`, background white/off-white, font Inter
- Tab variables: `tab1` through `tab6`, then `tab8`, `tab9` (no `tab7`)
