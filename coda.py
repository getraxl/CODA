import streamlit as st
from snowflake.snowpark.context import get_active_session
import json
import re
from datetime import datetime
import zipfile
import io
import os
import time
import yaml

session = get_active_session()

# ═══════════════════════════════════════════════════════════════════════════════
# CONVERSION RULES (from AGENTS.md — hardcoded for reliability)
# ═══════════════════════════════════════════════════════════════════════════════
CONVERSION_RULES_NAMING = """
NAMING RULES (MANDATORY):
- ALL column names, table names, and identifiers MUST be UPPERCASE
- Remove ALL spaces from identifiers: "Created Date" → CREATEDDATE
- Strip bracket notation: [Created Date] → CREATEDDATE
- Strip [dbo]. prefix: [dbo].[TableName] → TABLENAME
- Preserve underscores: Order_Details → ORDER_DETAILS
- CamelCase to UPPERCASE: CreatedDate → CREATEDDATE
"""

CONVERSION_RULES_TYPES = """
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
- Hash / encrypted / checksum / HASHKEY columns (SHA, MD5, HASHBYTES output) → VARCHAR(16777216)
- When CAST-ing to VARCHAR without explicit length, always use VARCHAR(16777216) to avoid truncation
- HASHBYTES('SHA2_512', ...) → SHA2(... , 512) and store result in VARCHAR(16777216)
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
"""

CONVERSION_RULES_FUNCTIONS = """
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
"""

CONVERSION_RULES_SYNTAX = """
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
"""

CONVERSION_RULES_ALL = CONVERSION_RULES_NAMING + CONVERSION_RULES_TYPES + CONVERSION_RULES_FUNCTIONS + CONVERSION_RULES_SYNTAX

def normalize_snowflake_identifiers(ddl):
    inside_string = False
    inside_body = False
    quote_char = None
    upper_ddl = ddl

    body_markers = ['AS $$', 'AS\n$$', "AS '", "AS\n'"]
    for marker in body_markers:
        if marker in upper_ddl.upper():
            inside_body = True
            break

    if inside_body:
        body_start = -1
        for marker in body_markers:
            idx = upper_ddl.upper().find(marker)
            if idx != -1:
                body_start = idx
                break
        if body_start != -1:
            header = upper_ddl[:body_start]
            body = upper_ddl[body_start:]
            header = _normalize_header(header)
            return header + body
        return upper_ddl

    return _normalize_header(upper_ddl)

def _normalize_header(ddl):
    ddl = re.sub(r'\[dbo\]\.\s*', '', ddl, flags=re.IGNORECASE)
    ddl = re.sub(r'\[([^\]]+)\]', lambda m: m.group(1).replace(' ', '').upper(), ddl)
    ddl = re.sub(r'"([^"]+)"', lambda m: m.group(1).replace(' ', '').upper(), ddl)

    lines = ddl.split('\n')
    result = []
    for line in lines:
        stripped = line.strip().upper()
        if stripped.startswith('--') or stripped.startswith('/*'):
            result.append(line)
            continue
        if re.match(r"^(CREATE|ALTER|DROP|INSERT|SELECT|FROM|WHERE|AND|OR|SET|USE|GO)", stripped):
            line = _uppercase_identifiers_in_line(line)
        elif re.match(r"^\s*\w+\s+(VARCHAR|NUMBER|INT|BOOLEAN|TIMESTAMP|FLOAT|DATE|BINARY|VARIANT|CHAR|TEXT|NVARCHAR|NCHAR|BIT|MONEY|DATETIME|BIGINT|TINYINT|REAL|SMALLINT|DECIMAL|NUMERIC|AUTOINCREMENT|IDENTITY|NOT NULL|NULL|DEFAULT|PRIMARY|UNIQUE)", stripped):
            line = _uppercase_identifiers_in_line(line)
        result.append(line)
    return '\n'.join(result)

def _uppercase_identifiers_in_line(line):
    parts = re.split(r"('(?:[^']|'')*')", line)
    result = []
    for i, part in enumerate(parts):
        if i % 2 == 1:
            result.append(part)
        else:
            result.append(part.upper())
    return ''.join(result)

@st.cache_data(ttl=300)
def get_databases():
    """Get list of accessible databases"""
    try:
        result = session.sql("SHOW DATABASES").collect()
        return [row['name'] for row in result]
    except Exception:
        return ["RAW_DB"]

@st.cache_data(ttl=300)
def get_schemas(database):
    """Get list of schemas in a database"""
    try:
        if database:
            result = session.sql(f"SHOW SCHEMAS IN DATABASE \"{database}\"").collect()
            return [row['name'] for row in result]
    except Exception:
        pass
    return ["DBO", "PUBLIC"]

def clean_sql_response(response):
    """Remove markdown code blocks from LLM response"""
    cleaned = response.strip()
    if cleaned.startswith("```sql"):
        cleaned = cleaned[6:]
    elif cleaned.startswith("```snowflake"):
        cleaned = cleaned[12:]
    elif cleaned.startswith("```"):
        cleaned = cleaned[3:]
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3]
    return cleaned.strip().strip('`')

def extract_main_ddl(sql_content):
    """Extract only CREATE TABLE/VIEW/PROCEDURE, ignore INDEX/GRANT/etc."""
    sql_content = sql_content.strip()
    if sql_content.startswith('\ufeff'):
        sql_content = sql_content[1:]
    
    parts = re.split(r'\nGO\s*\n|\nGO\s*$', sql_content, flags=re.IGNORECASE)
    
    for part in parts:
        part = part.strip()
        if re.match(r'CREATE\s+(OR\s+REPLACE\s+)?(TABLE|VIEW|PROC|PROCEDURE|FUNCTION)', part, re.IGNORECASE):
            if part.count(';') > 1:
                part = part.split(';')[0]
            return part.rstrip(';')
    
    statements = sql_content.split(';')
    for stmt in statements:
        stmt = stmt.strip()
        if re.match(r'CREATE\s+(OR\s+REPLACE\s+)?(TABLE|VIEW|PROC|PROCEDURE|FUNCTION)', stmt, re.IGNORECASE):
            return stmt
    
    return sql_content

def convert_sp_to_snowflake(source_sp, source_type="SQL Server"):
    pitfalls = _get_known_pitfalls()
    prompt = f"""Convert this {source_type} stored procedure to a Snowflake Python stored procedure.

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
7. Use f-strings for dynamic SQL: session.sql(f"SELECT * FROM table WHERE col = '{{var}}'").collect()
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

{source_sp}"""
    
    result = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-opus-4-5', '{prompt.replace("'", "''")}') as response
    """).collect()
    
    response = clean_sql_response(result[0]['RESPONSE'])
    
    response = re.sub(r'CREATE\s+PROCEDURE\s+', 'CREATE OR REPLACE PROCEDURE ', response, count=1, flags=re.IGNORECASE)
    response = re.sub(r'CREATE\s+OR\s+REPLACE\s+OR\s+REPLACE', 'CREATE OR REPLACE', response, flags=re.IGNORECASE)
    
    response = normalize_snowflake_identifiers(response)
    
    return response

def convert_udf_to_snowflake(source_udf, source_type="SQL Server"):
    pitfalls = _get_known_pitfalls()
    prompt = f"""Convert this {source_type} User Defined Function (UDF) to a Snowflake UDF.

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
{source_udf}"""

    result = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-opus-4-5', '{prompt.replace("'", "''")}') as response
    """).collect()

    response = clean_sql_response(result[0]['RESPONSE'])

    response = re.sub(r'CREATE\s+FUNCTION\s+', 'CREATE OR REPLACE FUNCTION ', response, count=1, flags=re.IGNORECASE)
    response = re.sub(r'CREATE\s+OR\s+REPLACE\s+OR\s+REPLACE', 'CREATE OR REPLACE', response, flags=re.IGNORECASE)

    response = normalize_snowflake_identifiers(response)

    return response

# ── Auto-provision DBT stage ─────────────────────────────────────────────────
def init_dbt_stage():
    """
    Resolves <current_db>.DBT_PROJECTS.DBT_PROJECT_STAGE.
    Creates schema and stage with CREATE IF NOT EXISTS — safe to call every run.
    Returns the fully qualified stage path string.
    """
    current_db = session.get_current_database().replace('"', '')
    
    # Fixed schema and stage name — never user-configurable
    DBT_SCHEMA    = "DBT_PROJECTS"
    DBT_STAGE     = "DBT_PROJECT_STAGE"
    STAGE_PATH    = f"@{current_db}.{DBT_SCHEMA}.{DBT_STAGE}"
    QUALIFIED_DB  = f"{current_db}.{DBT_SCHEMA}"

    try:
        # Step 1: Ensure schema exists
        session.sql(f"""
            CREATE SCHEMA IF NOT EXISTS {current_db}.{DBT_SCHEMA}
        """).collect()

        # Step 2: Ensure stage exists (internal, no encryption needed)
        session.sql(f"""
            CREATE STAGE IF NOT EXISTS {current_db}.{DBT_SCHEMA}.{DBT_STAGE}
            COMMENT = 'Auto-provisioned by Migration Studio for dbt project files'
        """).collect()

        return STAGE_PATH, True, None

    except Exception as e:
        # Return path anyway — execute section will show the error inline
        return STAGE_PATH, False, str(e)

# Resolve once at app startup — stored in session state so it doesn't re-run on every widget interaction
if "dbt_stage" not in st.session_state or "dbt_stage_ready" not in st.session_state:
    _stage_path, _stage_ready, _stage_err = init_dbt_stage()
    st.session_state.dbt_stage       = _stage_path
    st.session_state.dbt_stage_ready = _stage_ready
    st.session_state.dbt_stage_error = _stage_err

def read_file_from_stage(stage_path: str, file_path: str) -> str | None:
    """
    Downloads a single file from stage and returns its content as string.
    Returns None if file doesn't exist on stage yet.
    """
    try:
        import tempfile, os
        with tempfile.TemporaryDirectory() as tmp_dir:
            full_stage_loc = f"{stage_path}/{file_path}"
            session.file.get(full_stage_loc, tmp_dir)
            # Snowflake appends .gz if auto-compressed — check both
            file_name = file_path.split("/")[-1]
            local_path = os.path.join(tmp_dir, file_name)
            gz_path    = local_path + ".gz"

            if os.path.exists(gz_path):
                import gzip
                with gzip.open(gz_path, "rt", encoding="utf-8") as f:
                    return f.read()
            elif os.path.exists(local_path):
                with open(local_path, "r", encoding="utf-8") as f:
                    return f.read()
            return None
    except Exception:
        return None  # File doesn't exist on stage yet — treat as new


def merge_yaml_with_stage(stage_path: str, file_path: str, new_content: str) -> str:
    """
    Downloads existing YAML from stage, merges with new_content.
    Falls back to new_content if nothing exists on stage yet.
    """
    file_name = file_path.split("/")[-1]
    existing  = read_file_from_stage(stage_path, file_path)

    if existing is None:
        return new_content  # Nothing on stage yet — use new content as-is

    if file_name == "sources.yml":
        return _merge_sources_yml(existing, new_content)
    elif file_name == "schema.yml":
        return _merge_schema_yml(existing, new_content)
    else:
        return existing  # dbt_project.yml / profiles.yml — keep existing

# ═══════════════════════════════════════════════════════════════════════════════
# KIPI.AI PREMIUM THEME CSS
# ═══════════════════════════════════════════════════════════════════════════════
@st.cache_resource
def _get_css():
    return """
<style>
/* ── Global ── */
[data-testid="stAppViewContainer"] { 
    background: linear-gradient(180deg, #FFFFFF 0%, #F7F8F6 100%); 
}
.stApp {
    background-color: #FFFFFF;
}
.block-container { 
    padding-top: 1rem; 
    padding-bottom: 2rem; 
    max-width: 1400px; 
}

/* ── Force ALL heading colors ── */
.stApp h1, .stApp h2, .stApp h3, .stApp h4, .stApp h5, .stApp h6,
[data-testid="stMarkdown"] h1, [data-testid="stMarkdown"] h2, [data-testid="stMarkdown"] h3,
[data-testid="stMarkdown"] h4, [data-testid="stMarkdown"] h5, [data-testid="stMarkdown"] h6,
.stMarkdown h1, .stMarkdown h2, .stMarkdown h3,
.element-container h1, .element-container h3,
.app-header h1, .section-header h3 {
    color: #272727 !important;
    -webkit-text-fill-color: #272727 !important;
    opacity: 1 !important;
}

/* ── App Header ── */
.app-header {
    display: flex; 
    align-items: center; 
    gap: 16px;
    padding: 1rem 1.5rem 1.5rem; 
    border-bottom: 2px solid #B3D561; 
    margin-bottom: 1rem;
    background: linear-gradient(90deg, #B3D561 0%, rgba(179, 213, 97, 0.25) 40%, transparent 80%);
    border-radius: 12px;
    justify-content: space-between;
}
.app-header-left {
    display: flex;
    align-items: center;
    gap: 16px;
}
.app-header-right {
    display: flex;
    align-items: center;
    flex-shrink: 0;
}
.app-header-right img {
    height: 48px;
    width: auto;
    object-fit: contain;
}
.app-logo img {
    height: 48px;
    width: auto;
}
.app-title { 
    font-size: 1.75rem; 
    font-weight: 800; 
    color: #272727;
    letter-spacing: -0.5px; 
    margin: 0; 
}
.app-badge {
    font-size: 0.65rem; 
    background: linear-gradient(135deg, #7AB929 0%, #5C9A1E 100%);
    color: white; 
    padding: 4px 12px; 
    border-radius: 20px; 
    font-weight: 600;
    letter-spacing: 0.8px; 
    text-transform: uppercase;
    box-shadow: 0 2px 8px rgba(122, 185, 41, 0.3);
}
.app-divider {
    width: 1px;
    height: 32px;
    background: #D1D5DB;
    margin: 0 4px;
}

/* ── Tabs ── */
[data-testid="stTabs"] [role="tablist"] { 
    gap: 4px; 
    border-bottom: 2px solid #E5E7EB; 
    padding-bottom: 0;
}
[data-testid="stTabs"] [role="tab"] {
    font-size: 0.75rem; 
    font-weight: 600; 
    padding: 0.6rem 1rem;
    border-radius: 8px 8px 0 0; 
    color: #6B7280; 
    border: none !important; 
    transition: all 0.2s ease;
    background: transparent;
}
[data-testid="stTabs"] [role="tab"]:hover {
    color: #1A1A1A;
    background: rgba(122, 185, 41, 0.08);
}
[data-testid="stTabs"] [role="tab"][aria-selected="true"] {
    color: #7AB929 !important;
    background: rgba(122, 185, 41, 0.1) !important;
    border-bottom: 2px solid #7AB929 !important;
}

/* ── Cards & Containers ── */
[data-testid="stExpander"] {
    background: #FFFFFF;
    border: 1px solid #E5E7EB;
    border-radius: 12px;
    overflow: hidden;
}
[data-testid="stExpander"] summary {
    font-weight: 600;
    color: #1A1A1A;
}

/* ── Buttons ── */
.stButton > button {
    border-radius: 8px;
    font-weight: 600;
    font-size: 0.8rem;
    padding: 0.5rem 1.25rem;
    transition: all 0.2s ease;
    border: 1px solid #D1D5DB;
    color: #374151;
}
.stButton > button[kind="primary"] {
    background: linear-gradient(135deg, #7AB929 0%, #5C9A1E 100%);
    border: none;
    color: white;
    box-shadow: 0 2px 8px rgba(122, 185, 41, 0.25);
}
.stButton > button[kind="primary"]:hover {
    box-shadow: 0 4px 16px rgba(122, 185, 41, 0.4);
    transform: translateY(-1px);
}
.stButton > button:not([kind="primary"]):hover {
    border-color: #7AB929;
    color: #7AB929;
}

/* ── Inputs ── */
.stTextInput > div > div > input,
.stTextArea > div > div > textarea,
.stSelectbox > div > div > div {
    background: #FFFFFF;
    border: 1px solid #D1D5DB;
    border-radius: 8px;
    color: #1A1A1A;
    transition: border-color 0.2s ease;
}
.stTextInput > div > div > input:focus,
.stTextArea > div > div > textarea:focus {
    border-color: #7AB929;
    box-shadow: 0 0 0 2px rgba(122, 185, 41, 0.15);
}

/* ── Metrics ── */
[data-testid="stMetric"] {
    background: linear-gradient(135deg, #FFFFFF 0%, #F7F8F6 100%);
    border: 1px solid #E5E7EB;
    border-radius: 12px;
    padding: 1rem;
}
[data-testid="stMetric"] label {
    color: #6B7280;
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}
[data-testid="stMetric"] [data-testid="stMetricValue"] {
    color: #1A1A1A;
    font-weight: 700;
}

/* ── Code Blocks ── */
.stCodeBlock {
    border-radius: 10px;
    border: 1px solid #E5E7EB;
}

/* ── Dataframes ── */
[data-testid="stDataFrame"] {
    border-radius: 10px;
    overflow: hidden;
    border: 1px solid #E5E7EB;
}

/* ── Alerts ── */
.stAlert {
    border-radius: 10px;
    border: none;
}

/* ── Progress Bar ── */
.stProgress > div > div > div {
    background: linear-gradient(90deg, #7AB929, #5C9A1E) !important;
}

/* ── Dividers ── */
hr {
    border-color: #E5E7EB;
    margin: 1.5rem 0;
}

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #F7F8F6 0%, #FFFFFF 100%);
    border-right: 1px solid #E5E7EB;
}
[data-testid="stSidebar"] .stMarkdown h4 {
    color: #1A1A1A;
    font-weight: 700;
    letter-spacing: -0.3px;
}

/* ── Status Badges ── */
.status-badge {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 12px;
    font-size: 0.7rem;
    font-weight: 600;
    text-transform: uppercase;
}
.status-success { background: rgba(122, 185, 41, 0.15); color: #5C9A1E; }
.status-pending { background: rgba(210, 153, 34, 0.15); color: #B8860B; }
.status-error { background: rgba(220, 38, 38, 0.12); color: #DC2626; }

/* ── Tab Info Tooltip ── */
.tab-info {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 10px 14px;
    margin-bottom: 1rem;
    background: linear-gradient(135deg, rgba(122, 185, 41, 0.06) 0%, rgba(122, 185, 41, 0.02) 100%);
    border-left: 3px solid #7AB929;
    border-radius: 0 8px 8px 0;
    font-size: 0.82rem;
    color: #4B5563;
    line-height: 1.4;
}
.tab-info .info-icon {
    font-size: 1rem;
    flex-shrink: 0;
}

/* ── Coming Soon / Roadmap ── */
.coming-soon-container {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 4rem 2rem;
    text-align: center;
}
.roadmap-header {
    text-align: center;
    margin-bottom: 2rem;
}
.roadmap-badge {
    display: inline-block;
    padding: 6px 18px;
    border-radius: 20px;
    font-size: 0.7rem;
    font-weight: 700;
    letter-spacing: 1.5px;
    text-transform: uppercase;
    background: linear-gradient(135deg, rgba(122, 185, 41, 0.12) 0%, rgba(122, 185, 41, 0.04) 100%);
    color: #5C9A1E;
    border: 1px solid rgba(122, 185, 41, 0.3);
    margin-bottom: 0.75rem;
}
.roadmap-title {
    font-size: 1.6rem;
    font-weight: 800;
    color: #272727;
    margin: 0.5rem 0 0.25rem;
    letter-spacing: -0.5px;
}
.roadmap-subtitle {
    font-size: 0.85rem;
    color: #6B7280;
    max-width: 600px;
    margin: 0 auto;
}
.roadmap-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 1.25rem;
    max-width: 900px;
    margin: 0 auto;
}
.roadmap-card {
    background: linear-gradient(135deg, #FFFFFF 0%, #F7F8F6 100%);
    border: 1px solid #E5E7EB;
    border-radius: 16px;
    padding: 1.5rem;
    text-align: left;
    transition: all 0.3s ease;
    position: relative;
    overflow: hidden;
}
.roadmap-card:hover {
    border-color: rgba(122, 185, 41, 0.4);
    box-shadow: 0 8px 24px rgba(122, 185, 41, 0.1);
    transform: translateY(-2px);
}
.roadmap-card::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    height: 3px;
    background: linear-gradient(90deg, #7AB929, #B3D561);
    opacity: 0;
    transition: opacity 0.3s ease;
}
.roadmap-card:hover::before {
    opacity: 1;
}
.roadmap-card-icon {
    font-size: 2rem;
    margin-bottom: 0.75rem;
    display: inline-block;
}
.roadmap-card-num {
    display: inline-block;
    font-size: 0.6rem;
    font-weight: 700;
    color: #7AB929;
    background: rgba(122, 185, 41, 0.1);
    padding: 2px 8px;
    border-radius: 10px;
    margin-left: 8px;
    vertical-align: middle;
    letter-spacing: 0.5px;
}
.roadmap-card-title {
    font-size: 1.05rem;
    font-weight: 700;
    color: #272727;
    margin-bottom: 0.5rem;
}
.roadmap-card-desc {
    font-size: 0.8rem;
    color: #6B7280;
    line-height: 1.6;
    margin-bottom: 0.75rem;
}
.roadmap-card-tags {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
}
.roadmap-tag {
    font-size: 0.6rem;
    font-weight: 600;
    letter-spacing: 0.3px;
    padding: 3px 10px;
    border-radius: 12px;
    background: rgba(122, 185, 41, 0.08);
    color: #5C9A1E;
    border: 1px solid rgba(122, 185, 41, 0.15);
}
.coming-soon-badge {
    display: inline-block;
    padding: 6px 18px;
    border-radius: 20px;
    font-size: 0.75rem;
    font-weight: 700;
    letter-spacing: 1px;
    text-transform: uppercase;
    background: linear-gradient(135deg, rgba(210, 153, 34, 0.15) 0%, rgba(210, 153, 34, 0.08) 100%);
    color: #B8860B;
    border: 1px solid rgba(210, 153, 34, 0.3);
    margin-bottom: 1.5rem;
}
.coming-soon-title {
    font-size: 1.5rem;
    font-weight: 800;
    color: #272727;
    margin: 0 0 0.5rem;
}
.coming-soon-desc {
    font-size: 0.9rem;
    color: #6B7280;
    max-width: 500px;
    line-height: 1.6;
}

/* ── Section Headers ── */
.section-header {
    display: flex;
    align-items: center;
    gap: 8px;
    margin: 1rem 0 0.75rem;
    padding-bottom: 0.5rem;
    border-bottom: 2px solid rgba(122, 185, 41, 0.25);
}
.section-title {
    font-size: 1rem;
    font-weight: 700;
    color: #272727;
    margin: 0;
}
.section-icon {
    font-size: 1.1rem;
}

/* ── File Uploader ── */
[data-testid="stFileUploader"] {
    background: #FFFFFF;
}
[data-testid="stFileUploader"] section {
    background: #FFFFFF !important;
    border: 2px dashed #D1D5DB !important;
    border-radius: 12px !important;
    padding: 1rem !important;
}
[data-testid="stFileUploader"] section:hover {
    border-color: #7AB929 !important;
    background: rgba(122, 185, 41, 0.03) !important;
}
[data-testid="stFileUploader"] section > div {
    color: #272727 !important;
}
[data-testid="stFileUploader"] button {
    color: #374151 !important;
    border-color: #D1D5DB !important;
    background: #FFFFFF !important;
}
[data-testid="stFileUploader"] button:hover {
    border-color: #7AB929 !important;
    color: #7AB929 !important;
}

/* ── Kipi Text Logo ── */
.kipi-logo {
    font-size: 1.6rem;
    font-weight: 900;
    color: #272727;
    -webkit-text-fill-color: #272727;
    letter-spacing: -0.5px;
    font-family: 'Inter', sans-serif;
}
.kipi-logo-ai {
    color: #7AB929;
    -webkit-text-fill-color: #7AB929;
}
.kipi-sidebar-brand {
    text-align: center;
    padding: 1rem 0 0.5rem;
    border-bottom: 1px solid rgba(122, 185, 41, 0.15);
    margin-bottom: 1rem;
}

/* ── Hide Streamlit Elements ── */
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header [data-testid="stHeader"] {visibility: hidden;}
[data-testid="collapsedControl"] {visibility: visible !important;}

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 8px; height: 8px; }
::-webkit-scrollbar-track { background: #F7F8F6; }
::-webkit-scrollbar-thumb { background: #D1D5DB; border-radius: 4px; }
::-webkit-scrollbar-thumb:hover { background: #9CA3AF; }

/* ── Download Button ── */
.stDownloadButton > button {
    background: rgba(122, 185, 41, 0.08);
    border: 1px solid rgba(122, 185, 41, 0.3);
    color: #5C9A1E;
}
.stDownloadButton > button:hover {
    background: rgba(122, 185, 41, 0.15);
    border-color: #7AB929;
}

/* ── Spinner ── */
.stSpinner > div > div {
    border-top-color: #7AB929 !important;
}

/* ── Labels & Captions ── */
.stSelectbox label, .stTextInput label, .stTextArea label,
.stFileUploader label, .stMultiSelect label, .stNumberInput label,
.stRadio label, .stCheckbox label {
    color: #374151 !important;
    font-weight: 600 !important;
}
p, .stMarkdown p {
    color: #374151;
}

/* ── Tooltips & Popovers (kipi green theme) ── */
[data-testid="stTooltipIcon"] + div,
[data-testid="stTooltipContent"],
div[data-baseweb="tooltip"] div,
div[data-baseweb="popover"] div,
[role="tooltip"],
[role="tooltip"] div,
[data-testid="tooltipHoverTarget"] + div,
div[data-floating-ui-portal] > div,
div[data-floating-ui-portal] > div > div,
div[data-floating-ui-portal] > div > div > div {
    background: linear-gradient(135deg, #F7FBF2 0%, #FFFFFF 100%) !important;
    background-color: #F7FBF2 !important;
    color: #2D3B1E !important;
    border: 1px solid rgba(122, 185, 41, 0.25) !important;
    border-left: 3px solid #7AB929 !important;
    box-shadow: 0 4px 16px rgba(122, 185, 41, 0.12), 0 1px 4px rgba(0, 0, 0, 0.06) !important;
    border-radius: 10px !important;
}
div[data-floating-ui-portal] p,
div[data-floating-ui-portal] span,
div[data-floating-ui-portal] div,
[role="tooltip"] p,
[role="tooltip"] span,
div[data-baseweb="tooltip"] p,
div[data-baseweb="tooltip"] span,
div[data-baseweb="popover"] p,
div[data-baseweb="popover"] span {
    color: #2D3B1E !important;
    -webkit-text-fill-color: #2D3B1E !important;
}
[data-testid="stTooltipIcon"] svg {
    color: #7AB929 !important;
    fill: #7AB929 !important;
    opacity: 0.6;
    transition: opacity 0.2s ease;
}
[data-testid="stTooltipIcon"]:hover svg {
    color: #5C9A1E !important;
    fill: #5C9A1E !important;
    opacity: 1;
}

/* ── Skeleton Shimmer Loading ── */
@keyframes shimmer {
    0% { background-position: -400px 0; }
    100% { background-position: 400px 0; }
}
.skeleton-block {
    background: linear-gradient(90deg, #F0F0F0 25%, #E0E0E0 50%, #F0F0F0 75%);
    background-size: 800px 100%;
    animation: shimmer 1.5s ease-in-out infinite;
    border-radius: 8px;
    height: 20px;
    margin: 8px 0;
}
.skeleton-block.skeleton-title { height: 28px; width: 60%; margin-bottom: 12px; }
.skeleton-block.skeleton-line { height: 14px; width: 100%; }
.skeleton-block.skeleton-line-short { height: 14px; width: 75%; }
.skeleton-block.skeleton-code {
    height: 180px;
    border-radius: 10px;
    border: 1px solid #E5E7EB;
}
.skeleton-card {
    background: #FFFFFF;
    border: 1px solid #E5E7EB;
    border-radius: 12px;
    padding: 1.25rem;
    margin: 0.75rem 0;
}

/* ── Toast Notifications ── */
@keyframes toastSlideIn {
    from { transform: translateX(120%); opacity: 0; }
    to   { transform: translateX(0); opacity: 1; }
}
@keyframes toastSlideOut {
    from { transform: translateX(0); opacity: 1; }
    to   { transform: translateX(120%); opacity: 0; }
}
.toast-container {
    position: fixed;
    bottom: 24px;
    right: 24px;
    z-index: 99999;
    display: flex;
    flex-direction: column-reverse;
    gap: 10px;
    pointer-events: none;
}
.toast {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 12px 20px;
    border-radius: 12px;
    font-size: 0.82rem;
    font-weight: 600;
    font-family: 'Inter', sans-serif;
    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.12);
    animation: toastSlideIn 0.4s ease forwards, toastSlideOut 0.4s ease 3.5s forwards;
    pointer-events: auto;
    max-width: 380px;
    backdrop-filter: blur(8px);
}
.toast-success {
    background: linear-gradient(135deg, rgba(122, 185, 41, 0.95) 0%, rgba(92, 154, 30, 0.95) 100%);
    color: #FFFFFF;
    border: 1px solid rgba(255, 255, 255, 0.2);
}
.toast-error {
    background: linear-gradient(135deg, rgba(220, 38, 38, 0.95) 0%, rgba(185, 28, 28, 0.95) 100%);
    color: #FFFFFF;
    border: 1px solid rgba(255, 255, 255, 0.2);
}
.toast-info {
    background: linear-gradient(135deg, rgba(255, 255, 255, 0.95) 0%, rgba(247, 248, 246, 0.95) 100%);
    color: #272727;
    border: 1px solid rgba(122, 185, 41, 0.3);
}
.toast-heal {
    background: linear-gradient(135deg, rgba(41, 181, 232, 0.95) 0%, rgba(30, 136, 180, 0.95) 100%);
    color: #FFFFFF;
    border: 1px solid rgba(255, 255, 255, 0.2);
}
.toast-icon { font-size: 1.1rem; flex-shrink: 0; }
.toast-close {
    cursor: pointer; opacity: 0.7; margin-left: auto; font-size: 1rem;
    pointer-events: auto;
}
.toast-close:hover { opacity: 1; }


/* ── Progress Stepper ── */
.stepper {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 0;
    margin: 1rem 0 1.5rem;
    padding: 0.75rem 1rem;
    background: linear-gradient(135deg, #FFFFFF 0%, #F7F8F6 100%);
    border: 1px solid #E5E7EB;
    border-radius: 12px;
}
.stepper-step {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 6px 12px;
    border-radius: 8px;
    transition: all 0.3s ease;
}
.stepper-step.active {
    background: rgba(122, 185, 41, 0.12);
}
.stepper-step.completed {
    background: rgba(122, 185, 41, 0.06);
}
.stepper-num {
    width: 24px; height: 24px;
    border-radius: 50%;
    display: flex; align-items: center; justify-content: center;
    font-size: 0.7rem; font-weight: 700;
    border: 2px solid #D1D5DB;
    color: #9CA3AF;
    transition: all 0.3s ease;
    flex-shrink: 0;
}
.stepper-step.active .stepper-num {
    border-color: #7AB929;
    background: #7AB929;
    color: #FFFFFF;
    box-shadow: 0 0 0 3px rgba(122, 185, 41, 0.2);
}
.stepper-step.completed .stepper-num {
    border-color: #7AB929;
    background: #7AB929;
    color: #FFFFFF;
}
.stepper-label {
    font-size: 0.72rem;
    font-weight: 600;
    color: #9CA3AF;
    font-family: 'Inter', sans-serif;
    white-space: nowrap;
}
.stepper-step.active .stepper-label { color: #272727; }
.stepper-step.completed .stepper-label { color: #5C9A1E; }
.stepper-step.warning { background: rgba(210, 153, 34, 0.1); }
.stepper-step.warning .stepper-num {
    border-color: #D29922;
    background: #D29922;
    color: #FFFFFF;
}
.stepper-step.warning .stepper-label { color: #B8860B; }
.stepper-step.failed { background: rgba(220, 38, 38, 0.08); }
.stepper-step.failed .stepper-num {
    border-color: #DC2626;
    background: #DC2626;
    color: #FFFFFF;
}
.stepper-step.failed .stepper-label { color: #DC2626; }
.stepper-badge {
    font-size: 0.55rem;
    font-weight: 700;
    padding: 1px 6px;
    border-radius: 8px;
    margin-left: 4px;
    vertical-align: middle;
}
.stepper-step.completed .stepper-badge {
    background: rgba(122, 185, 41, 0.12);
    color: #5C9A1E;
}
.stepper-step.warning .stepper-badge {
    background: rgba(210, 153, 34, 0.15);
    color: #B8860B;
}
.stepper-step.failed .stepper-badge {
    background: rgba(220, 38, 38, 0.12);
    color: #DC2626;
}
.stepper-connector.warn {
    background: linear-gradient(90deg, #D29922, #E8B84A);
}
.stepper-connector {
    width: 32px; height: 2px;
    background: #E5E7EB;
    flex-shrink: 0;
    transition: background 0.3s ease;
}
.stepper-connector.done {
    background: linear-gradient(90deg, #7AB929, #B3D561);
}
</style>
"""

st.markdown(_get_css(), unsafe_allow_html=True)

import base64

def _load_logo_b64():
    try:
        with open("Kipi Logo.png", "rb") as f:
            return base64.b64encode(f.read()).decode()
    except Exception:
        return None

KIPI_LOGO_B64 = _load_logo_b64()

# ═══════════════════════════════════════════════════════════════════════════════
# SPLASH SCREEN (first load only)
# ═══════════════════════════════════════════════════════════════════════════════
if "splash_dismissed" not in st.session_state:
    st.session_state.splash_dismissed = False

if not st.session_state.splash_dismissed:
    _splash_logo_tag = f'<img src="data:image/png;base64,{KIPI_LOGO_B64}" alt="kipi.ai" />' if KIPI_LOGO_B64 else '<span style="font-size:3rem;font-weight:900;color:#272727;font-family:Inter,sans-serif;">kipi<span style="color:#7AB929;">.ai</span></span>'
    st.markdown("""
    <style>
    @keyframes splashPulse {
        0%, 100% { transform: scale(1); opacity: 1; }
        50% { transform: scale(1.03); opacity: 0.95; }
    }
    @keyframes lineExpand {
        from { width: 0; } to { width: 120px; }
    }
    @keyframes splashGlow {
        0%, 100% { box-shadow: 0 0 40px rgba(122, 185, 41, 0.08); }
        50% { box-shadow: 0 0 80px rgba(122, 185, 41, 0.15); }
    }
    @keyframes titleReveal {
        from { opacity: 0; letter-spacing: 8px; }
        to   { opacity: 1; letter-spacing: -1px; }
    }
    @keyframes fadeUp {
        from { opacity: 0; transform: translateY(12px); }
        to   { opacity: 1; transform: translateY(0); }
    }
    .splash-card {
        text-align: center;
        padding: 3.5rem 2.5rem 2.5rem;
        border-radius: 24px;
        background: linear-gradient(135deg, #FFFFFF 0%, #F7F8F6 100%);
        border: 1px solid #E5E7EB;
        box-shadow: 0 4px 24px rgba(0, 0, 0, 0.08);
        animation: splashGlow 3s ease-in-out infinite;
        margin: 3rem auto 2rem;
        max-width: 620px;
    }
    .splash-logo {
        animation: splashPulse 3s ease-in-out infinite;
        margin-bottom: 0.5rem;
    }
    .splash-logo img {
        height: 64px;
        width: auto;
        object-fit: contain;
    }
    .splash-line {
        width: 120px; height: 3px;
        background: linear-gradient(90deg, #7AB929, #B3D561, #7AB929);
        border-radius: 2px;
        margin: 1rem auto 1.5rem;
        animation: lineExpand 1s ease forwards;
    }
    .splash-app-name {
        font-size: 2.4rem;
        font-weight: 900;
        color: #1A1A1A;
        font-family: 'Inter', -apple-system, sans-serif;
        letter-spacing: -1px;
        margin: 0 0 0.25rem;
        animation: titleReveal 0.8s ease-out forwards;
    }
    .splash-app-name-accent {
        background: linear-gradient(135deg, #7AB929 0%, #5C9A1E 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }
    .splash-badge {
        display: inline-block; font-size: 0.55rem; font-weight: 700; letter-spacing: 2px;
        text-transform: uppercase; color: #5C9A1E; border: 1px solid rgba(122, 185, 41, 0.3);
        padding: 5px 14px; border-radius: 20px; margin-bottom: 1.5rem; background: rgba(122, 185, 41, 0.08);
        animation: fadeUp 0.6s ease 0.3s both;
    }
    .splash-sub {
        font-size: 0.82rem; color: #6B7280; line-height: 1.7; margin-bottom: 2rem;
        font-family: 'Inter', sans-serif; max-width: 440px; margin-left: auto; margin-right: auto;
        animation: fadeUp 0.6s ease 0.5s both;
    }
    .splash-features {
        display: flex; gap: 0; justify-content: center; margin-bottom: 0.5rem;
        animation: fadeUp 0.6s ease 0.7s both;
    }
    .splash-feat {
        text-align: center; padding: 0.75rem 1.25rem;
        border-right: 1px solid #E5E7EB;
    }
    .splash-feat:last-child { border-right: none; }
    .splash-feat-icon { font-size: 1.3rem; margin-bottom: 6px; }
    .splash-feat-label {
        font-size: 0.6rem; color: #6B7280; text-transform: uppercase;
        letter-spacing: 0.8px; font-weight: 600; font-family: 'Inter', sans-serif;
    }
    </style>
    <div class="splash-card">
        <div class="splash-logo">""" + _splash_logo_tag + """</div>
        <div class="splash-line"></div>
        <div class="splash-app-name"><span class="splash-app-name-accent">CODA</span></div>
        <div class="splash-badge">Cortex-Orchestrated Data Automation</div>
        <div class="splash-sub">
            Accelerating legacy migrations from SQL Server to Snowflake & dbt
            with AI-driven code conversion, one-click deployment & real-time analytics.
        </div>
        <div class="splash-features">
            <div class="splash-feat"><div class="splash-feat-icon">⚡</div><div class="splash-feat-label">DDL Conversion</div></div>
            <div class="splash-feat"><div class="splash-feat-icon">🔄</div><div class="splash-feat-label">SP to dbt</div></div>
            <div class="splash-feat"><div class="splash-feat-icon">🚀</div><div class="splash-feat-label">One-Click Deploy</div></div>
            <div class="splash-feat"><div class="splash-feat-icon">📊</div><div class="splash-feat-label">Live Dashboard</div></div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    _splash_col1, _splash_col2, _splash_col3 = st.columns([2, 1, 2])
    with _splash_col2:
        if st.button("🚀 Get Started", type="primary", key="dismiss_splash", use_container_width=True):
            st.session_state.splash_dismissed = True
            st.rerun()
    st.stop()

# ═══════════════════════════════════════════════════════════════════════════════
# HEADER
# ═══════════════════════════════════════════════════════════════════════════════
_logo_html = ""
if KIPI_LOGO_B64:
    _logo_html = f'<img src="data:image/png;base64,{KIPI_LOGO_B64}" alt="kipi.ai - A WNS Company" />'

st.markdown(f"""
<div class="app-header">
    <div class="app-header-left">
        <div style="display:flex;flex-direction:column;">
            <h1 style="font-size:1.75rem;font-weight:800;color:#272727 !important;letter-spacing:-0.5px;margin:0;-webkit-text-fill-color:#272727 !important;line-height:1.1;">CODA</h1>
            <div style="font-size:0.65rem;font-weight:700;color:#6B7280;letter-spacing:2px;text-transform:uppercase;margin-top:3px;">Cortex-Orchestrated Data Automation</div>
        </div>
        <span class="app-badge">Cortex AI</span>
    </div>
    <div class="app-header-right">
        {_logo_html}
    </div>
</div>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION (UNCHANGED)
# ═══════════════════════════════════════════════════════════════════════════════
TARGET_SCHEMA = f"{session.get_current_database()}.{session.get_current_schema()}"
CONVERSION_TABLE = "CONVERTED_OBJECTS"

if 'converted_objects' not in st.session_state:
    st.session_state.converted_objects = {
        "TABLE_DDL": [], "VIEW_DDL": [], "STORED_PROCEDURE": [], 
        "DBT_PROJECT": [], "SSIS_PACKAGE": []
    }
if 'deployed_models' not in st.session_state:
    st.session_state.deployed_models = {"bronze": [], "silver": [], "gold": []}

if 'dbt_project_files' not in st.session_state:
    _stage_loaded = False
    _stage_files = {}
    try:
        _dbt_stage_path = f"@{session.get_current_database().replace(chr(34), '')}.DBT_PROJECTS.DBT_PROJECT_STAGE"
        _stage_rows = session.sql(f"LIST {_dbt_stage_path}").collect()
        if _stage_rows:
            for _sf in _stage_rows:
                _fname = _sf.get("name", "") if isinstance(_sf, dict) else _sf.as_dict().get("name", "")
                _rel = "/".join(_fname.split("/")[1:])
                if _rel and not _fname.endswith("/"):
                    _ext = "." + _rel.rsplit(".", 1)[-1] if "." in _rel else ""
                    if _ext.lower() in ('.sql', '.yml', '.yaml', '.md', '.txt'):
                        _content = read_file_from_stage(_dbt_stage_path, _rel)
                        if _content:
                            _stage_files[_rel] = _content
            if _stage_files:
                st.session_state.dbt_project_files = _stage_files
                st.session_state.stage_upload_done = True
                st.session_state["_uploaded_file_fingerprint"] = {fp: hash(content) for fp, content in _stage_files.items()}
                _stage_loaded = True
    except Exception:
        pass

    try:
        _saved_rows = session.sql(f"""
            SELECT TARGET_NAME, TARGET_CODE, SOURCE_NAME, SOURCE_CODE, DURATION_SECS
            FROM {TARGET_SCHEMA}.{CONVERSION_TABLE}
            WHERE CONVERSION_TYPE = 'DBT_PROJECT'
            ORDER BY CREATED_AT ASC
        """).collect()
        if _saved_rows:
            _restored_project_name = None
            _restored_results = []
            if not _stage_loaded:
                st.session_state['dbt_project_files'] = {}
            for _row in _saved_rows:
                try:
                    _models_json = json.loads(_row["TARGET_CODE"])
                    _proj_name = _row["SOURCE_NAME"] or "restored_project"
                    _restored_project_name = _proj_name
                    src_db = session.get_current_database().replace('"', '')
                    src_schema = session.get_current_schema().replace('"', '')
                    _new_files = generate_dbt_project_files(
                        _proj_name, _models_json,
                        source_database=src_db, source_schema=src_schema,
                    )
                    st.session_state.dbt_project_files = merge_dbt_project_files(_new_files)
                    _sp_fname = _row["TARGET_NAME"] or "unknown.sql"
                    if _sp_fname.startswith(_proj_name + "_dbt_"):
                        _sp_fname = _sp_fname[len(_proj_name + "_dbt_"):]
                    _restored_results.append({
                        "file": _sp_fname,
                        "source": _row["SOURCE_CODE"] or "",
                        "status": "ok",
                        "error": "",
                        "validation": None,
                        "medallion_models": _models_json,
                        "duration": _row["DURATION_SECS"] or 0,
                    })
                except Exception:
                    continue
            if _restored_project_name:
                st.session_state.current_project_name = _restored_project_name
            if _restored_results:
                st.session_state["dbt_sp_results"] = _restored_results
    except Exception:
        pass

# ═══════════════════════════════════════════════════════════════════════════════
# PREMIUM UX HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
import random as _random

def show_toast(message, toast_type="success", icon=None):
    _icons = {"success": "✅", "error": "❌", "info": "💡", "heal": "⚡"}
    _icon = icon or _icons.get(toast_type, "ℹ️")
    _id = f"toast-{_random.randint(1000, 9999)}"
    st.markdown(f"""
    <div class="toast-container">
        <div class="toast toast-{toast_type}" id="{_id}">
            <span class="toast-icon">{_icon}</span>
            <span>{message}</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

def show_skeleton(lines=5, show_code=True):
    html = '<div class="skeleton-card"><div class="skeleton-block skeleton-title"></div>'
    for i in range(lines):
        cls = "skeleton-line-short" if i == lines - 1 else "skeleton-line"
        html += f'<div class="skeleton-block {cls}"></div>'
    if show_code:
        html += '<div class="skeleton-block skeleton-code"></div>'
    html += '</div>'
    return html

def render_stepper(steps, current_step=0, step_states=None):
    html = '<div class="stepper">'
    for i, step in enumerate(steps):
        state = (step_states or {}).get(i)
        badge = ""
        if state and state.get("badge"):
            badge = f'<span class="stepper-badge">{state["badge"]}</span>'

        if state and state.get("status") in ("warning", "failed"):
            cls = state["status"]
            icon = "⚠" if state["status"] == "warning" else "✕"
        elif state and state.get("status") == "completed":
            cls = "completed"
            icon = "✓"
        elif i < current_step:
            cls = "completed"
            icon = "✓"
        elif i == current_step:
            cls = "active"
            icon = str(i + 1)
        else:
            cls = ""
            icon = str(i + 1)

        html += f'<div class="stepper-step {cls}"><div class="stepper-num">{icon}</div><div class="stepper-label">{step}{badge}</div></div>'
        if i < len(steps) - 1:
            if state and state.get("status") == "warning":
                conn_cls = "warn"
            elif i < current_step:
                conn_cls = "done"
            else:
                conn_cls = ""
            html += f'<div class="stepper-connector {conn_cls}"></div>'
    html += '</div>'
    st.markdown(html, unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS (UNCHANGED LOGIC)
# ═══════════════════════════════════════════════════════════════════════════════
def init_conversion_table():
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.{CONVERSION_TABLE} (
            ID NUMBER AUTOINCREMENT,
            CONVERSION_TYPE VARCHAR(50),
            SOURCE_NAME VARCHAR(255),
            SOURCE_CODE TEXT,
            TARGET_NAME VARCHAR(255),
            TARGET_CODE TEXT,
            STATUS VARCHAR(50),
            DURATION_SECS FLOAT DEFAULT 0,
            WAS_HEALED BOOLEAN DEFAULT FALSE,
            HEAL_ATTEMPTS NUMBER DEFAULT 0,
            ORIGINAL_DDL TEXT,
            CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """).collect()
    try:
        session.sql(f"ALTER TABLE {TARGET_SCHEMA}.{CONVERSION_TABLE} ADD COLUMN IF NOT EXISTS DURATION_SECS FLOAT DEFAULT 0").collect()
    except Exception:
        pass
    try:
        session.sql(f"ALTER TABLE {TARGET_SCHEMA}.{CONVERSION_TABLE} ADD COLUMN IF NOT EXISTS WAS_HEALED BOOLEAN DEFAULT FALSE").collect()
        session.sql(f"ALTER TABLE {TARGET_SCHEMA}.{CONVERSION_TABLE} ADD COLUMN IF NOT EXISTS HEAL_ATTEMPTS NUMBER DEFAULT 0").collect()
        session.sql(f"ALTER TABLE {TARGET_SCHEMA}.{CONVERSION_TABLE} ADD COLUMN IF NOT EXISTS ORIGINAL_DDL TEXT").collect()
    except Exception:
        pass

if "_init_conv_done" not in st.session_state:
    init_conversion_table()
    st.session_state["_init_conv_done"] = True

HEAL_KB_TABLE = "HEAL_KNOWLEDGE_BASE"

def init_heal_kb():
    try:
        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.{HEAL_KB_TABLE} (
                ID NUMBER AUTOINCREMENT,
                ERROR_PATTERN VARCHAR(500),
                ERROR_CODE VARCHAR(20),
                SOURCE_PATTERN TEXT,
                FIX_DESCRIPTION TEXT,
                ORIGINAL_DDL TEXT,
                HEALED_DDL TEXT,
                OBJECT_TYPE VARCHAR(50),
                OCCURRENCE_COUNT NUMBER DEFAULT 1,
                CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                LAST_SEEN_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """).collect()
    except Exception:
        pass

if "_init_heal_done" not in st.session_state:
    init_heal_kb()
    st.session_state["_init_heal_done"] = True

def _generalize_error(error_msg):
    cleaned = re.sub(r"'[^']*'", "'<value>'", error_msg)
    cleaned = re.sub(r"line \d+ at position \d+", "line <N> at position <N>", cleaned)
    cleaned = re.sub(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", "<query_id>", cleaned)
    cleaned = re.sub(r"\(\d+\):", "(<code>):", cleaned, count=1)
    return cleaned[:500]

def _extract_error_code(error_msg):
    m = re.search(r'(\d{6})\s*\(', error_msg)
    return m.group(1) if m else ""

def _save_heal_knowledge(error_msg, original_ddl, healed_ddl, obj_type, attempts):
    try:
        pattern = _generalize_error(error_msg)
        code = _extract_error_code(error_msg)
        source_snip = original_ddl[:2000].replace("'", "''") if original_ddl else ""
        healed_snip = healed_ddl[:2000].replace("'", "''") if healed_ddl else ""
        fix_desc = ""
        try:
            summarize_prompt = f"""Compare these two Snowflake DDLs and describe ONLY what changed to fix the error. Be concise (2-3 bullet points max).

ERROR: {error_msg[:300]}

BEFORE (failed):
{original_ddl[:1500]}

AFTER (fixed):
{healed_ddl[:1500]}

Return ONLY the bullet points describing the fix, no preamble:"""
            fix_result = session.sql(f"""
                SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', '{summarize_prompt.replace("'", "''")}') as response
            """).collect()
            fix_desc = fix_result[0]['RESPONSE'].strip()[:2000]
        except Exception:
            if attempts:
                parts = []
                for a in attempts:
                    if a.get("error") and a.get("healed_ddl"):
                        parts.append(f"Error: {a['error'][:200]}")
                fix_desc = " | ".join(parts)
        fix_desc_escaped = fix_desc[:2000].replace("'", "''")
        pattern_escaped = pattern.replace("'", "''")
        existing = session.sql(f"""
            SELECT ID, OCCURRENCE_COUNT FROM {TARGET_SCHEMA}.{HEAL_KB_TABLE}
            WHERE ERROR_PATTERN = '{pattern_escaped}' AND OBJECT_TYPE = '{obj_type}'
            LIMIT 1
        """).collect()
        if existing:
            row_id = existing[0]["ID"]
            count = existing[0]["OCCURRENCE_COUNT"] + 1
            session.sql(f"""
                UPDATE {TARGET_SCHEMA}.{HEAL_KB_TABLE}
                SET OCCURRENCE_COUNT = {count},
                    LAST_SEEN_AT = CURRENT_TIMESTAMP(),
                    HEALED_DDL = '{healed_snip}'
                WHERE ID = {row_id}
            """).collect()
        else:
            session.sql(f"""
                INSERT INTO {TARGET_SCHEMA}.{HEAL_KB_TABLE}
                (ERROR_PATTERN, ERROR_CODE, SOURCE_PATTERN, FIX_DESCRIPTION, ORIGINAL_DDL, HEALED_DDL, OBJECT_TYPE)
                VALUES ('{pattern_escaped}', '{code}', '{source_snip}', '{fix_desc_escaped}', '{source_snip}', '{healed_snip}', '{obj_type}')
            """).collect()
    except Exception:
        pass

@st.cache_data(ttl=120)
def _get_known_pitfalls():
    try:
        rows = session.sql(f"""
            SELECT ERROR_PATTERN, FIX_DESCRIPTION, OCCURRENCE_COUNT
            FROM {TARGET_SCHEMA}.{HEAL_KB_TABLE}
            ORDER BY OCCURRENCE_COUNT DESC
            LIMIT 15
        """).collect()
        if not rows:
            return ""
        lines = []
        for r in rows:
            pattern = r["ERROR_PATTERN"]
            fix = r["FIX_DESCRIPTION"][:150] if r["FIX_DESCRIPTION"] else ""
            count = r["OCCURRENCE_COUNT"]
            lines.append(f"- [{count}x] {pattern} → {fix}")
        return "\nKNOWN PITFALLS (these errors have occurred before in past conversions — AVOID them):\n" + "\n".join(lines) + "\n"
    except Exception:
        return ""

def save_conversion(conv_type, source_name, source_code, target_name, target_code, status="CONVERTED", duration_secs=0, was_healed=False, heal_attempts=0, original_ddl=None):
    escaped_source = source_code.replace("'", "''") if source_code else ""
    escaped_target = target_code.replace("'", "''") if target_code else ""
    escaped_original = original_ddl.replace("'", "''") if original_ddl else ""
    healed_flag = "TRUE" if was_healed else "FALSE"
    session.sql(f"""
        INSERT INTO {TARGET_SCHEMA}.{CONVERSION_TABLE} 
        (CONVERSION_TYPE, SOURCE_NAME, SOURCE_CODE, TARGET_NAME, TARGET_CODE, STATUS, DURATION_SECS, WAS_HEALED, HEAL_ATTEMPTS, ORIGINAL_DDL)
        VALUES ('{conv_type}', '{source_name}', '{escaped_source}', '{target_name}', '{escaped_target}', '{status}', {duration_secs}, {healed_flag}, {heal_attempts}, '{escaped_original}')
    """).collect()

def convert_ddl_to_snowflake(source_ddl, source_type="SQL Server"):
    pitfalls = _get_known_pitfalls()
    prompt = f"""Convert this {source_type} DDL to Snowflake SQL.
IMPORTANT: Always use CREATE OR REPLACE syntax.

{CONVERSION_RULES_ALL}
{pitfalls}
Return ONLY the converted DDL, no explanations, no markdown:

{source_ddl}"""
    
    result = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-opus-4-5', '{prompt.replace("'", "''")}') as response
    """).collect()
    
    response = clean_sql_response(result[0]['RESPONSE'])
    
    response = re.sub(r'CREATE\s+TABLE\s+', 'CREATE OR REPLACE TABLE ', response, count=1, flags=re.IGNORECASE)
    response = re.sub(r'CREATE\s+VIEW\s+', 'CREATE OR REPLACE VIEW ', response, count=1, flags=re.IGNORECASE)
    response = re.sub(r'CREATE\s+OR\s+REPLACE\s+OR\s+REPLACE', 'CREATE OR REPLACE', response, flags=re.IGNORECASE)
    
    response = normalize_snowflake_identifiers(response)
    
    return response


def _sanitize_sp_for_prompt(sp_body: str, source_db: str = "") -> str:
    sp_body = re.sub(r'\[\$\(([^)]+)\)\]', source_db if source_db else r'\1', sp_body)
    sp_body = re.sub(r'\$\(([^)]+)\)', source_db if source_db else r'\1', sp_body)
    return sp_body


# ── Model Registry (cross-reference already-converted dbt models) ─────────────
def _build_model_registry() -> dict:
    """
    Scans st.session_state.dbt_project_files for existing model names.
    Returns a dict mapping UPPERCASED SQL Server-style names to dbt model names.
    Example: {"DIMADDRESS": "dim_address", "PIPELINEDEAL": "pipeline_deal"}
    """
    registry = {}
    project_files = st.session_state.get("dbt_project_files", {})
    for fpath in project_files:
        # Only look at model SQL files (models/*/*.sql)
        if not fpath.startswith("models/") or not fpath.endswith(".sql"):
            continue
        # Extract model name from path: models/gold/dim_address.sql → dim_address
        model_name = fpath.rsplit("/", 1)[-1].replace(".sql", "")
        # Skip schema/sources yml files that might sneak in
        if model_name in ("schema", "sources"):
            continue
        # Build reverse lookup: uppercase no-underscore → model_name
        # e.g. dim_address → DIMADDRESS, stg_customer_address → STGCUSTOMERADDRESS
        upper_squashed = model_name.upper().replace("_", "")
        registry[upper_squashed] = model_name
        # Also register the original uppercase with underscores
        registry[model_name.upper()] = model_name
    # Also scan converted_objects for table/view DDLs
    for obj_type_key in ["TABLE_DDL", "VIEW_DDL"]:
        for obj in st.session_state.get("converted_objects", {}).get(obj_type_key, []):
            src_name = obj.get("source", "").upper().replace(" ", "")
            tgt_name = obj.get("target", "").upper().replace(" ", "")
            if src_name and tgt_name:
                registry[src_name] = tgt_name.lower()
    return registry


def _format_registry_for_prompt(registry: dict) -> str:
    """
    Formats the model registry as a prompt section for the LLM.
    Only includes gold/mart models (no stg_/int_ intermediates) to keep the prompt focused.
    """
    if not registry:
        return ""
    # Filter to only meaningful models (gold layer, not staging/intermediate scaffolding)
    gold_entries = {}
    for upper_key, model_name in registry.items():
        if model_name.startswith("stg_") or model_name.startswith("int_"):
            continue
        gold_entries[upper_key] = model_name
    if not gold_entries:
        return ""
    lines = []
    seen = set()
    for upper_key, model_name in sorted(gold_entries.items()):
        if model_name not in seen:
            lines.append(f"  - {upper_key} → already exists as dbt model: {model_name}")
            seen.add(model_name)
    return (
        "\nALREADY CONVERTED MODELS (use ref() instead of source() for these):\n"
        "If a source table in this SP matches one of the following already-converted gold models, "
        "use {{ ref('<model_name>') }} instead of {{ source('raw', '<TABLE>') }} and do NOT create a staging model for it.\n"
        + "\n".join(lines) + "\n"
    )


def convert_view_to_dbt_model(source_ddl: str, source_type: str = "SQL Server") -> dict | None:
    """
    Converts a SQL Server view definition to a dbt gold-layer model,
    cross-referencing already-converted models for proper ref() usage.
    Returns a dict: {"model_name": str, "sql": str, "description": str, "base_tables": list}
    or None on failure.
    """
    registry = _build_model_registry()
    registry_prompt = _format_registry_for_prompt(registry)
    pitfalls = _get_known_pitfalls()

    prompt = f"""[STRICT JSON OUTPUT ONLY - NO PREAMBLE - NO MARKDOWN]
TASK: Convert this {source_type} view definition into a dbt gold-layer model.

RULES:
1. The output model should be materialized as 'view' in the 'gold' schema.
2. Convert all SQL Server syntax to Snowflake SQL (types, functions, identifiers).
3. ALL identifiers must be UPPERCASE.
4. The model name must be lowercase_snake_case derived from the view name.
{CONVERSION_RULES_NAMING}
{CONVERSION_RULES_TYPES}
{CONVERSION_RULES_FUNCTIONS}
{registry_prompt}
5. CRITICAL: If the view references a table that already exists as a dbt model (listed above),
   use {{{{ ref('<model_name>') }}}} instead of a direct table reference.
   For tables NOT in the list above, use {{{{ source('raw', '<TABLE_NAME>') }}}}.
6. Preserve all WHERE clauses, column aliases, and logic exactly.
7. Strip [dbo]. prefixes, bracket notation, and SQL Server-specific hints.
{pitfalls}

Return ONLY this exact JSON (no markdown, no backticks):
{{{{
    "model_name": "<lowercase_snake_case_name>",
    "description": "<one-line description>",
    "sql": "{{{{{{ config(materialized='view', schema='gold') }}}}}}\\n\\nSELECT ...\\nFROM {{{{{{ ref('...') }}}}}}",
    "base_tables": ["<table_name_1>", "<table_name_2>"]
}}}}

View DDL:
{source_ddl}"""

    escaped_prompt = prompt.replace("'", "''")
    result = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-opus-4-5', '{escaped_prompt}') as response
    """).collect()

    response_text = result[0]['RESPONSE'].strip()
    parsed = _robust_json_parse(response_text)
    if parsed is None:
        st.error("JSON Parse Error: Could not parse view-to-dbt LLM response.")
        with st.expander("Raw LLM Output"):
            st.code(response_text)
        return None
    return parsed


def _robust_json_parse(raw_text: str) -> dict | None:
    cleaned = clean_sql_response(raw_text)
    json_match = re.search(r'\{[\s\S]*\}', cleaned)
    if not json_match:
        return None

    raw_json = json_match.group()
    raw_json = re.sub(r',\s*}', '}', raw_json)
    raw_json = re.sub(r',\s*]', ']', raw_json)

    try:
        return json.loads(raw_json)
    except json.JSONDecodeError:
        pass

    try:
        sanitized = raw_json.replace('\n', '\\n').replace('\r', '\\r')
        return json.loads(sanitized)
    except json.JSONDecodeError:
        pass

    try:
        fixed = re.sub(r"(?<!\\)'([^'\\]*(?:\\.[^'\\]*)*)'\s*:", r'"\1":', raw_json)
        fixed = re.sub(r":\s*'([^'\\]*(?:\\.[^'\\]*)*)'", r': "\1"', fixed)
        fixed = fixed.replace('\n', '\\n').replace('\r', '\\r')
        return json.loads(fixed)
    except json.JSONDecodeError:
        pass

    try:
        import ast
        return ast.literal_eval(raw_json)
    except Exception:
        pass

    return None


# ── Schema Catalog Fetcher (single query, reused across all SPs) ──────────────
def fetch_schema_catalog(source_db: str, source_schema: str) -> dict:
    """
    Issues exactly 2 INFORMATION_SCHEMA queries and returns a catalog dict:
        {
            "tables":    {"TABLE_NAME": "BASE TABLE" | "VIEW", ...},
            "functions": {"FUNC_NAME", ...},
            "error":     str | None
        }
    Call once per batch — all SPs share the same source db/schema.
    """
    catalog = {"tables": {}, "functions": set(), "error": None}
    try:
        rows = session.sql(f"""
            SELECT TABLE_NAME, TABLE_TYPE
            FROM "{source_db}".INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{source_schema}'
        """).collect()
        catalog["tables"] = {r['TABLE_NAME'].upper(): r['TABLE_TYPE'] for r in rows}
    except Exception as e:
        catalog["error"] = f"Metadata query failed (tables): {e}"
        return catalog

    try:
        rows = session.sql(f"""
            SELECT FUNCTION_NAME
            FROM "{source_db}".INFORMATION_SCHEMA.FUNCTIONS
            WHERE FUNCTION_SCHEMA = '{source_schema}'
        """).collect()
        catalog["functions"] = {r['FUNCTION_NAME'].upper() for r in rows}
    except Exception:
        catalog["functions"] = set()   # non-fatal — schema may have no UDFs

    return catalog


# ── Metadata Dependency Validator ─────────────────────────────────────────────
def validate_sp_dependencies(sp_source: str, source_db: str, source_schema: str,
                             catalog: dict | None = None) -> dict:
    """
    Pre-conversion metadata check.  Parses the SP to extract referenced tables,
    views and functions, then cross-references against the *catalog* dict.

    If *catalog* is supplied the function is pure-CPU (no SQL round-trips).
    If omitted, it calls fetch_schema_catalog() internally (legacy path).

    Returns:
        {
            "valid":   bool,
            "found":   [{"name": ..., "type": ...}],
            "missing": [{"name": ..., "type": ...}],
            "target":  str | None,
            "error":   str | None
        }
    """
    # ── Obtain catalog (once, or reuse) ───────────────────────────────────────
    if catalog is None:
        catalog = fetch_schema_catalog(source_db, source_schema)
    if catalog.get("error"):
        return {"valid": False, "found": [], "missing": [],
                "target": None, "error": catalog["error"]}

    existing_tables = catalog["tables"]     # {NAME: TABLE_TYPE}
    existing_funcs  = catalog["functions"]  # {NAME}

    # ── Step 0: Normalize the SP text ─────────────────────────────────────────
    sp_clean = re.sub(r'--[^\n]*', ' ', sp_source, flags=re.IGNORECASE)
    sp_clean = re.sub(r'/\*.*?\*/', ' ', sp_clean, flags=re.DOTALL)
    sp_clean = re.sub(r"'[^']*'", "''", sp_clean)
    sp_clean_upper = sp_clean.upper()

    # ── Step 1: Identify TARGET table (INSERT INTO / MERGE INTO) ──────────────
    target_names = set()
    for m in re.finditer(r'MERGE\s+(?:INTO\s+)?(?:[\[\]\w$()\".]+\.)*\[?(\w+)\]?', sp_clean_upper):
        target_names.add(m.group(1))
    for m in re.finditer(r'INSERT\s+INTO\s+(?:[\[\]\w$()\".]+\.)*\[?(\w+)\]?', sp_clean_upper):
        target_names.add(m.group(1))

    # ── Step 2: CTE names (noise) ────────────────────────────────────────────
    cte_names = set()
    for m in re.finditer(r'WITH\s+(\w+)\s+AS\s*\(', sp_clean_upper):
        cte_names.add(m.group(1))

    # ── Step 3: Candidate TABLE/VIEW names ───────────────────────────────────
    candidate_tables = set()
    for m in re.finditer(r'(?:FROM|JOIN)\s+(?:[\[\]\w$()\".]+\.)*\[?(\w+)\]?', sp_clean_upper):
        candidate_tables.add(m.group(1))
    for m in re.finditer(r'UPDATE\s+(?:[\[\]\w$()\".]+\.)*\[?(\w+)\]?\s+SET', sp_clean_upper):
        candidate_tables.add(m.group(1))

    # ── Step 4: Candidate FUNCTION names ─────────────────────────────────────
    candidate_functions = set()
    for m in re.finditer(r'(?:\[?\w+\]?\.)(\w+)\s*\(', sp_clean_upper):
        candidate_functions.add(m.group(1))

    # ── Step 5: Filter noise ─────────────────────────────────────────────────
    sql_keywords = {
        'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE', 'FROM', 'JOIN', 'WHERE',
        'SET', 'INTO', 'VALUES', 'CREATE', 'ALTER', 'DROP', 'TABLE', 'VIEW',
        'PROCEDURE', 'FUNCTION', 'AS', 'ON', 'AND', 'OR', 'NOT', 'NULL', 'IS',
        'IN', 'EXISTS', 'BETWEEN', 'LIKE', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
        'BEGIN', 'DECLARE', 'IF', 'WHILE', 'RETURN', 'EXEC', 'EXECUTE', 'PRINT',
        'CAST', 'CONVERT', 'COALESCE', 'ISNULL', 'NULLIF', 'LEFT', 'RIGHT',
        'INNER', 'OUTER', 'CROSS', 'FULL', 'UNION', 'ALL', 'DISTINCT', 'TOP',
        'ORDER', 'BY', 'GROUP', 'HAVING', 'WITH', 'NOLOCK', 'GO', 'USE',
        'GETDATE', 'NEWID', 'OBJECT_ID', 'SCOPE_IDENTITY', 'CHARINDEX', 'LEN',
        'SUBSTRING', 'REPLACE', 'LTRIM', 'RTRIM', 'UPPER', 'LOWER', 'DATEADD',
        'DATEDIFF', 'YEAR', 'MONTH', 'DAY', 'FORMAT', 'TRY_CAST', 'IIF',
        'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'OVER', 'PARTITION', 'SUM', 'COUNT',
        'AVG', 'MIN', 'MAX', 'STRING_AGG', 'STUFF', 'TRIGGER', 'INDEX',
        'CONSTRAINT', 'PRIMARY', 'KEY', 'FOREIGN', 'REFERENCES', 'DEFAULT',
        'IDENTITY', 'CHECK', 'UNIQUE', 'CLUSTERED', 'NONCLUSTERED', 'ASC', 'DESC',
        'TEMPDB', 'DBO', 'SYS', 'INFORMATION_SCHEMA',
        'VARCHAR', 'INT', 'BIGINT', 'BIT', 'DATETIME', 'FLOAT', 'DECIMAL',
        'NUMBER', 'BOOLEAN', 'TIMESTAMP', 'DATE', 'TIME', 'MONEY', 'SMALLMONEY',
        'NVARCHAR', 'CHAR', 'NCHAR', 'TEXT', 'NTEXT', 'IMAGE', 'BINARY',
        'VARBINARY', 'UNIQUEIDENTIFIER', 'XML', 'VARIANT', 'TINYINT', 'SMALLINT',
        'NUMERIC', 'REAL', 'SMALLDATETIME', 'DATETIMEOFFSET',
        'CURRENT_TIMESTAMP', 'AUTOINCREMENT', 'MATCHED', 'SOURCE', 'TARGET',
    }

    candidate_tables -= sql_keywords
    candidate_tables -= cte_names
    candidate_tables -= target_names
    candidate_tables = {t for t in candidate_tables if not t.startswith('#') and not t.startswith('@')}

    candidate_functions -= sql_keywords
    candidate_functions -= {'COALESCE', 'ISNULL', 'NULLIF', 'CONVERT', 'CAST',
                            'GETDATE', 'NEWID', 'LEN', 'SUBSTRING', 'CHARINDEX',
                            'REPLACE', 'LTRIM', 'RTRIM', 'UPPER', 'LOWER',
                            'DATEADD', 'DATEDIFF', 'FORMAT', 'IIF', 'TRY_CAST',
                            'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'SUM', 'COUNT',
                            'AVG', 'MIN', 'MAX', 'STRING_AGG', 'STUFF',
                            'OBJECT_ID', 'SCOPE_IDENTITY', 'YEAR', 'MONTH', 'DAY',
                            'CURRENT_TIMESTAMP', 'UUID_STRING', 'POSITION',
                            'LENGTH', 'OCTET_LENGTH', 'LISTAGG', 'TO_CHAR'}

    # ── Step 6: Cross-reference against catalog (pure CPU, no queries) ────────
    found = []
    missing = []

    for tbl in sorted(candidate_tables):
        if tbl in existing_tables:
            obj_type = 'VIEW' if 'VIEW' in existing_tables[tbl].upper() else 'TABLE'
            found.append({"name": tbl, "type": obj_type})
        else:
            missing.append({"name": tbl, "type": "TABLE/VIEW"})

    for fn in sorted(candidate_functions):
        if fn in existing_funcs:
            found.append({"name": fn, "type": "FUNCTION"})
        else:
            missing.append({"name": fn, "type": "FUNCTION"})

    return {
        "valid": len(missing) == 0,
        "found": found,
        "missing": missing,
        "target": list(target_names)[0] if target_names else None,
        "error": None
    }



def _normalize_model_names(models, source_sp):
    source_tables = set()
    for src in models.get('sources', []):
        src_name = src.get('name', '')
        if src_name:
            source_tables.add(src_name.upper())

    rename_map = {}

    for model in models.get('bronze_models', []):
        old_name = model.get('name', '')
        base = re.sub(r'^stg_', '', old_name, flags=re.IGNORECASE)
        matched_src = None
        for st_name in source_tables:
            if base.upper().replace('_', '') == st_name.replace('_', ''):
                matched_src = st_name
                break
        if matched_src:
            new_name = 'stg_' + matched_src.lower()
        else:
            new_name = 'stg_' + base.lower()
        if new_name != old_name:
            rename_map[old_name] = new_name
            model['name'] = new_name

    for model in models.get('silver_models', []):
        old_name = model.get('name', '')
        base = re.sub(r'^int_', '', old_name, flags=re.IGNORECASE)
        new_name = 'int_' + base.lower()
        if new_name != old_name:
            rename_map[old_name] = new_name
            model['name'] = new_name

    for model in models.get('gold_models', []):
        old_name = model.get('name', '')
        has_dim = old_name.lower().startswith('dim_') or old_name.lower().startswith('dim')
        has_fct = old_name.lower().startswith('fct_') or old_name.lower().startswith('fact_')

        if has_dim:
            base = re.sub(r'^dim_?', '', old_name, flags=re.IGNORECASE)
            new_name = 'dim_' + base.lower()
        elif has_fct:
            base = re.sub(r'^(?:fct|fact)_?', '', old_name, flags=re.IGNORECASE)
            new_name = 'fact_' + base.lower()
        else:
            new_name = old_name.lower()

        if new_name != old_name:
            rename_map[old_name] = new_name
            model['name'] = new_name

    if rename_map:
        for layer in ['bronze_models', 'silver_models', 'gold_models']:
            for model in models.get(layer, []):
                sql = model.get('sql', '')
                for old, new in rename_map.items():
                    sql = re.sub(
                        r"(ref\s*\(\s*['\"])" + re.escape(old) + r"(['\"])",
                        r"\g<1>" + new + r"\2",
                        sql
                    )
                model['sql'] = sql

        old_columns = models.get('columns', {})
        new_columns = {}
        for col_key, col_val in old_columns.items():
            new_key = rename_map.get(col_key, col_key)
            new_columns[new_key] = col_val
        models['columns'] = new_columns


def convert_sp_to_dbt_medallion(source_sp, sp_name, project_name=None, source_database=None, source_schema=None, existing_models_registry=None):
    """
    Converts a stored procedure to a complete dbt project JSON including
    proper schema.yml, sources.yml, and dbt_project.yml content.
    If existing_models_registry is provided, injects cross-reference info
    so the LLM uses ref() for already-converted models.
    """
    proj = project_name or sp_name.lower().replace(" ", "_")
    
    src_db = source_database or session.get_current_database().replace('"', '')
    src_schema = source_schema or 'DBO'

    source_sp = _sanitize_sp_for_prompt(source_sp, src_db)

    source_table_ddls = ""
    try:
        catalog = fetch_schema_catalog(src_db, src_schema)
        known_tables = set(catalog.get("tables", {}).keys())
        sp_upper = source_sp.upper()
        referenced_tables = [t for t in known_tables if t in sp_upper]
        ddl_parts = []
        for tbl in referenced_tables:
            try:
                ddl_row = session.sql(
                    f"SELECT GET_DDL('TABLE', '{src_db}.{src_schema}.{tbl}') AS DDL"
                ).collect()
                if ddl_row:
                    ddl_parts.append(f"-- {tbl}\n{ddl_row[0]['DDL']}")
            except Exception:
                pass
        if ddl_parts:
            source_table_ddls = "\n\n".join(ddl_parts)
    except Exception:
        source_table_ddls = ""

    prompt = f"""[STRICT JSON OUTPUT ONLY - NO PREAMBLE - NO MARKDOWN]
TASK: Convert the attached SQL Server stored procedure to modular dbt models. Break into multiple models following dbt best practices with staging → intermediate → marts architecture.

RULES:
REPLICATE EXACT SP LOGIC INTO DBT MODELS.
CRITICAL - TARGET TABLE IDENTIFICATION: The TARGET TABLE is the table being INSERT INTO, MERGE INTO, or UPDATE in the SP. This is the FINAL OUTPUT table.
CRITICAL - DO NOT CREATE ANY SOURCE OR STAGING MODEL FOR THE TARGET TABLE! The target table does not exist yet - it will be created by dbt. Never use source('raw', '<target_table>') or create stg_<target_table> models.
USE FUNCTIONS LOGIC CORRECTLY, USE FUNCTION CALLS AT RIGHT PLACES, For FUNCTION CALLS USE Fully qualify the FUNCTIONS in the model SQL using SOURCE DATABASE: {src_db} AND SOURCE SCHEMA: {src_schema} for eg. {src_db}.{src_schema}.<FUNCTION_NAME>
IMPORTANT: ALL source objects (tables, views, functions) exist in database '{src_db}' and schema '{src_schema}'. Use these exact values for all source references and function calls.
USE single quotes for string literals eg. 'N/A', not double quotes. Double quotes in Snowflake mean identifiers (column/table names).
{_format_registry_for_prompt(existing_models_registry or {})}
1. REPLICATE THE EXACT LOGIC FROM STORED PROCEDURE INTO THE DBT MODELS WHICH WILL MATCH THE OUTPUT OF SP TO THE FINAL DBT MODEL!!!
2. Identify ALL SOURCE tables (tables being READ FROM via SELECT/JOIN). Map them to dbt source() references. EXCLUDE the target table from sources!
3. CONVERSION APPROACH TO BE FOLLOWED STRICTLY:
    
    LAYER 1 - STAGING (stg_*.sql):

    - Create one model per SOURCE table only (tables being SELECT FROM, not INSERT INTO)
    - NEVER create staging model for the target/output table
    - Materialized as VIEW (always — lightweight, no storage cost, always fresh from source)
    - CRITICAL: SELECT **ALL** COLUMNS from the source table using SELECT * or by explicitly listing EVERY column that exists in the source table. Do NOT cherry-pick only columns used in the SP — the bronze layer must be a COMPLETE 1:1 mirror of the source table with all columns included.
    - REFERENCE THE SOURCE TABLE DDLs BELOW to know the EXACT columns each table has. You MUST include every column from the DDL in the staging model.
    - Apply only: CAST to Snowflake-compatible types, RENAME columns to UPPERCASE, COALESCE nulls where needed
    - Do NOT filter rows, do NOT drop columns, do NOT add business logic
    - Example: Lines 38-60 → stg_sales_freeze_so.sql
    
    LAYER 2 - INTERMEDIATE (int_*.sql):
    
    - One model per business logic operation
    - Choose materialization based on complexity:
      * VIEW — if simple transforms, referenced by 1-2 downstream models
      * TABLE — if heavy joins, window functions, or referenced by 3+ downstream models
      * EPHEMERAL — if only used once as a CTE in another model
    - Convert WHILE loops → CTEs with ROW_NUMBER()/window functions
    - Convert temp tables → CTEs or separate models
    - Convert UPDATE statements → SELECT with CASE/transformations
    - Example: Lines 118-203 WHILE loop → int_invoice_shifts.sql
    
    LAYER 3 - MARTS (fct_*.sql, dim_*.sql):

    - Choose materialization based on the SP logic and downstream usage:
      * TABLE — default for marts; best for BI tool queries, large result sets, aggregations, dimension tables
      * VIEW — only if the mart is a simple passthrough or rarely queried
      * INCREMENTAL — if the SP uses INSERT with date filters, MERGE, or processes only new/changed rows
    - UNION ALL of all intermediate classifications
    - Example: All classifications → output_model.sql

5. Every model SQL must start with a dbt config block. Choose materialization per model based on rules above:
   {{{{ config(materialized='view', schema='bronze') }}}}        <- bronze: always view
   {{{{ config(materialized='table', schema='silver') }}}}       <- silver: table if heavy logic
   {{{{ config(materialized='view', schema='silver') }}}}        <- silver: view if light logic
   {{{{ config(materialized='table', schema='gold') }}}}         <- gold: table for BI-facing models
   {{{{ config(materialized='incremental', schema='gold') }}}}   <- gold: incremental if SP has date-based inserts/merges
6. For schema.yml: List every column with name, description, and at least 1 test (not_null or unique where applicable).
7. For sources.yml: List ONLY source tables (tables being read from), NEVER include the target table.
8. Use Snowflake-compatible SQL only (no TOP, use LIMIT; no ISNULL, use COALESCE).
{CONVERSION_RULES_NAMING}
{CONVERSION_RULES_TYPES}
{CONVERSION_RULES_FUNCTIONS}
9. DO NOT use single quotes inside SQL strings in JSON - use double quotes or escape them.
10. Escape all newlines as \\n in JSON strings.
11. All the SQL server UDFs are converted and already exist in Snowflake in same schema as the source tables. So where ever SQL server UDFs are used replace with SF UDFs.
12. The FINAL GOLD MODEL name must be the TARGET TABLE name from the stored procedure (the table being INSERT INTO or populated). For example, if SP inserts into DIM_ADDRESS table, the gold model = dim_address.
13. ALWAYS convert WHILE loops to set-based CTEs/recursive CTEs.
14. AS DBT CONTRACTS IS NOT IMPLEMENTED ENSURE THAT EXPLICIT CASTING IS DONE CORRECTLY IN ALL MODELS.
15. DBT MODEL NAMING (CRITICAL — overrides DDL naming rules for model file names):
    - ALL model names MUST use lowercase snake_case
    - CamelCase MUST be split into snake_case: DimAddress → dim_address, DimAllocationDetail → dim_allocation_detail
    - Preserve existing underscores: DIM_ADDRESS → dim_address
    - Bronze: stg_<snake_case_source_table>.sql (e.g. stg_customer_address)
    - Silver: int_<snake_case_logic>.sql (e.g. int_address_default_record)
    - Gold: <snake_case_target_table>.sql (e.g. dim_address, fct_sales_summary)
    - NEVER concatenate words without underscores: dimaddress is WRONG, dim_address is CORRECT

REQUIRED OUTPUT FILES:

1. All SQL model files(complete without asking to fill similar patterns) organized in staging/intermediate/marts folders
2. sources.yml defining all source tables
3. schema.yml for each layer(staging, intermediate,mart) with descriptions

QUALITY CHECKLIST:

- STRICTLY HOW HAVE THE LOGIC OF THE ATTACHED STORED PROCEDURE
- All WHILE loops converted to set-based operations
- All temp tables converted to CTEs or models
- All UPDATE logic converted to SELECT transformations
- All business logic preserved
- Proper model dependencies
- Uses Snowflake SQL syntax (not SQL Server)
- ROLE: Act as a Senior Analytics Engineer and dbt Expert specialized in migrating T-SQL (SQL Server) Stored Procedures to Snowflake.
- Enforce Jinja Templating (ref & source).
- handle syntax translation, not just logic conversion.
- schema.yml: Ask for tests (unique, not_null) creation.


Return ONLY this exact JSON structure (no markdown, no backticks, no explanation):
{{
    "bronze_models": [
        {{
            "name": "stg_<table_name>",
            "description": "Raw ingestion of <table_name> from source",
            "sql": "{{{{ config(materialized=\\"view\\", schema=\\"bronze\\") }}}}\\n\\nSELECT ...\\nFROM {{{{ source(\\"raw\\", \\"<table_name>\\") }}}}"
        }}
    ],
    "silver_models": [
        {{
            "name": "int_<model_name>",
            "description": "Cleaned and transformed <model_name>",
            "sql": "{{{{ config(materialized=\\"table\\", schema=\\"silver\\") }}}}\\n\\nSELECT ...\\nFROM {{{{ ref(\\"bronze_<table_name>\\") }}}}"
        }}
    ],
    "gold_models": [
        {{
            "name": "<model_name>",
            "description": "Business-ready aggregation of <model_name>",
            "sql": "{{{{ config(materialized=\\"table\\", schema=\\"gold\\") }}}}\\n\\nSELECT ...\\nFROM {{{{ ref(\\"silver_<model_name>\\") }}}}  -- choose: table (default for BI), view, or incremental"
        }}
    ],
    "sources": [
        {{
            "name": "<source_table_name>",
            "database": "{src_db}",
            "schema": "{src_schema}",
            "description": "Source table <table_name> from raw layer"
        }}
    ],
    "columns": {{
        "<model_name>": [
            {{"name": "<col_name>", "description": "<description>", "tests": ["not_null"]}}
        ]
    }}
}}

Stored Procedure Name: {sp_name}

SOURCE TABLE DDLs (use these to list ALL columns in staging models):
{source_table_ddls if source_table_ddls else "Not available — use SELECT * for staging models."}

Stored Procedure:
{source_sp}"""

    escaped_prompt = prompt.replace("'", "''")

    result = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-opus-4-5', '{escaped_prompt}') as response
    """).collect()

    response_text = result[0]['RESPONSE'].strip()

    models = _robust_json_parse(response_text)
    if models is None:
        st.error("JSON Parse Error: Could not parse LLM response as valid JSON after all fallback attempts.")
        with st.expander("Raw LLM Output"):
            st.code(response_text)
        return {"bronze_models": [], "silver_models": [], "gold_models": [], "sources": [], "columns": {}}

    for layer in ['bronze_models', 'silver_models', 'gold_models']:
        for model in models.get(layer, []):
            if 'content' in model and 'sql' not in model:
                model['sql'] = model['content']

    _normalize_model_names(models, source_sp)
    return models


def _regenerate_single_model(model_name, layer, current_sql, source_db=None, source_schema=None):
    src_db = source_db or session.get_current_database().replace('"', '')
    src_schema = source_schema or 'DBO'

    table_ddl = ""
    if layer == "bronze":
        raw_table = model_name.replace("stg_", "").upper()
        try:
            ddl_row = session.sql(
                f"SELECT GET_DDL('TABLE', '{src_db}.{src_schema}.{raw_table}') AS DDL"
            ).collect()
            if ddl_row:
                table_ddl = ddl_row[0]['DDL']
        except Exception:
            pass

    layer_instructions = {
        "bronze": f"""Regenerate this dbt STAGING model. It must be a COMPLETE 1:1 mirror of the source table.
- Materialized as VIEW in schema 'bronze'
- SELECT ALL columns from the source table — do NOT skip any
- Apply CAST to Snowflake types, UPPERCASE all identifiers
- Use {{{{ source('raw', '{model_name.replace("stg_", "").upper()}') }}}} as the FROM clause
- Do NOT add business logic, filtering, or joins

SOURCE TABLE DDL (include EVERY column from this):
{table_ddl if table_ddl else 'Not available — use SELECT * instead.'}""",
        "silver": """Regenerate this dbt INTERMEDIATE model.
- Keep the existing logic and transformations intact
- Fix any syntax issues for Snowflake compatibility
- Use ref() for upstream models
- UPPERCASE all identifiers""",
        "gold": """Regenerate this dbt GOLD/MART model.
- Keep the existing business logic intact
- Fix any syntax issues for Snowflake compatibility  
- Use ref() for upstream models
- UPPERCASE all identifiers""",
    }

    prompt = f"""[STRICT SQL OUTPUT ONLY - NO MARKDOWN - NO EXPLANATION]
You are a Snowflake dbt expert. {layer_instructions.get(layer, layer_instructions['gold'])}

Current model SQL (improve/fix this):
{current_sql}

{CONVERSION_RULES_NAMING}
{CONVERSION_RULES_TYPES}

Return ONLY the complete model SQL starting with {{{{ config(...) }}}}. No markdown fences, no explanation."""

    try:
        escaped = prompt.replace("'", "''")
        result = session.sql(f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-opus-4-5', '{escaped}') as response
        """).collect()
        return clean_sql_response(result[0]['RESPONSE'].strip())
    except Exception as e:
        return None

def convert_ssis_to_snowflake(ssis_xml):
    prompt = f"""Analyze this SSIS package XML and convert it to Snowflake equivalents.
For each data flow, create appropriate Snowflake objects (tasks, streams, procedures).

{CONVERSION_RULES_NAMING}
{CONVERSION_RULES_TYPES}
{CONVERSION_RULES_FUNCTIONS}

Return as JSON with structure:
{{
    "tasks": [{{"name": "...", "sql": "...", "description": "..."}}],
    "streams": [{{"name": "...", "sql": "...", "description": "..."}}],
    "procedures": [{{"name": "...", "sql": "...", "description": "..."}}]
}}

SSIS Package:
{ssis_xml}"""
    
    result = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-opus-4-5', '{prompt.replace("'", "''")}') as response
    """).collect()
    
    response_text = result[0]['RESPONSE'].strip()
    json_match = re.search(r'\{[\s\S]*\}', response_text)
    if json_match:
        return json.loads(json_match.group())
    return {"tasks": [], "streams": [], "procedures": []}

def generate_dbt_project_files(project_name, models_data, source_database=None, source_schema=None):
    files = {}
    proj = project_name.lower().replace(" ", "_")
    # ── profiles.yml (auto-populated from active session) ────────────────────
    try:
        current_db      = session.get_current_database().replace('"', '')
        current_schema  = session.get_current_schema().replace('"', '')
        current_wh      = session.get_current_warehouse().replace('"', '')
        current_role    = session.get_current_role().replace('"', '')
    except Exception:
        current_db      = "YOUR_DATABASE"
        current_schema  = "PUBLIC"
        current_wh      = "COMPUTE_WH"
        current_role    = "SYSADMIN"
        current_user    = "YOUR_USERNAME"
        account_info    = "YOUR_ACCOUNT_IDENTIFIER"
    
    src_db = source_database or current_db
    src_schema_name = source_schema or 'DBO'
        
    # ── dbt_project.yml ──────────────────────────────────────────────────────
    files['dbt_project.yml'] = """name: '{proj}'
version: '1.0.0'
config-version: 2

profile: '{proj}'

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
  {proj}:
    bronze:
      +materialized: view
      +schema: bronze
      +database: {current_db}
    silver:
      +materialized: table
      +schema: silver
      +database: {current_db}
    gold:
      +materialized: table
      +schema: gold
      +database: {current_db}
""".format(proj=proj, current_db=current_db)

    

    files['profiles.yml'] = f"""{proj}:
    target: dev
    outputs:
        dev:
            type: snowflake
            account: ""
            user: ""
            role: "{current_role}"
            database: "{current_db}"
            warehouse: "{current_wh}"
            schema: "{current_schema}"
            threads: 4
"""

    # ── sources.yml (under models/staging) ───────────────────────────────────
    sources = models_data.get('sources', [])
    if sources:
        source_entries = ""
        for src in sources:
            source_entries += f"""      - name: {src.get('name', 'unknown_table')}
        description: "{src.get('description', 'Source table')}"
"""
        files['models/staging/sources.yml'] = f"""version: 2

sources:
  - name: raw
    database: "{src_db}"
    schema: "{src_schema_name}"
    description: "Raw source layer from {src_db}.{src_schema_name}"
    tables:
{source_entries}"""
    else:
        files['models/staging/sources.yml'] = f"""version: 2

sources:
  - name: raw
    database: "{src_db}"
    schema: "{src_schema_name}"
    description: "Raw source layer from {src_db}.{src_schema_name}"
    tables:
      - name: source_table
        description: "Update with actual source tables"
"""
        
    files['macros/generate_schema_name.sql'] = """
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
"""

    # ── Model SQL files + per-layer schema.yml ────────────────────────────────
    columns_map = models_data.get('columns', {})

    for layer in ['bronze', 'silver', 'gold']:
        layer_models  = models_data.get(f'{layer}_models', [])
        schema_models_block = ""

        for model in layer_models:
            model_name = model.get('name', f'{layer}_model')
            model_sql  = model.get('sql', f'-- TODO: Add SQL for {model_name}')
            model_desc = model.get('description', f'{layer.capitalize()} layer model')

            # ── Write .sql file ───────────────────────────────────────────
            files[f'models/{layer}/{model_name}.sql'] = model_sql

            # ── Build columns block for schema.yml ────────────────────────
            cols      = columns_map.get(model_name, [])
            cols_block = ""
            if cols:
                for col in cols:
                    tests_str = ""
                    for t in col.get('tests', []):
                        tests_str += f"\n            - {t}"
                    cols_block += f"""      - name: {col['name']}
        description: "{col.get('description', '')}"
        tests:{tests_str if tests_str else chr(10) + '            - not_null'}
"""
            else:
                cols_block = """      - name: id
        description: "Primary key"
        tests:
          - not_null
"""

             # ── Build schema.yml model block with contract enforced ───────
            schema_models_block += f"""  - name: {model_name}
    description: "{model_desc}"
    columns:
{cols_block}"""

        # ── Write schema.yml for this layer ──────────────────────────────
        files[f'models/{layer}/schema.yml'] = f"""version: 2

models:
{schema_models_block if schema_models_block else '  # No models generated for this layer'}
"""
    return files

def merge_dbt_project_files(new_files: dict) -> dict:
    """
    Merges new_files into the existing cumulative dbt_project_files in session state.
    
    Strategy per file type:
    - schema.yml  → merge model blocks (deduplicate by model name)
    - sources.yml → merge table entries (deduplicate by table name)
    - dbt_project.yml / profiles.yml → keep existing, don't overwrite
    - *.sql files → always overwrite (latest SQL wins)
    """
    existing: dict = st.session_state.get("dbt_project_files", {})
    merged = dict(existing)  # start with full copy of existing

    for file_path, new_content in new_files.items():
        file_name = file_path.split("/")[-1]

        # ── SQL model files: always take latest ──────────────────────────
        if file_name.endswith(".sql"):
            merged[file_path] = new_content

        # ── dbt_project.yml / profiles.yml: keep existing ────────────────
        elif file_name in ("dbt_project.yml", "profiles.yml"):
            if file_path not in merged:
                merged[file_path] = new_content
            # else: keep the existing one untouched

        # ── sources.yml: merge tables block ──────────────────────────────
        elif file_name == "sources.yml":
            if file_path not in merged:
                merged[file_path] = new_content
            else:
                merged[file_path] = _merge_sources_yml(
                    existing_yaml = merged[file_path],
                    new_yaml      = new_content
                )

        # ── schema.yml: merge model blocks ───────────────────────────────
        elif file_name == "schema.yml":
            if file_path not in merged:
                merged[file_path] = new_content
            else:
                merged[file_path] = _merge_schema_yml(
                    existing_yaml = merged[file_path],
                    new_yaml      = new_content
                )

        # ── macros and other files: keep existing ─────────────────────────
        else:
            if file_path not in merged:
                merged[file_path] = new_content

    return merged


def _merge_schema_yml(existing_yaml: str, new_yaml: str) -> str:
    """Merges model blocks from new_yaml into existing_yaml. New model wins on name collision."""
    try:
        existing_data = yaml.safe_load(existing_yaml) or {"version": 2, "models": []}
        new_data      = yaml.safe_load(new_yaml)      or {"version": 2, "models": []}

        existing_models = existing_data.get("models", []) or []
        new_models      = new_data.get("models", [])      or []

        # Index existing by name for dedup
        model_index = {m["name"]: m for m in existing_models if "name" in m}

        for model in new_models:
            if "name" in model:
                model_index[model["name"]] = model  # new SP's version wins

        existing_data["models"] = list(model_index.values())
        return yaml.dump(existing_data, default_flow_style=False, sort_keys=False, allow_unicode=True)

    except Exception:
        # YAML parse failed — append as comment block to avoid data loss
        return existing_yaml + "\n\n# --- MERGE CONFLICT: manually integrate below ---\n" + new_yaml


def _merge_sources_yml(existing_yaml: str, new_yaml: str) -> str:
    """Merges table entries from new_yaml into existing sources.yml."""
    try:
        existing_data = yaml.safe_load(existing_yaml) or {"version": 2, "sources": []}
        new_data      = yaml.safe_load(new_yaml)      or {"version": 2, "sources": []}

        # Work at the sources[0].tables level (single source block 'raw')
        existing_sources = existing_data.get("sources", []) or []
        new_sources      = new_data.get("sources", [])      or []

        # Build index of existing tables inside each source by (source_name, table_name)
        source_index = {s.get("name"): s for s in existing_sources}

        for new_src in new_sources:
            src_name = new_src.get("name")
            if src_name in source_index:
                # Merge tables within the same named source
                existing_tables = source_index[src_name].get("tables", []) or []
                new_tables      = new_src.get("tables", []) or []
                table_index     = {t["name"]: t for t in existing_tables if "name" in t}
                for t in new_tables:
                    if "name" in t:
                        table_index[t["name"]] = t  # new wins on collision
                source_index[src_name]["tables"] = list(table_index.values())
            else:
                source_index[src_name] = new_src  # brand new source block

        existing_data["sources"] = list(source_index.values())
        return yaml.dump(existing_data, default_flow_style=False, sort_keys=False, allow_unicode=True)

    except Exception:
        return existing_yaml + "\n\n# --- MERGE CONFLICT: manually integrate below ---\n" + new_yaml

def deploy_model_to_snowflake(model_name, model_sql, target_db, target_schema):
    try:
        # Use fully qualified name directly - no USE statements
        view_name = f"{target_db}.{target_schema}.{model_name.upper()}"
        create_sql = f"CREATE OR REPLACE VIEW {view_name} AS {model_sql}"
        session.sql(create_sql).collect()
        return {"success": True, "name": model_name, "view": view_name}
    except Exception as e:
        return {"success": False, "name": model_name, "error": str(e)}

# ═══════════════════════════════════════════════════════════════════════════════
# REUSABLE MULTI-FILE CONVERSION TAB RENDERER
# ═══════════════════════════════════════════════════════════════════════════════

def _extract_object_name(ddl: str, obj_type: str) -> str:
    """
    Extracts just the bare object name from converted DDL.
    Handles: dbo.TableName, [dbo].[TableName], "schema"."name", plain names.
    """
    patterns = {
        "TABLE":     r'CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+([\w.\[\]"]+)',
        "VIEW":      r'CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+([\w.\[\]"]+)',
        "FUNCTION":  r'CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+([\w.\[\]"]+)',
        "PROCEDURE": r'CREATE\s+(?:OR\s+REPLACE\s+)?PROC(?:EDURE)?\s+([\w.\[\]"]+)',
    }
    m = re.search(patterns.get(obj_type, r'(\w+)'), ddl, re.IGNORECASE)
    if not m:
        return f"UNKNOWN_{obj_type}"

    raw = m.group(1)                            # e.g. [dbo].[MyTable] or dbo.MyTable
    # Strip brackets, quotes, and take last part after any dot
    raw = re.sub(r'[\[\]"]', '', raw)           # remove [ ] "
    return raw.split('.')[-1]                   # take bare name only

def _extract_sql_from_zip(zip_bytes):
    extracted = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes), 'r') as zf:
        for entry in zf.namelist():
            if entry.endswith('/') or entry.startswith('__MACOSX'):
                continue
            lower = entry.lower()
            if lower.endswith('.sql') or lower.endswith('.txt'):
                try:
                    content = zf.read(entry).decode('utf-8')
                    fname = os.path.basename(entry)
                    extracted.append({"name": fname, "content": content})
                except Exception:
                    pass
    return extracted

def _strip_unsupported_constraints(table_ddl: str) -> str:
    """
    Removes constraint lines that Snowflake parses but cannot resolve at CREATE time:
      - FOREIGN KEY ... REFERENCES ...
      - CHECK (...)  — cross-table checks not supported
    Keeps: PRIMARY KEY, UNIQUE, NOT NULL (these are fine in Snowflake)
    """
    lines = table_ddl.split('\n')
    cleaned = []
    skip_continuation = False

    for line in lines:
        stripped = line.strip().upper()

        # Start of an FK or CHECK constraint line
        if re.search(r'CONSTRAINT\s+\w+\s+FOREIGN\s+KEY', stripped) or \
           re.search(r'^\s*FOREIGN\s+KEY', stripped) or \
           re.search(r'CONSTRAINT\s+\w+\s+CHECK\s*\(', stripped):
            skip_continuation = True
            continue

        # Multi-line continuation of a skipped constraint
        if skip_continuation:
            # A line that closes the constraint (ends with ) or ),)
            if re.search(r'\)\s*,?\s*$', stripped):
                skip_continuation = False
            continue

        cleaned.append(line)

    result = '\n'.join(cleaned)

    # Remove trailing comma before the closing ) of the CREATE TABLE
    # Pattern: last comma on a line just before the final closing )
    result = re.sub(r',\s*\n(\s*\))', r'\n\1', result)

    return result
    
HEALABLE_PATTERNS = [
    r'SQL compilation error',
    r'invalid identifier',
    r'invalid expression',
    r'unexpected',
    r'syntax error',
    r'invalid value',
    r'invalid default',
    r'not recognized',
    r'unsupported',
    r'unknown function',
    r'missing column',
    r'duplicate column',
    r'001003', r'001007', r'002262', r'002043', r'000904', r'001044',
]

NON_HEALABLE_PATTERNS = [
    r'insufficient privileges',
    r'permission denied',
    r'access denied',
    r'warehouse.*not.*found',
    r'no active warehouse',
    r'ACCOUNTADMIN',
    r'quota exceeded',
    r'session does not have a current database',
    r'network',
    r'timeout',
    r'authentication',
    r'role .* does not exist',
]

def _is_healable_error(error_msg: str) -> bool:
    upper = error_msg.upper()
    for pat in NON_HEALABLE_PATTERNS:
        if re.search(pat, error_msg, re.IGNORECASE):
            return False
    for pat in HEALABLE_PATTERNS:
        if re.search(pat, error_msg, re.IGNORECASE):
            return True
    return False

def _cortex_auto_heal(ddl: str, error_msg: str, obj_type: str, obj_name: str,
                       target_db: str, target_schema: str, max_attempts: int = 3):
    attempts = []
    current_ddl = ddl

    for attempt_num in range(1, max_attempts + 1):
        heal_prompt = f"""You are a Snowflake SQL expert. The following DDL failed to execute on Snowflake.

FAILED DDL:
{current_ddl}

SNOWFLAKE ERROR:
{error_msg}

Fix ONLY the specific error. Return ONLY the corrected DDL — no explanation, no markdown fences, no commentary.
Keep CREATE OR REPLACE and all column definitions intact. Use valid Snowflake syntax only."""

        try:
            escaped = heal_prompt.replace("'", "''")
            result = session.sql(f"""
                SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-opus-4-5', '{escaped}') as response
            """).collect()
            healed_ddl = clean_sql_response(result[0]['RESPONSE'].strip())
        except Exception as heal_err:
            attempts.append({"attempt": attempt_num, "error": error_msg, "healed_ddl": "", "heal_error": str(heal_err)})
            return False, current_ddl, attempts

        attempts.append({"attempt": attempt_num, "error": error_msg, "healed_ddl": healed_ddl, "heal_error": ""})

        try:
            _deploy_ddl(healed_ddl, obj_type, obj_name, target_db, target_schema)
            return True, healed_ddl, attempts
        except Exception as deploy_err:
            error_msg = str(deploy_err)
            current_ddl = healed_ddl
            if not _is_healable_error(error_msg):
                attempts[-1]["heal_error"] = f"Non-healable: {error_msg}"
                return False, current_ddl, attempts

    return False, current_ddl, attempts

def _parse_dbt_run_errors(error_msg: str) -> list:
    errors = []
    pattern = r"(?:error in model|Error in model)\s+'?(\w+)'?\s*\(([^)]+)\)\s*[\n:]*\s*(.*?)(?=(?:error in model|Error in model|$))"
    for m in re.finditer(pattern, error_msg, re.IGNORECASE | re.DOTALL):
        model_name = m.group(1).strip()
        file_path = m.group(2).strip()
        err_detail = m.group(3).strip()
        err_detail = re.sub(r'\s*compiled code at /tmp\S*', '', err_detail).strip()
        err_detail = re.sub(r'\s*Context:.*$', '', err_detail, flags=re.DOTALL).strip()
        errors.append({"model": model_name, "file": file_path, "error": err_detail})
    if not errors and error_msg:
        m2 = re.search(r"model '(\w+)'", error_msg)
        if m2:
            errors.append({"model": m2.group(1), "file": "", "error": error_msg[:500]})
    return errors

def _dbt_auto_heal_model(model_sql: str, error_msg: str, model_name: str) -> str | None:
    pitfalls = _get_known_pitfalls()
    heal_prompt = f"""You are a Snowflake dbt SQL expert. The following dbt model failed during `dbt run`.

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
Keep all {{{{ ref() }}}} and {{{{ source() }}}} macros intact. Do not change model logic beyond fixing the error."""

    try:
        escaped = heal_prompt.replace("'", "''")
        result = session.sql(f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-opus-4-5', '{escaped}') as response
        """).collect()
        return clean_sql_response(result[0]['RESPONSE'].strip())
    except Exception:
        return None

def _deploy_ddl(ddl: str, obj_type: str, obj_name: str, target_db: str, target_schema: str):
    """
    Replaces the entire CREATE ... OBJECT_NAME header with a fully qualified one.
    Strategy per type:
      TABLE     → split on first '(' — safest, avoids regex over column defs
      VIEW      → split on first ' AS ' after the header
      FUNCTION  → split on first '(' (parameters start)
      PROCEDURE → split on first '(' (parameters start)
    """
    fq = f"{target_db}.{target_schema}.{obj_name}"

    if obj_type == "TABLE":
        paren = ddl.find('(')
        if paren == -1:
            raise ValueError("No opening '(' found in TABLE DDL")
        body = ddl[paren:]                              # everything from ( onward
        body = _strip_unsupported_constraints(body)     # ← strip FK/CHECK
        final_ddl = f"CREATE OR REPLACE TABLE {fq} {body}"

    elif obj_type == "VIEW":
        # Find ' AS ' that comes AFTER the view name (not inside subqueries)
        # Reliable: match the CREATE...VIEW...name block then take rest
        header_match = re.search(
            r'(CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+[\w.\[\]"]+\s*)',
            ddl, re.IGNORECASE
        )
        if header_match:
            rest = ddl[header_match.end():]   # everything after the name
            # rest may start with optional WITH SCHEMABINDING — skip it
            rest = re.sub(r'^WITH\s+\w+\s*', '', rest, flags=re.IGNORECASE).strip()
            # ensure it starts with AS
            if not rest.upper().startswith('AS'):
                rest = 'AS ' + rest
            final_ddl = f"CREATE OR REPLACE VIEW {fq} {rest}"
        else:
            raise ValueError("Cannot parse VIEW header from DDL")

    elif obj_type == "FUNCTION":
        # Everything from first '(' is the parameter list + body
        paren = ddl.find('(')
        if paren == -1:
            raise ValueError("No opening '(' found in FUNCTION DDL")
        final_ddl = f"CREATE OR REPLACE FUNCTION {fq}{ddl[paren:]}"

    elif obj_type == "PROCEDURE":
        paren = ddl.find('(')
        if paren == -1:
            raise ValueError("No opening '(' found in PROCEDURE DDL")
        final_ddl = f"CREATE OR REPLACE PROCEDURE {fq}{ddl[paren:]}"

    else:
        final_ddl = ddl

    session.sql(final_ddl).collect()


def render_conversion_tab(
    tab_key: str,
    obj_type: str,          # "TABLE" | "VIEW" | "FUNCTION" | "PROCEDURE"
    convert_fn,             # function(source_code, source_type) → str
    save_type: str,         # string for save_conversion()
    upload_label: str = "Drop or browse files",
):
    """
    Renders a full multi-file conversion tab with:
    - Platform selector
    - Multi-file uploader
    - Convert All button
    - Shared deploy target (db + schema)
    - Per-DDL expander with code view + individual Deploy button
    - Deploy All button
    """
    state_key = f"multi_conversions_{tab_key}"

    # ── Top row: Platform selector + Convert All ──────────────────────────────
    ctl1, ctl2, ctl3 = st.columns([2, 2, 1])
    with ctl1:
        source_type = st.selectbox(
            "Source Platform",
            ["SQL Server", "Oracle", "MySQL", "PostgreSQL"],
            key=f"{tab_key}_source_platform"
        )
    with ctl2:
        uploaded_files = st.file_uploader(
            upload_label,
            type=["sql", "txt", "zip"],
            accept_multiple_files=True,
            key=f"{tab_key}_uploader",
            label_visibility="collapsed"
        )
        all_files = []
        zip_count = 0
        if uploaded_files:
            for uf in uploaded_files:
                if uf.name.lower().endswith('.zip'):
                    zip_entries = _extract_sql_from_zip(uf.read())
                    zip_count += len(zip_entries)
                    for ze in zip_entries:
                        all_files.append({"name": ze["name"], "content": ze["content"]})
                else:
                    all_files.append({"name": uf.name, "content": uf.read().decode("utf-8")})
            parts = []
            sql_count = len(all_files) - zip_count if zip_count else len(all_files)
            if sql_count > 0: parts.append(f"{sql_count} file(s)")
            if zip_count > 0: parts.append(f"{zip_count} from zip")
            st.caption(f"{len(all_files)} total — {', '.join(parts)}")
    with ctl3:
        st.markdown("<br>", unsafe_allow_html=True)
        convert_clicked = st.button(
            ":material/bolt: Convert All",
            key=f"{tab_key}_convert_all",
            type="primary",
            disabled=not all_files
        )

    # ── Run conversions ───────────────────────────────────────────────────────
    if convert_clicked and all_files:
        results = []
        _batch_start = time.time()
        prog = st.progress(0, text="Converting...")
        _skeleton_placeholder = st.empty()
        _skeleton_placeholder.markdown(show_skeleton(3, True), unsafe_allow_html=True)
        for i, f in enumerate(all_files):
            raw   = f["content"]
            clean = extract_main_ddl(raw)
            try:
                _t0 = time.time()
                converted = clean_sql_response(convert_fn(clean, source_type))
                _dur = round(time.time() - _t0, 2)
                name      = _extract_object_name(converted, obj_type)
                save_conversion(save_type, name, clean, name, converted, duration_secs=_dur)
                results.append({
                    "file":      f["name"],
                    "name":      name,
                    "source":    clean,
                    "converted": converted,
                    "status":    "ok",
                    "error":     "",
                    "duration":  _dur,
                })
            except Exception as e:
                results.append({
                    "file":      f["name"],
                    "name":      f["name"],
                    "source":    clean,
                    "converted": "",
                    "status":    "error",
                    "error":     str(e),
                    "duration":  0,
                })
            prog.progress((i + 1) / len(all_files), text=f"Converting {f['name']}...")
        prog.empty()
        _skeleton_placeholder.empty()
        _batch_total = round(time.time() - _batch_start, 2)
        st.session_state[state_key] = results
        st.session_state[f"{state_key}_timing"] = _batch_total
        ok  = sum(1 for r in results if r["status"] == "ok")
        bad = len(results) - ok
        _avg = round(_batch_total / len(results), 2) if results else 0
        if ok > 0 and bad == 0:
            show_toast(f"{ok} objects converted in {_batch_total}s", "success")
        elif bad > 0:
            show_toast(f"{ok} converted, {bad} failed — {_batch_total}s", "info", "⚠️")
        st.success(f"✅ {ok} converted{f' · ❌ {bad} failed' if bad else ''} — **{len(results)} objects in {_batch_total}s** (avg {_avg}s/object)")

    # ── Render results if any ─────────────────────────────────────────────────
    results: list = st.session_state.get(state_key, [])
    st.divider()

    # ── Shared Deploy Target ──────────────────────────────────────────────────
    st.markdown("**Deploy Target** — applied to all objects")
    dt1, dt2, dt3 = st.columns([2, 2, 2])
    with dt1:
        target_db = st.selectbox("Database", get_databases(), key=f"{tab_key}_target_db")
    with dt2:
        schemas_list = get_schemas(target_db)
        target_schema = st.selectbox("Schema", schemas_list, key=f"{tab_key}_target_schema_{target_db}")
    with dt3:
        st.markdown("<br>", unsafe_allow_html=True)
        deploy_all = st.button(
            ":material/rocket_launch: Deploy All",
            key=f"{tab_key}_deploy_all",
            type="primary",
            disabled=not any(r["status"] == "ok" for r in results)
        )

    # ── Auto-Heal settings ────────────────────────────────────────────────────
    heal_col1, heal_col2 = st.columns([1, 1])
    with heal_col1:
        auto_heal_enabled = st.toggle(
            "⚡ Auto-Heal on deploy errors",
            value=True,
            key=f"{tab_key}_auto_heal_toggle",
            help="When enabled, Cortex AI will attempt to fix deployment errors automatically."
        )
    with heal_col2:
        heal_max_attempts = st.select_slider(
            "Max heal attempts",
            options=[1, 2, 3],
            value=1,
            key=f"{tab_key}_heal_attempts",
            disabled=not auto_heal_enabled,
            help="Number of Cortex retry attempts per failed object. Lower = faster deploys."
        )

    # ── Deploy All handler (with Cortex Auto-Heal) ─────────────────────────────
    heal_results_key = f"{state_key}_heal_results"
    excluded_key = f"{state_key}_excluded"   # set of excluded healed object names
    if excluded_key not in st.session_state:
        st.session_state[excluded_key] = set()

    if deploy_all:
        heal_results = []
        prog_deploy = st.progress(0, text="Deploying...")
        _excluded = st.session_state.get(excluded_key, set())
        deployable = [r for r in results if r["status"] == "ok" and r["name"] not in _excluded]
        _heal_on  = auto_heal_enabled
        _heal_max = heal_max_attempts if _heal_on else 0
        for di, r in enumerate(deployable):
            entry = {"name": r["name"], "status": "deployed", "attempts": [], "final_ddl": r["converted"], "error": ""}
            try:
                _deploy_ddl(r["converted"], obj_type, r["name"], target_db, target_schema)
            except Exception as e:
                err = str(e)
                if _heal_on and _is_healable_error(err):
                    success, final_ddl, attempts = _cortex_auto_heal(
                        r["converted"], err, obj_type, r["name"], target_db, target_schema,
                        max_attempts=_heal_max
                    )
                    entry["attempts"] = attempts
                    entry["final_ddl"] = final_ddl
                    entry["original_ddl"] = r["converted"]
                    if success:
                        # Verification succeeded — DROP immediately so nothing
                        # persists without explicit human approval.
                        try:
                            session.sql(
                                f'DROP {obj_type} IF EXISTS {target_db}.{target_schema}.{r["name"]}'
                            ).collect()
                        except Exception:
                            pass
                        entry["status"] = "pending_review"
                        _save_heal_knowledge(err, r["converted"], final_ddl, obj_type, attempts)
                    else:
                        entry["status"] = "failed"
                        entry["error"] = attempts[-1]["error"] if attempts else err
                else:
                    entry["status"] = "failed"
                    entry["error"] = err
            heal_results.append(entry)
            prog_deploy.progress((di + 1) / len(deployable), text=f"Deploying {r['name']}...")
        prog_deploy.empty()
        st.session_state[heal_results_key] = heal_results
        _n_ok = sum(1 for h in heal_results if h["status"] in ("deployed", "pending_review"))
        _n_fail = sum(1 for h in heal_results if h["status"] == "failed")
        if _n_ok > 0 and _n_fail == 0:
            show_toast(f"All {_n_ok} objects deployed successfully!", "success")
        elif _n_ok > 0 and _n_fail > 0:
            show_toast(f"{_n_ok} deployed, {_n_fail} failed", "info", "⚠️")
        elif _n_fail > 0:
            show_toast(f"{_n_fail} objects failed to deploy", "error")

    heal_results: list = st.session_state.get(heal_results_key, [])
    # ── Flash message for just-approved objects ──────────────────────────
    _flash_approved_key = f"{state_key}_flash_approved"
    _flash_name = st.session_state.pop(_flash_approved_key, None)
    if _flash_name:
        show_toast(f"{_flash_name} approved & deployed to {target_db}.{target_schema}", "success")
    if heal_results:
        n_deployed = sum(1 for h in heal_results if h["status"] == "deployed")
        n_pending  = sum(1 for h in heal_results if h["status"] == "pending_review")
        n_failed   = sum(1 for h in heal_results if h["status"] == "failed")
        summary_parts = []
        if n_deployed: summary_parts.append(f"**{n_deployed} deployed**")
        if n_pending:  summary_parts.append(f"**{n_pending} pending review** ⚡")
        if n_failed:   summary_parts.append(f"**{n_failed} failed**")
        st.markdown(
            f"Deploy Complete  ·  {' · '.join(summary_parts)}  ·  Target: `{target_db}.{target_schema}`"
        )
        for h in heal_results:
            fq = f"{target_db}.{target_schema}.{h['name']}"
            if h["status"] == "deployed":
                _was_healed = bool(h.get("attempts"))
                _heal_tag = (' <span style="color:#F59E0B;font-weight:600;margin-left:8px;">'
                             '⚡ Reviewed & Approved</span>') if _was_healed else ''
                st.markdown(
                    f'<div style="padding:8px 14px;margin:3px 0;border-radius:8px;'
                    f'background:rgba(122,185,41,0.08);border-left:4px solid #7AB929;">'
                    f'<span style="color:#5C9A1E;font-size:1.05rem;">✅</span> '
                    f'<span style="color:#1A1A1A;font-weight:700;font-size:0.95rem;">{h["name"]}</span>'
                    f' <span style="color:#9CA3AF;">→</span> '
                    f'<code style="color:#6B7280;font-size:0.85rem;">{fq}</code>'
                    f'{_heal_tag}'
                    f'</div>', unsafe_allow_html=True
                )
            elif h["status"] == "pending_review":
                _h_name = h["name"]
                st.markdown(
                    f'<div style="padding:8px 14px;margin:4px 0;border-radius:8px;'
                    f'background:rgba(245,158,11,0.08);border-left:4px solid #F59E0B;">'
                    f'<span style="font-size:1.05rem;">⚡</span> '
                    f'<span style="color:#1A1A1A;font-weight:700;">{_h_name}</span>'
                    f' <span style="color:#92400E;font-size:0.85rem;margin-left:8px;">'
                    f'Auto-healed by Cortex — pending your review</span>'
                    f'</div>', unsafe_allow_html=True
                )

                with st.expander(f"🔍 Review healed DDL for {_h_name}", expanded=True):
                    st.markdown("**Original DDL (failed on deploy)**")
                    st.code(h.get("original_ddl", ""), language="sql")
                    st.markdown("---")
                    st.markdown("**Healed DDL by Cortex ⚡**")
                    st.code(h.get("final_ddl", ""), language="sql")

                    if h["attempts"]:
                        with st.expander("Heal log", expanded=False):
                            for a in h["attempts"]:
                                st.caption(f"Attempt {a['attempt']}: {a['error'][:300]}")

                    _btn_left, _btn_right, _ = st.columns([1, 1, 3])
                    with _btn_left:
                        if st.button("✅ Approve & Deploy", key=f"{tab_key}_approve_{_h_name}",
                                     type="primary"):
                            try:
                                _deploy_ddl(h["final_ddl"], obj_type, _h_name,
                                            target_db, target_schema)
                                save_conversion(
                                    save_type, _h_name, h.get("original_ddl", ""),
                                    _h_name, h["final_ddl"],
                                    status="DEPLOYED_HEALED", was_healed=True,
                                    heal_attempts=len(h["attempts"]),
                                    original_ddl=h.get("original_ddl", ""),
                                )
                                h["status"] = "deployed"
                                st.session_state[heal_results_key] = heal_results
                                # Flash message — survives the rerun
                                st.session_state[_flash_approved_key] = _h_name
                                st.rerun()
                            except Exception as deploy_err:
                                st.error(f"Deploy failed: {deploy_err}")
                    with _btn_right:
                        if st.button("❌ Reject", key=f"{tab_key}_reject_{_h_name}",
                                     type="secondary"):
                            h["status"] = "rejected"
                            st.session_state[heal_results_key] = heal_results
                            st.rerun()

            elif h["status"] == "rejected":
                st.markdown(
                    f'<div style="padding:6px 12px;margin:2px 0;border-radius:6px;'
                    f'background:rgba(107,114,128,0.06);border-left:3px solid #9CA3AF;'
                    f'opacity:0.6;">'
                    f'<span style="color:#9CA3AF;">❌</span> '
                    f'<span style="color:#6B7280;font-weight:600;">{h["name"]}</span>'
                    f' <span style="color:#9CA3AF;">\u2014 rejected by reviewer</span>'
                    f'</div>', unsafe_allow_html=True
                )
            else:
                st.markdown(
                    f'<div style="padding:6px 12px;margin:2px 0;border-radius:6px;'
                    f'background:rgba(220,38,38,0.06);border-left:3px solid #DC2626;">'
                    f'<span style="color:#DC2626;">❌</span> '
                    f'<span style="color:#1A1A1A;font-weight:600;">{h["name"]}</span>'
                    f'</div>', unsafe_allow_html=True
                )
                with st.expander(f"Error details for {h['name']}"):
                    st.error(h["error"][:500])
                    if h["attempts"]:
                        st.markdown(f"**Heal attempts:** {len(h['attempts'])}")
                        for a in h["attempts"]:
                            st.markdown(f"Attempt {a['attempt']}: {a['error'][:200]}")

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Per-file expanders ────────────────────────────────────────────────────
    for idx, r in enumerate(results):
        _d = r.get("duration", 0)
        _d_str = f"  ·  {_d}s" if _d else ""
        if r["status"] == "ok":
            label = f" {r['name']}  ·  `{r['file']}`{_d_str}"
        else:
            label = f"❌  {r['file']}  ·  conversion failed"

        with st.expander(label, expanded=(len(results) == 1)):
            if r["status"] == "ok":
                # ── Session-state keys for individual heal review ──────────
                _ind_heal_key  = f"{tab_key}_ind_heal_{idx}"
                _ind_flash_key = f"{tab_key}_ind_flash_{idx}"

                # Show flash success from a previous approve action
                _ind_flash = st.session_state.pop(_ind_flash_key, None)
                if _ind_flash:
                    st.success(f"✅ **{r['name']}** approved & deployed to "
                               f"`{target_db}.{target_schema}`")

                c_left, c_right = st.columns([3, 1])
                with c_left:
                    st.code(r["converted"], language="sql")
                with c_right:
                    st.markdown("**Source File**")
                    st.caption(r["file"])
                    st.markdown("**Object Name**")
                    st.code(r["name"], language="text")
                    st.markdown(f"**Target**")
                    st.caption(f"{target_db}.{target_schema}.{r['name']}")

                    # ── Pending heal review? Hide Deploy, show review UI below ──
                    if _ind_heal_key not in st.session_state:
                        if st.button(
                            ":material/rocket_launch: Deploy",
                            key=f"{tab_key}_deploy_{idx}",
                            type="primary",
                        ):
                            try:
                                _deploy_ddl(r["converted"], obj_type, r["name"],
                                            target_db, target_schema)
                                st.success(f"Deployed to {target_db}.{target_schema}")
                            except Exception as e:
                                err = str(e)
                                if auto_heal_enabled and _is_healable_error(err):
                                    with st.spinner("⚡ Cortex Auto-Heal in progress..."):
                                        success, final_ddl, attempts = _cortex_auto_heal(
                                            r["converted"], err, obj_type, r["name"],
                                            target_db, target_schema,
                                            max_attempts=heal_max_attempts,
                                        )
                                    if success:
                                        try:
                                            session.sql(
                                                f'DROP {obj_type} IF EXISTS '
                                                f'{target_db}.{target_schema}.{r["name"]}'
                                            ).collect()
                                        except Exception:
                                            pass
                                        _save_heal_knowledge(
                                            err, r["converted"], final_ddl,
                                            obj_type, attempts,
                                        )
                                        # Persist heal result for review on next render
                                        st.session_state[_ind_heal_key] = {
                                            "original_ddl": r["converted"],
                                            "final_ddl": final_ddl,
                                            "attempts": attempts,
                                        }
                                        st.rerun()
                                    else:
                                        st.error(
                                            f"Auto-heal failed after {len(attempts)} "
                                            f"attempts: {err[:200]}"
                                        )
                                        for a in attempts:
                                            st.caption(
                                                f"Attempt {a['attempt']}: "
                                                f"{a['error'][:150]}"
                                            )
                                else:
                                    st.error(err)

                # ── Heal review UI (rendered OUTSIDE the Deploy button) ────
                if _ind_heal_key in st.session_state:
                    _hd = st.session_state[_ind_heal_key]
                    st.warning(
                        f"⚡ Cortex healed **{r['name']}** — review the "
                        f"fix below before deploying."
                    )
                    st.markdown("**Original DDL (failed on deploy)**")
                    st.code(_hd["original_ddl"], language="sql")
                    st.markdown("---")
                    st.markdown("**Healed DDL by Cortex ⚡**")
                    st.code(_hd["final_ddl"], language="sql")
                    if _hd["attempts"]:
                        with st.expander("Heal log", expanded=False):
                            for a in _hd["attempts"]:
                                st.caption(
                                    f"Attempt {a['attempt']}: {a['error'][:300]}"
                                )
                    _appr_l, _appr_r, _ = st.columns([1, 1, 3])
                    with _appr_l:
                        if st.button("✅ Approve & Deploy",
                                     key=f"{tab_key}_ind_approve_{idx}",
                                     type="primary"):
                            try:
                                _deploy_ddl(
                                    _hd["final_ddl"], obj_type, r["name"],
                                    target_db, target_schema,
                                )
                                save_conversion(
                                    save_type, r["name"], r.get("source", ""),
                                    r["name"], _hd["final_ddl"],
                                    status="DEPLOYED_HEALED", was_healed=True,
                                    heal_attempts=len(_hd["attempts"]),
                                    original_ddl=_hd["original_ddl"],
                                )
                                del st.session_state[_ind_heal_key]
                                st.session_state[_ind_flash_key] = True
                                st.rerun()
                            except Exception as dep_err:
                                st.error(f"Deploy failed: {dep_err}")
                    with _appr_r:
                        if st.button("❌ Reject",
                                     key=f"{tab_key}_ind_reject_{idx}",
                                     type="secondary"):
                            del st.session_state[_ind_heal_key]
                            st.rerun()

                with st.expander("Source Preview"):
                    st.code(r["source"], language="sql")
            else:
                st.error(f"Conversion Error: {r['error']}")
                with st.expander("Source Preview"):
                    st.code(r["source"], language="sql")
    
# ═══════════════════════════════════════════════════════════════════════════════
# TABS
# ═══════════════════════════════════════════════════════════════════════════════
tab1, tab2, tab3, tab4, tab5, tab6, tab8, tab9 = st.tabs([
    ":material/table: Tables",
    ":material/visibility: Views",
    ":material/functions: Functions",
    ":material/code: Procedures",
    ":material/swap_horiz: SP → dbt",
    ":material/play_arrow: Execute",
    ":material/dashboard: Dashboard",
    ":material/rocket_launch: Roadmap"
])


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1: TABLES
# ═══════════════════════════════════════════════════════════════════════════════
with tab1:
    st.markdown('<div class="tab-info"><span class="info-icon">ℹ️</span>Convert SQL Server table DDLs to Snowflake-compatible CREATE TABLE statements with automatic type mapping and constraint handling.</div>', unsafe_allow_html=True)
    render_conversion_tab(
        tab_key      = "table",
        obj_type     = "TABLE",
        convert_fn   = convert_ddl_to_snowflake,
        save_type    = "TABLE_DDL",
        upload_label = "Drop or browse Table DDL files (.sql / .txt)",
    )

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2: VIEWS
# ═══════════════════════════════════════════════════════════════════════════════
with tab2:
    st.markdown('<div class="tab-info"><span class="info-icon">ℹ️</span>Convert source view definitions to Snowflake views — translates functions, joins, and syntax to Snowflake SQL.</div>', unsafe_allow_html=True)

    # ── Mode toggle: DDL vs dbt model ─────────────────────────────────────
    _view_mode = st.radio(
        "Conversion mode",
        ["Snowflake DDL", "dbt Model (cross-references existing models)"],
        horizontal=True,
        key="view_conversion_mode",
        help="**Snowflake DDL**: Raw CREATE VIEW. **dbt Model**: Generates a gold-layer .sql with ref() to already-converted models."
    )

    if _view_mode == "Snowflake DDL":
        render_conversion_tab(
            tab_key      = "view",
            obj_type     = "VIEW",
            convert_fn   = convert_ddl_to_snowflake,
            save_type    = "VIEW_DDL",
            upload_label = "Drop or browse View DDL files (.sql / .txt)",
        )
    else:
        # ── dbt-aware view conversion ─────────────────────────────────────
        _registry = _build_model_registry()
        if _registry:
            _gold_models = {v for v in _registry.values() if not v.startswith("stg_") and not v.startswith("int_")}
            st.success(f"✅ **{len(_gold_models)} existing dbt models** detected — views referencing these will use `ref()` automatically.")
        else:
            st.info("ℹ️ No existing dbt models found yet. Convert SPs in Tab 5 first so views can cross-reference them.")

        _v_ctl1, _v_ctl2, _v_ctl3 = st.columns([2, 2, 1])
        with _v_ctl1:
            _v_src_type = st.selectbox("Source Platform", ["SQL Server", "Oracle", "MySQL", "PostgreSQL"], key="view_dbt_platform")
        with _v_ctl2:
            _v_uploaded = st.file_uploader(
                "Drop or browse View DDL files (.sql / .txt)",
                type=["sql", "txt", "zip"],
                accept_multiple_files=True,
                key="view_dbt_uploader",
                label_visibility="collapsed"
            )
            _v_all_files = []
            if _v_uploaded:
                for uf in _v_uploaded:
                    if uf.name.lower().endswith('.zip'):
                        for ze in _extract_sql_from_zip(uf.read()):
                            _v_all_files.append({"name": ze["name"], "content": ze["content"]})
                    else:
                        _v_all_files.append({"name": uf.name, "content": uf.read().decode("utf-8")})
                st.caption(f"{len(_v_all_files)} file(s) ready")
        with _v_ctl3:
            st.markdown("<br>", unsafe_allow_html=True)
            _v_convert = st.button(":material/bolt: Convert to dbt", key="view_dbt_convert", type="primary", disabled=not _v_all_files)

        if _v_convert and _v_all_files:
            _v_results = []
            _v_prog = st.progress(0, text="Converting views to dbt models…")
            for vi, vf in enumerate(_v_all_files):
                raw = vf["content"]
                clean = extract_main_ddl(raw)
                try:
                    _t0 = time.time()
                    dbt_result = convert_view_to_dbt_model(clean, _v_src_type)
                    _dur = round(time.time() - _t0, 2)
                    if dbt_result:
                        _v_results.append({
                            "file": vf["name"],
                            "model_name": dbt_result.get("model_name", "unknown"),
                            "sql": dbt_result.get("sql", ""),
                            "description": dbt_result.get("description", ""),
                            "base_tables": dbt_result.get("base_tables", []),
                            "status": "ok",
                            "duration": _dur,
                        })
                    else:
                        _v_results.append({"file": vf["name"], "status": "error", "error": "LLM returned invalid JSON"})
                except Exception as e:
                    _v_results.append({"file": vf["name"], "status": "error", "error": str(e)})
                _v_prog.progress((vi + 1) / len(_v_all_files), text=f"Converting {vf['name']}…")
            _v_prog.empty()
            st.session_state["view_dbt_results"] = _v_results
            _v_ok = sum(1 for r in _v_results if r["status"] == "ok")
            _v_err = len(_v_results) - _v_ok
            _total_models = sum(1 for r in _v_results if r["status"] == "ok")
            st.success(f"✅ {_v_ok} views converted to dbt models{f' · ❌ {_v_err} failed' if _v_err else ''}")

        # ── Render dbt view results ───────────────────────────────────────
        _v_results = st.session_state.get("view_dbt_results", [])
        if _v_results:
            st.divider()
            _add_to_proj = st.button("📦 Add all to dbt project", key="view_dbt_add_project", type="primary")
            if _add_to_proj:
                _added = 0
                for r in _v_results:
                    if r["status"] != "ok":
                        continue
                    _model_path = f"models/gold/{r['model_name']}.sql"
                    if "dbt_project_files" not in st.session_state:
                        st.session_state.dbt_project_files = {}
                    st.session_state.dbt_project_files[_model_path] = r["sql"]
                    _added += 1
                show_toast(f"{_added} view models added to dbt project", "success")
                st.rerun()

            for r in _v_results:
                if r["status"] == "ok":
                    _uses_ref = "{{ ref(" in r.get("sql", "")
                    _ref_badge = ' <span style="color:#7AB929;font-weight:600;">✅ uses ref()</span>' if _uses_ref else ' <span style="color:#F59E0B;">⚠️ uses source()</span>'
                    with st.expander(f"✅ {r['model_name']}  ·  {r['file']}  ·  {r['duration']}s{' · ref() linked' if _uses_ref else ''}"):
                        st.markdown(f"**Description:** {r.get('description', '')}")
                        if r.get("base_tables"):
                            st.markdown(f"**Base tables:** {', '.join(r['base_tables'])}")
                        st.code(r["sql"], language="sql")
                else:
                    st.error(f"❌ {r['file']} — {r.get('error', 'Unknown error')}")

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3: FUNCTIONS  (new tab from previous change)
# ═══════════════════════════════════════════════════════════════════════════════
with tab3:
    st.markdown('<div class="tab-info"><span class="info-icon">ℹ️</span>Convert User Defined Functions to Snowflake SQL UDFs (preferred) or Python UDFs — flattens procedural logic into clean expressions.</div>', unsafe_allow_html=True)
    render_conversion_tab(
        tab_key      = "udf",
        obj_type     = "FUNCTION",
        convert_fn   = convert_udf_to_snowflake,
        save_type    = "FUNCTION",
        upload_label = "Drop or browse UDF / User Function files (.sql / .txt)",
    )

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4: STORED PROCEDURES
# ═══════════════════════════════════════════════════════════════════════════════
with tab4:
    st.markdown('<div class="tab-info"><span class="info-icon">ℹ️</span>Convert stored procedures to Snowflake Python stored procedures using Snowpark — translates T-SQL control flow to Python with session.sql() calls.</div>', unsafe_allow_html=True)
    render_conversion_tab(
        tab_key      = "sp",
        obj_type     = "PROCEDURE",
        convert_fn   = convert_sp_to_snowflake,
        save_type    = "STORED_PROCEDURE",
        upload_label = "Drop or browse Stored Procedure files (.sql / .txt)",
    )

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1: TABLE DDLs
# ═══════════════════════════════════════════════════════════════════════════════
# with tab1:
#     col1, col2 = st.columns([1, 1])
    
#     with col1:
#         st.markdown("**Source**")
#         source_type = st.selectbox("Platform", ["SQL Server", "Oracle", "MySQL", "PostgreSQL"], key="table_source")
        
#         uploaded_table = st.file_uploader("Drop or browse DDL file", type=['sql', 'txt'], key="table_upload")
        
#         if uploaded_table:
#             table_ddl = uploaded_table.getvalue().decode('utf-8')
#             with st.expander("Preview"):
#                 st.code(table_ddl[:1000] + ("..." if len(table_ddl) > 1000 else ""), language="sql")
            
#             if st.button(":material/bolt: Convert", key="convert_table", type="primary"):
#                 if uploaded_table:
#                     table_ddl = uploaded_table.getvalue().decode('utf-8')
#                     table_ddl = extract_main_ddl(table_ddl)
                    
#                     with st.spinner("Converting..."):
#                         converted = convert_ddl_to_snowflake(table_ddl, source_type)
#                         st.session_state.converted_table_ddl = converted
#                         table_name_match = re.search(r'CREATE\s+TABLE\s+(\w+)', table_ddl, re.IGNORECASE)
#                         table_name = table_name_match.group(1) if table_name_match else "UNKNOWN_TABLE"
#                         save_conversion("TABLE_DDL", table_name, table_ddl, table_name, converted)
#                         st.session_state.converted_objects["TABLE_DDL"].append({
#                             "source": table_name, "target": table_name, "ddl": converted
#                         })
    
#     with col2:
#         st.markdown("**Snowflake Output**")
#         if 'converted_table_ddl' in st.session_state:
#             st.code(st.session_state.converted_table_ddl, language="sql")
            
#             st.markdown("**Deploy Target**")
#             col_db, col_schema = st.columns(2)
#             with col_db:
#                 table_target_db = st.selectbox("Database", get_databases(), key="table_target_db")
#             with col_schema:
#                 table_target_schema = st.selectbox("Schema", get_schemas(table_target_db), key="table_target_schema")
            
#             if st.button(":material/rocket_launch: Deploy", key="deploy_table"):
#                 try:
#                     ddl = st.session_state.converted_table_ddl
#                     ddl = re.sub(
#                         r'CREATE\s+(OR\s+REPLACE\s+)?TABLE\s+(\w+)',
#                         f'CREATE OR REPLACE TABLE {table_target_db}.{table_target_schema}.\\2',
#                         ddl,
#                         count=1,
#                         flags=re.IGNORECASE
#                     )
#                     session.sql(ddl).collect()
#                     st.markdown(f"Deployed to {table_target_db}.{table_target_schema}")
#                 except Exception as e:
#                     st.error(str(e))

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2: VIEW DDLs
# ═══════════════════════════════════════════════════════════════════════════════
# with tab2:
#     col1, col2 = st.columns([1, 1])
    
#     with col1:
#         st.markdown("**Source**")
#         view_source_type = st.selectbox("Platform", ["SQL Server", "Oracle", "MySQL", "PostgreSQL"], key="view_source")
        
#         uploaded_view = st.file_uploader("Drop or browse DDL file", type=['sql', 'txt'], key="view_upload")
        
#         if uploaded_view:
#             raw_view_ddl = uploaded_view.getvalue().decode('utf-8')
#             view_ddl = extract_main_ddl(raw_view_ddl)
#             with st.expander("Preview"):
#                 st.code(view_ddl[:1000] + ("..." if len(view_ddl) > 1000 else ""), language="sql")
            
#             if st.button(":material/bolt: Convert", key="convert_view", type="primary"):
#                 with st.spinner("Converting..."):
#                     converted = convert_ddl_to_snowflake(view_ddl, view_source_type)
#                     converted = clean_sql_response(converted)
#                     st.session_state.converted_view_ddl = converted
#                     view_name_match = re.search(r'CREATE\s+(OR\s+REPLACE\s+)?VIEW\s+\[?(\w+)\]?', view_ddl, re.IGNORECASE)
#                     view_name = view_name_match.group(2) if view_name_match else "UNKNOWN_VIEW"
#                     save_conversion("VIEW_DDL", view_name, view_ddl, view_name, converted)
#                     st.session_state.converted_objects["VIEW_DDL"].append({
#                         "source": view_name, "target": view_name, "ddl": converted
#                     })
    
#     with col2:
#         st.markdown("**Snowflake Output**")
#         if 'converted_view_ddl' in st.session_state:
#             st.code(st.session_state.converted_view_ddl, language="sql")
            
#             st.markdown("**Deploy Target**")
#             col_db, col_schema = st.columns(2)
#             with col_db:
#                 view_target_db = st.selectbox("Database", get_databases(), key="view_target_db")
#             with col_schema:
#                 view_target_schema = st.selectbox("Schema", get_schemas(view_target_db), key="view_target_schema")
            
#             if st.button(":material/rocket_launch: Deploy", key="deploy_view"):
#                 try:
#                     ddl = st.session_state.converted_view_ddl
#                     ddl = re.sub(
#                         r'CREATE\s+(OR\s+REPLACE\s+)?VIEW\s+(\w+)',
#                         f'CREATE OR REPLACE VIEW {view_target_db}.{view_target_schema}.\\2',
#                         ddl,
#                         count=1,
#                         flags=re.IGNORECASE
#                     )
#                     session.sql(ddl).collect()
#                     st.markdown(f"Deployed to {view_target_db}.{view_target_schema}")
#                 except Exception as e:
#                     st.error(str(e))

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3: FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════
# with tab3:

#     col1, col2 = st.columns([1, 1])

#     with col1:
#         st.markdown("**Source**")
#         udf_source_type = st.selectbox(
#             "Platform", ["SQL Server", "Oracle", "MySQL", "PostgreSQL"],
#             key="udf_source"
#         )

#         uploaded_udf = st.file_uploader(
#             "Drop or browse UDF / User Function file",
#             type=['sql', 'txt'],
#             key="udf_upload"
#         )

#         if uploaded_udf:
#             raw_udf_code = uploaded_udf.getvalue().decode('utf-8')
#             udf_code     = extract_main_ddl(raw_udf_code)

#             with st.expander("Preview"):
#                 st.code(udf_code[:1500] + ("..." if len(udf_code) > 1500 else ""), language="sql")

#             if st.button(":material/bolt: Convert", key="convert_udf", type="primary"):
#                 with st.spinner("Converting UDF to Snowflake native function..."):
#                     converted_udf = convert_udf_to_snowflake(udf_code, udf_source_type)
#                     converted_udf = clean_sql_response(converted_udf)
#                     st.session_state.converted_udf = converted_udf

#                     udf_name_match = re.search(
#                         r'CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(?:\[?dbo\]?\.)?\[?(\w+)\]?',
#                         udf_code, re.IGNORECASE
#                     )
#                     udf_name = udf_name_match.group(1) if udf_name_match else "UNKNOWN_UDF"
#                     save_conversion("FUNCTION", udf_name, udf_code, udf_name, converted_udf)

#     with col2:
#         st.markdown("**Snowflake Output**")
#         if 'converted_udf' in st.session_state:
#             st.code(st.session_state.converted_udf, language="sql")

#             # ── UDF type badge ────────────────────────────────────────────
#             udf_out = st.session_state.converted_udf.upper()
#             if "LANGUAGE PYTHON" in udf_out:
#                 st.markdown(
#                     '<span class="status-badge status-pending">Python UDF</span>',
#                     unsafe_allow_html=True
#                 )
#             elif "RETURNS TABLE" in udf_out:
#                 st.markdown(
#                     '<span class="status-badge status-success">Table Function</span>',
#                     unsafe_allow_html=True
#                 )
#             else:
#                 st.markdown(
#                     '<span class="status-badge status-success">SQL UDF</span>',
#                     unsafe_allow_html=True
#                 )

#             st.markdown("**Deploy Target**")
#             col_db, col_schema = st.columns(2)
#             with col_db:
#                 udf_target_db = st.selectbox("Database", get_databases(), key="udf_target_db")
#             with col_schema:
#                 udf_target_schema = st.selectbox("Schema", get_schemas(udf_target_db), key="udf_target_schema")

#             if st.button(":material/rocket_launch: Deploy", key="deploy_udf"):
#                 try:
#                     ddl = st.session_state.converted_udf
#                     # Inject fully qualified name
#                     ddl = re.sub(
#                         r'CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(\w+)',
#                         f'CREATE OR REPLACE FUNCTION {udf_target_db}.{udf_target_schema}.\\1',
#                         ddl, count=1, flags=re.IGNORECASE
#                     )
#                     session.sql(ddl).collect()
#                     st.markdown(f"Deployed to {udf_target_db}.{udf_target_schema}")
#                 except Exception as e:
#                     st.error(str(e))

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4: STORED PROCEDURES
# ═══════════════════════════════════════════════════════════════════════════════
# with tab4:

#     col1, col2 = st.columns([1, 1])
    
#     with col1:
#         st.markdown("**Source**")
#         sp_source_type = st.selectbox("Platform", ["SQL Server", "Oracle", "MySQL", "PostgreSQL"], key="sp_source")
        
#         uploaded_sp = st.file_uploader("Drop or browse procedure file", type=['sql', 'txt'], key="sp_upload")
        
#         if uploaded_sp:
#             raw_sp_code = uploaded_sp.getvalue().decode('utf-8')
#             sp_code = extract_main_ddl(raw_sp_code)
#             with st.expander("Preview"):
#                 st.code(sp_code[:1500] + ("..." if len(sp_code) > 1500 else ""), language="sql")
            
#             if st.button(":material/bolt: Convert", key="convert_sp", type="primary"):
#                 with st.spinner("Converting..."):
#                     converted = convert_sp_to_snowflake(sp_code, sp_source_type)
#                     converted = clean_sql_response(converted)
#                     st.session_state.converted_sp = converted
#                     sp_name_match = re.search(r'CREATE\s+(OR\s+REPLACE\s+)?PROC(?:EDURE)?\s+\[?(?:dbo\.)?\]?\[?(\w+)\]?', sp_code, re.IGNORECASE)
#                     sp_name = sp_name_match.group(2) if sp_name_match else "UNKNOWN_SP"
#                     save_conversion("STORED_PROCEDURE", sp_name, sp_code, sp_name, converted)
#                     st.session_state.converted_objects["STORED_PROCEDURE"].append({
#                         "source": sp_name, "target": sp_name, "code": converted
#                     })
    
#     with col2:
#         st.markdown("**Snowflake Output**")
#         if 'converted_sp' in st.session_state:
#             st.code(st.session_state.converted_sp, language="sql")
            
#             st.markdown("**Deploy Target**")
#             col_db, col_schema = st.columns(2)
#             with col_db:
#                 sp_target_db = st.selectbox("Database", get_databases(), key="sp_target_db")
#             with col_schema:
#                 sp_target_schema = st.selectbox("Schema", get_schemas(sp_target_db), key="sp_target_schema")
            
#             if st.button(":material/rocket_launch: Deploy", key="deploy_sp"):
#                 try:
#                     ddl = st.session_state.converted_sp
#                     ddl = re.sub(
#                         r'CREATE\s+(OR\s+REPLACE\s+)?PROCEDURE\s+(\w+)',
#                         f'CREATE OR REPLACE PROCEDURE {sp_target_db}.{sp_target_schema}.\\2',
#                         ddl,
#                         count=1,
#                         flags=re.IGNORECASE
#                     )
#                     session.sql(ddl).collect()
#                     st.markdown(f"Deployed to {sp_target_db}.{sp_target_schema}")
#                 except Exception as e:
#                     st.error(str(e))

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 5: SP TO DBT MEDALLION
# ═══════════════════════════════════════════════════════════════════════════════
with tab5:
    _dbt_steps = ["Configure", "Upload", "Validate", "Convert", "Review"]
    _stepper_placeholder = st.empty()
    st.markdown('<div class="tab-info"><span class="info-icon">ℹ️</span>Decompose a stored procedure into a full dbt project with bronze/silver/gold medallion architecture — generates staging, intermediate, and mart models.</div>', unsafe_allow_html=True)

    st.markdown("**DBT Project Configuration**")
    
    col1, col2 = st.columns([3, 1])
    with col1:
        project_name_input = st.text_input("Project Name", key="dbt_project_name", placeholder="my_dbt_project", help="Name for the dbt project (used in dbt_project.yml)")
    with col2:
        dbt_source_type = st.selectbox("Platform", ["SQL Server", "Oracle"], key="dbt_source")
    
    st.markdown("**Source Configuration**")
    st.caption("Specify the Snowflake database and schema where your source tables, views, and functions exist.")
    src_col1, src_col2 = st.columns(2)
    with src_col1:
        source_database = st.selectbox(
            "Source Database",
            options=get_databases(),
            key="dbt_source_database",
            help="Database containing source tables and functions for dbt models"
        )
    with src_col2:
        source_schema = st.selectbox(
            "Source Schema",
            options=get_schemas(source_database),
            key="dbt_source_schema",
            help="Schema containing source tables and functions for dbt models"
        )
    
    st.session_state['source_database'] = source_database
    st.session_state['source_schema'] = source_schema

    st.warning("⚠️ **Before uploading stored procedures**, ensure all dependent objects (tables, views, functions) referenced by your SPs have already been converted and exist in the selected source database/schema. Missing dependencies will block model conversion.")

    # ── Multi-file uploader (SQL + ZIP) ───────────────────────────────────────
    up_col, btn_col = st.columns([4, 1])
    with up_col:
        uploaded_dbt_files = st.file_uploader(
            "Drop or browse procedure files",
            type=['sql', 'txt', 'zip'],
            accept_multiple_files=True,
            key="dbt_sp_upload",
        )
        all_sp_files = []
        zip_count = 0
        if uploaded_dbt_files:
            for uf in uploaded_dbt_files:
                if uf.name.lower().endswith('.zip'):
                    zip_entries = _extract_sql_from_zip(uf.read())
                    zip_count += len(zip_entries)
                    for ze in zip_entries:
                        all_sp_files.append({"name": ze["name"], "content": ze["content"]})
                else:
                    all_sp_files.append({"name": uf.name, "content": uf.read().decode("utf-8")})
            parts = []
            sql_count = len(all_sp_files) - zip_count if zip_count else len(all_sp_files)
            if sql_count > 0:
                parts.append(f"{sql_count} file(s)")
            if zip_count > 0:
                parts.append(f"{zip_count} from zip")
            st.caption(f"{len(all_sp_files)} total — {', '.join(parts)}")
    with btn_col:
        st.markdown("<br>", unsafe_allow_html=True)
        generate_clicked = st.button(
            ":material/layers: Generate Models",
            key="convert_to_dbt",
            type="primary",
            disabled=not all_sp_files or not project_name_input,
        )

    # ── Two-phase batch: validate-all → convert-valid ─────────────────────────
    dbt_results_key = "dbt_sp_results"

    if generate_clicked and all_sp_files:
        if not project_name_input:
            st.warning("Enter project name")
        else:
            src_db = st.session_state.get('source_database', session.get_current_database().replace('"', ''))
            src_schema = st.session_state.get('source_schema', 'DBO')
            results = []          # accumulates every SP entry
            _batch_start = time.time()

            # ══════════════════════════════════════════════════════════════
            # PHASE 1 — Fetch catalog ONCE, then validate ALL SPs (CPU-only)
            # ══════════════════════════════════════════════════════════════
            st.session_state['_dbt_phase'] = 'validating'
            with _stepper_placeholder.container():
                render_stepper(_dbt_steps, 2)
            with st.spinner(f"Fetching metadata catalog for `{src_db}.{src_schema}`…"):
                catalog = fetch_schema_catalog(src_db, src_schema)

            if catalog.get("error"):
                st.error(f"⚠️ Catalog fetch failed: {catalog['error']}")
            else:
                _cat_tables = len(catalog['tables'])
                _cat_funcs  = len(catalog['functions'])
                st.caption(f"Catalog loaded — {_cat_tables} tables/views · {_cat_funcs} functions in `{src_db}.{src_schema}`")

                prog_v = st.progress(0, text="Validating…")
                for idx, sp_file in enumerate(all_sp_files):
                    sp_source = sp_file["content"]
                    sp_fname  = sp_file["name"]
                    entry = {
                        "file": sp_fname,
                        "source": sp_source,
                        "status": "ok",
                        "error": "",
                        "validation": None,
                        "medallion_models": None,
                        "duration": 0,
                    }

                    prog_v.progress((idx + 1) / len(all_sp_files), text=f"Validating {sp_fname} ({idx+1}/{len(all_sp_files)})")
                    try:
                        dep_result = validate_sp_dependencies(
                            sp_source, src_db, src_schema, catalog=catalog,
                        )
                    except Exception as ve:
                        dep_result = {"valid": False, "found": [], "missing": [],
                                      "target": None, "error": str(ve)}

                    entry["validation"] = dep_result

                    if dep_result.get("error"):
                        entry["status"] = "validation_error"
                        entry["error"]  = dep_result["error"]
                    elif not dep_result["valid"]:
                        entry["status"] = "blocked"
                        entry["error"]  = f"{len(dep_result['missing'])} missing object(s)"
                    # else status stays "ok" — eligible for conversion

                    results.append(entry)
                prog_v.empty()

                # Validation summary
                n_valid   = sum(1 for r in results if r["status"] == "ok")
                n_blocked = sum(1 for r in results if r["status"] == "blocked")
                n_verr    = sum(1 for r in results if r["status"] == "validation_error")
                _v_dur    = round(time.time() - _batch_start, 2)
                st.info(
                    f"**Validation complete** ({_v_dur}s) — "
                    f"✅ {n_valid} ready · ⛔ {n_blocked} blocked · ❌ {n_verr} error  "
                    f"out of **{len(results)}** SPs"
                )

                _v_total = len(results)
                _v_ok = n_valid
                _live_states = {}
                if _v_ok == _v_total:
                    _live_states[2] = {"status": "completed", "badge": f"{_v_ok}/{_v_total}"}
                elif n_verr > 0 and _v_ok == 0:
                    _live_states[2] = {"status": "failed", "badge": f"0/{_v_total}"}
                elif _v_total > 0:
                    _live_states[2] = {"status": "warning", "badge": f"{_v_ok}/{_v_total}"}

                # ══════════════════════════════════════════════════════════
                # PHASE 2 — Convert only the valid SPs (Cortex LLM calls)
                # ══════════════════════════════════════════════════════════
                convertible = [r for r in results if r["status"] == "ok"]
                if convertible:
                    st.session_state['_dbt_phase'] = 'converting'
                    with _stepper_placeholder.container():
                        render_stepper(_dbt_steps, 3, _live_states)
                    prog_c = st.progress(0, text="Converting…")
                    # Build model registry once — updated after each SP for cross-referencing
                    _model_registry = _build_model_registry()
                    for ci, entry in enumerate(convertible):
                        sp_source = entry["source"]
                        sp_fname  = entry["file"]
                        prog_c.progress((ci + 1) / len(convertible),
                                        text=f"Converting {sp_fname} ({ci+1}/{len(convertible)})")
                        try:
                            _t0 = time.time()
                            medallion_models = convert_sp_to_dbt_medallion(
                                sp_source, project_name_input,
                                source_database=src_db, source_schema=src_schema,
                                existing_models_registry=_model_registry,
                            )
                            _dur = round(time.time() - _t0, 2)
                            entry["medallion_models"] = medallion_models
                            entry["duration"] = _dur

                            new_project_files = generate_dbt_project_files(
                                project_name_input, medallion_models,
                                source_database=src_db, source_schema=src_schema,
                            )
                            st.session_state.dbt_project_files = merge_dbt_project_files(new_project_files)
                            # Rebuild registry so next SP sees this SP's models
                            _model_registry = _build_model_registry()

                            st.session_state.converted_objects["DBT_PROJECT"].append({
                                "project_name": project_name_input,
                                "source_file": sp_fname,
                                "bronze_models": medallion_models.get('bronze_models', []),
                                "silver_models": medallion_models.get('silver_models', []),
                                "gold_models":   medallion_models.get('gold_models', []),
                            })
                            save_conversion(
                                "DBT_PROJECT", project_name_input, sp_source,
                                f"{project_name_input}_dbt_{sp_fname}",
                                json.dumps(medallion_models), duration_secs=_dur,
                            )
                        except Exception as ce:
                            entry["status"] = "error"
                            entry["error"]  = str(ce)
                    prog_c.empty()

            _conv_total = round(time.time() - _batch_start, 2)
            st.session_state[dbt_results_key] = results
            st.session_state["dbt_batch_timing"] = _conv_total
            st.session_state.current_project_name = project_name_input

            n_ok      = sum(1 for r in results if r["status"] == "ok" and r.get("medallion_models"))
            n_err     = sum(1 for r in results if r["status"] == "error")
            _conv_dur = sum(r.get("duration", 0) for r in results if r["status"] == "ok" and r.get("medallion_models"))
            _conv_dur = round(_conv_dur, 2)
            parts = []
            if n_ok:  parts.append(f"✅ {n_ok} converted")
            if n_err: parts.append(f"❌ {n_err} failed")
            st.success(f"{' · '.join(parts)} — **{n_ok + n_err} SP(s) in {_conv_dur}s**")

    # ── Render per-SP results ─────────────────────────────────────────────────
    dbt_results: list = st.session_state.get(dbt_results_key, [])
    if dbt_results:
        st.divider()
        import pandas as pd

        for res in dbt_results:
            sp_fname = res["file"]
            status = res["status"]

            # ── Status badge ──────────────────────────────────────────────
            if status == "ok":
                badge = "✅"
                _mc = res.get("medallion_models") or {}
                _cnt = len(_mc.get('bronze_models', [])) + len(_mc.get('silver_models', [])) + len(_mc.get('gold_models', []))
                label = f"{badge} **{sp_fname}** — {_cnt} models · {res['duration']}s"
            elif status == "blocked":
                badge = "⛔"
                label = f"{badge} **{sp_fname}** — blocked ({res['error']})"
            else:
                badge = "❌"
                label = f"{badge} **{sp_fname}** — {res['error']}"

            with st.expander(label, expanded=(status != "ok")):
                # ── Compression ratio warning ─────────────────────────────
                if status == "ok" and res.get("source") and res.get("medallion_models"):
                    _src_lines = len(res["source"].splitlines())
                    _all_models = res["medallion_models"]
                    _out_lines = sum(
                        len(m.get('sql', '').splitlines())
                        for layer in ('bronze_models', 'silver_models', 'gold_models')
                        for m in _all_models.get(layer, [])
                    )
                    if _src_lines > 0 and _out_lines > 0:
                        _ratio = round(_out_lines / _src_lines * 100, 1)
                        if _ratio < 20:
                            st.warning(
                                f"⚠️ **High compression detected** — source has {_src_lines} lines, "
                                f"output has {_out_lines} lines ({_ratio}% ratio). "
                                f"Manual review recommended to verify conversion fidelity."
                            )

                # ── Validation details (always shown) ─────────────────────
                dep = res.get("validation")
                if dep:
                    if dep.get("error"):
                        st.error(f"Metadata validation error: {dep['error']}")
                    else:
                        val_rows = []
                        for obj in dep.get("found", []):
                            val_rows.append({"Object": obj["name"], "Type": obj["type"], "Status": "✅ Found"})
                        for obj in dep.get("missing", []):
                            val_rows.append({"Object": obj["name"], "Type": obj["type"], "Status": "❌ Missing"})
                        if dep.get("target"):
                            val_rows.append({"Object": dep["target"], "Type": "TABLE (target)", "Status": "⏭ Skipped"})
                        if val_rows:
                            st.dataframe(pd.DataFrame(val_rows), use_container_width=True, hide_index=True)
                        if not dep["valid"]:
                            st.warning("Conversion blocked. Create or convert the missing objects first, then retry.")

                # ── Medallion models (only for successful conversions) ────
                models = res.get("medallion_models")
                if models and status == "ok":
                    medal_tabs = st.tabs([":orange[Bronze]", ":blue[Silver]", ":green[Gold]"])
                    _layer_map = {"bronze_models": ("bronze", medal_tabs[0]),
                                  "silver_models": ("silver", medal_tabs[1]),
                                  "gold_models": ("gold", medal_tabs[2])}
                    for layer_key, (layer_name, tab_obj) in _layer_map.items():
                        with tab_obj:
                            for mi, m in enumerate(models.get(layer_key, [])):
                                _mname = m.get('name', f'{layer_name}_model')
                                _mpath = None
                                _pfiles = st.session_state.get("dbt_project_files", {})
                                for _fp in _pfiles:
                                    if _fp.endswith(f"/{_mname}.sql"):
                                        _mpath = _fp
                                        break
                                if not _mpath:
                                    _mpath = f"models/{layer_name}/{_mname}.sql"
                                with st.expander(f"**{_mname}**"):
                                    _edit_key = f"edit_{layer_key}_{mi}_{_mname}"
                                    _edited_sql = st.text_area(
                                        "SQL",
                                        value=st.session_state.get("dbt_project_files", {}).get(_mpath, m.get('sql', '')),
                                        height=300,
                                        key=_edit_key,
                                        label_visibility="collapsed",
                                    )
                                    _btn_save, _btn_regen = st.columns([1, 1])
                                    with _btn_save:
                                        if st.button(f"Save", key=f"save_{_edit_key}", type="primary"):
                                            if "dbt_project_files" not in st.session_state:
                                                st.session_state.dbt_project_files = {}
                                            st.session_state.dbt_project_files[_mpath] = _edited_sql
                                            show_toast(f"Saved {_mname}", "success")
                                            st.rerun()
                                    with _btn_regen:
                                        if st.button(f"Regenerate", key=f"regen_{_edit_key}"):
                                            _src_db = st.session_state.get('source_database', session.get_current_database().replace('"', ''))
                                            _src_schema = st.session_state.get('source_schema', 'DBO')
                                            with st.spinner(f"Regenerating {_mname}..."):
                                                _new_sql = _regenerate_single_model(_mname, layer_name, _edited_sql, _src_db, _src_schema)
                                            if _new_sql:
                                                if "dbt_project_files" not in st.session_state:
                                                    st.session_state.dbt_project_files = {}
                                                st.session_state.dbt_project_files[_mpath] = _new_sql
                                                show_toast(f"Regenerated {_mname}", "success")
                                                st.rerun()
                                            else:
                                                st.error("Regeneration failed. Try again.")

        # ── Download cumulative project ZIP ───────────────────────────────────
        st.divider()
        if 'dbt_project_files' in st.session_state:
            if st.button(":material/download: Download Project", type="primary", key="download_dbt"):
                zip_buffer = io.BytesIO()
                with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
                    for file_path, content in st.session_state.dbt_project_files.items():
                        zf.writestr(file_path, content)

                st.download_button(
                    ":material/folder_zip: Download ZIP",
                    zip_buffer.getvalue(),
                    f"{st.session_state.get('current_project_name', 'dbt')}_dbt_project.zip",
                    "application/zip",
                )

    _dbt_step = 0
    _dbt_phase = st.session_state.get('_dbt_phase', '')
    _has_project_name = bool(st.session_state.get("dbt_project_name", "").strip())
    _has_uploads = bool(st.session_state.get("dbt_sp_upload"))
    _dbt_res = st.session_state.get("dbt_sp_results", [])
    _has_results = bool(_dbt_res)
    _has_converted = _has_results and any(r.get("medallion_models") for r in _dbt_res)
    _has_project_files = bool(st.session_state.get("dbt_project_files"))

    _step_states = {}

    _total_sps = len(_dbt_res)
    _n_valid = sum(1 for r in _dbt_res if r["status"] not in ("blocked", "validation_error"))
    _n_blocked = sum(1 for r in _dbt_res if r["status"] == "blocked")
    _n_verr = sum(1 for r in _dbt_res if r["status"] == "validation_error")
    _n_converted = sum(1 for r in _dbt_res if r.get("medallion_models"))
    _n_conv_fail = sum(1 for r in _dbt_res if r["status"] == "error")

    if _has_project_name:
        _dbt_step = 1
    if _has_uploads:
        _dbt_step = 1

    if _dbt_phase == 'validating' and not _has_results:
        _dbt_step = 2
    elif _has_results:
        _dbt_step = 2
        if _total_sps > 0 and _n_valid == _total_sps:
            _step_states[2] = {"status": "completed", "badge": f"{_n_valid}/{_total_sps}"}
        elif _n_verr > 0 and _n_valid == 0:
            _step_states[2] = {"status": "failed", "badge": f"0/{_total_sps}"}
        elif _total_sps > 0:
            _step_states[2] = {"status": "warning", "badge": f"{_n_valid}/{_total_sps}"}

    if _dbt_phase == 'converting' and not _has_converted and _n_conv_fail == 0:
        _dbt_step = 3
    elif _has_converted or _n_conv_fail > 0:
        _dbt_step = 3
        _conv_total = _n_valid
        if _conv_total > 0 and _n_converted == _conv_total:
            _step_states[3] = {"status": "completed", "badge": f"{_n_converted}/{_conv_total}"}
        elif _n_converted > 0:
            _step_states[3] = {"status": "warning", "badge": f"{_n_converted}/{_conv_total}"}
        elif _conv_total > 0:
            _step_states[3] = {"status": "failed", "badge": f"0/{_conv_total}"}

    if _has_project_files and _has_converted:
        _dbt_step = 4
        st.session_state.pop('_dbt_phase', None)
    elif _has_project_files and not _has_results:
        _dbt_step = 0
    elif _has_results:
        st.session_state.pop('_dbt_phase', None)
        if _step_states.get(2, {}).get("status") == "failed":
            _dbt_step = 2
        elif _step_states.get(2, {}).get("status") == "warning" and _n_valid == 0:
            _dbt_step = 2
        elif _step_states.get(3, {}).get("status") == "failed":
            _dbt_step = 3

    with _stepper_placeholder.container():
        render_stepper(_dbt_steps, _dbt_step, _step_states)

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 6: EXECUTE DBT
# ═══════════════════════════════════════════════════════════════════════════════
with tab6:
    st.markdown('<div class="tab-info"><span class="info-icon">ℹ️</span>Deploy your generated dbt project to a Snowflake stage, then run <b>dbt build</b>, <b>dbt test</b>, or <b>dbt docs</b> directly from the app.</div>', unsafe_allow_html=True)
    # ── Resolved stage info banner (read-only, no user input) ────────────────
    dbt_stage_path = st.session_state.get("dbt_stage", "")
    stage_ready    = st.session_state.get("dbt_stage_ready", False)
    stage_error    = st.session_state.get("dbt_stage_error", None)

    if stage_ready:
        st.markdown(f"""
        <div style="display:flex;align-items:center;gap:10px;
                    background:#C2DDA6;border:1px solid #B3D561;
                    border-radius:10px;padding:0.65rem 1rem;margin-bottom:0.5rem;">
            <span style="color:#3D5622;font-size:1.1rem;"></span>
            <div>
                <span style="color:#3D5622;font-size:0.7rem;text-transform:uppercase;
                             letter-spacing:0.8px;font-weight:600;">Active Stage</span><br>
                <code style="color:#272727;font-size:0.85rem;background:transparent;">{dbt_stage_path}</code>
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div style="display:flex;align-items:center;gap:10px;
                    background:rgba(220,38,38,0.06);border:1px solid rgba(220,38,38,0.3);
                    border-radius:10px;padding:0.65rem 1rem;margin-bottom:0.5rem;">
            <span style="color:#DC2626;font-size:1.1rem;">⚠️</span>
            <div>
                <span style="color:#6B7280;font-size:0.7rem;text-transform:uppercase;
                             letter-spacing:0.8px;font-weight:600;">Stage Unavailable</span><br>
                <span style="color:#DC2626;font-size:0.8rem;">{stage_error or 'Unknown error during stage provisioning'}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

        # Retry button if stage failed
        if st.button(":material/refresh: Retry Stage Setup", type="primary", key="retry_stage"):
            _stage_path, _stage_ready, _stage_err = init_dbt_stage()
            st.session_state.dbt_stage       = _stage_path
            st.session_state.dbt_stage_ready = _stage_ready
            st.session_state.dbt_stage_error = _stage_err
            st.rerun()

    st.divider()

    # ── Upload All Files ─────────────────────────────────────────────────────
    st.markdown("**Upload Project Files to Stage**")

    has_project = bool(st.session_state.get("dbt_project_files"))

    if has_project:
        project_files = st.session_state.dbt_project_files
        total_files   = len(project_files)
        _current_fingerprint = {fp: hash(content) for fp, content in project_files.items()}
        _uploaded_fingerprint = st.session_state.get("_uploaded_file_fingerprint", {})
        _changed_files = [fp for fp, h in _current_fingerprint.items() if _uploaded_fingerprint.get(fp) != h]
      ##  if _changed_files:
          ##  st.caption(f"{len(_changed_files)} file(s) to upload")
        
        upload_disabled = not stage_ready or not _changed_files
        upload_type = "primary" if _changed_files else "secondary"
        if st.button(":material/cloud_upload: Upload All Files to Stage", key="upload_all_models",
             type=upload_type, disabled=upload_disabled):

            # File type routing
            OVERWRITE_ALWAYS  = {".sql", ".md"}          # always take latest
            MERGE_YAML        = {"schema.yml", "sources.yml"}   # merge with existing
            KEEP_IF_EXISTS    = {"dbt_project.yml", "profiles.yml"}  # never overwrite
        
            upload_results = []
            progress = st.progress(0, text="Uploading files...")
        
            for idx, (file_path, content) in enumerate(project_files.items()):
                file_name    = file_path.split("/")[-1]
                ext          = "." + file_name.rsplit(".", 1)[-1] if "." in file_name else ""
                stage_folder = "/".join(file_path.split("/")[:-1])
                stage_target = f"{dbt_stage_path}/{stage_folder}" if stage_folder else dbt_stage_path
        
                try:
                    # ── Determine final content to upload ──────────────────────
                    if file_name in KEEP_IF_EXISTS:
                        # Check if already on stage — if yes skip entirely
                        existing = read_file_from_stage(dbt_stage_path, file_path)
                        if existing is not None:
                            upload_results.append({
                                "file":   file_path,
                                "status": "⏭️ skipped (kept existing)"
                            })
                            progress.progress((idx + 1) / len(project_files),
                                              text=f"Skipping {file_path}...")
                            continue
                        final_content = content  # First time — upload as new
        
                    elif file_name in MERGE_YAML:
                        # Download existing from stage and merge
                        final_content = merge_yaml_with_stage(dbt_stage_path, file_path, content)
        
                    else:
                        # .sql and everything else — always take latest
                        final_content = content
        
                    # ── Upload ─────────────────────────────────────────────────
                    file_buffer = io.BytesIO(final_content.encode("utf-8"))
                    session.file.put_stream(
                        input_stream   = file_buffer,
                        stage_location = f"{stage_target}/{file_name}",
                        auto_compress  = False,
                        overwrite      = True
                    )
        
                    if file_name in MERGE_YAML:
                        existing_on_stage = read_file_from_stage(dbt_stage_path, file_path)
                        action = "merged" if existing_on_stage is not None else "uploaded"
                    else:
                        action = "uploaded"
                    upload_results.append({"file": file_path, "status": f"✅ {action}"})
        
                except Exception as e:
                    upload_results.append({"file": file_path, "status": f"❌ {str(e)[:80]}"})
        
                progress.progress((idx + 1) / len(project_files),
                                  text=f"Processing {file_path}...")
        
            progress.empty()
        
            # ── Summary ────────────────────────────────────────────────────────
            uploaded = sum(1 for r in upload_results if "✅ uploaded" in r["status"])
            merged   = sum(1 for r in upload_results if "✅ merged"   in r["status"])
            skipped  = sum(1 for r in upload_results if "⏭️"          in r["status"])
            failed   = sum(1 for r in upload_results if "❌"          in r["status"])
        
            if failed == 0:
                st.markdown(
                    f"Done — {uploaded} uploaded · {merged} merged · {skipped} skipped"
                )
            else:
                st.warning(
                    f"{uploaded} uploaded · {merged} merged · {skipped} skipped · {failed} failed"
                )
        
            with st.expander("Upload Details", expanded=(failed > 0)):
                for r in upload_results:
                    st.markdown(f"`{r['file']}` → {r['status']}")
        
            st.session_state.stage_upload_done = True
            st.session_state["_uploaded_file_fingerprint"] = {fp: hash(content) for fp, content in project_files.items()}

    else:
        st.info("No project files found. Convert a procedure in the **SP → dbt** tab first.")

    st.divider()

    # ── Execute Section ──────────────────────────────────────────────────────
    st.markdown("**Execute**")

    deploy_disabled = not st.session_state.get("stage_upload_done", False)
    if deploy_disabled:
        st.caption("⚠️ Upload files to stage first before deploying or running.")

    heal_col1, heal_col2 = st.columns([1, 1])
    with heal_col1:
        dbt_auto_heal_enabled = st.toggle(
            "⚡ Auto-Heal on dbt run errors",
            value=True,
            key="dbt_auto_heal_toggle",
            help="When enabled, Cortex AI will attempt to fix failed dbt models automatically."
        )
    with heal_col2:
        dbt_heal_max_attempts = st.select_slider(
            "Max heal attempts",
            options=[1, 2, 3],
            value=1,
            key="dbt_heal_max_attempts",
            disabled=not dbt_auto_heal_enabled,
            help="Number of Cortex retry attempts per failed model."
        )

    col1, col2, col3, col4 = st.columns(4)

    # Helper to parse stage path → db.schema
    def _parse_stage(stage_path):
        parts = stage_path.replace("@", "").split(".")
        return parts[0], parts[1]

    def _get_project_and_task(stage_path):
        db, schema = _parse_stage(stage_path)
        project_name = f"{db}.{schema}.AUTO_MIGRATION_PROJECT"
        task_name    = f"{db}.{schema}.DBT_EXECUTION_TASK"
        curr_wh      = session.get_current_warehouse().replace('"', '')
        return db, schema, project_name, task_name, curr_wh

    def _run_dbt_task(task_name, project_name, curr_wh, args: str):

        # Drop and recreate — no RESUME, no SCHEDULE
        try:
            session.sql(f"ALTER TASK IF EXISTS {task_name} SUSPEND").collect()
        except Exception:
            pass
        session.sql(f"DROP TASK IF EXISTS {task_name}").collect()

        create_task_sql = """
            CREATE OR REPLACE TASK {task_name}
            WAREHOUSE = {curr_wh}
            AS
            EXECUTE DBT PROJECT {project_name} ARGS='{args}';
        """.format(
            task_name    = task_name,
            curr_wh      = curr_wh,
            project_name = project_name,
            args         = args
        )
        session.sql(create_task_sql).collect()

        # Fire directly — no RESUME needed
        session.sql(f"EXECUTE TASK {task_name}").collect()

        # Small fixed wait for task to queue
        time.sleep(4)

        task_short = task_name.split(".")[-1]
        db         = task_name.split(".")[0]
        status     = "PENDING"
        error_msg  = None

        # 40 × 3s = 120s max — enough for 6 VIEW models
        for _ in range(40):
            time.sleep(3)
            try:
                history = session.sql("""
                    SELECT STATE, ERROR_MESSAGE
                    FROM TABLE({db}.INFORMATION_SCHEMA.TASK_HISTORY(
                        RESULT_LIMIT => 5,
                        TASK_NAME => '{task_short}'
                    ))
                    ORDER BY SCHEDULED_TIME DESC
                    LIMIT 1
                """.format(db=db, task_short=task_short)).collect()

                if history:
                    state = history[0]["STATE"]
                    if state in ("SUCCEEDED", "FAILED"):
                        status    = state
                        error_msg = history[0]["ERROR_MESSAGE"]
                        break
                    elif state in ("EXECUTING", "SCHEDULED"):
                        status = state
            except Exception:
                st.warning("Task running — no permission to read TASK_HISTORY. Check Snowsight.")
                break

        # Suspend after done
        try:
            session.sql(f"ALTER TASK IF EXISTS {task_name} SUSPEND").collect()
        except Exception:
            pass

        return status, error_msg or ""

    # ── Deploy Project ───────────────────────────────────────────────────────
    with col1:
        if st.button(":material/inventory: Deploy Project", type="primary", disabled=deploy_disabled, key="btn_deploy"):
            with st.spinner("Deploying project..."):
                try:
                    db, schema, project_name, _, _ = _get_project_and_task(dbt_stage_path)
                    session.sql("""
                        CREATE OR REPLACE DBT PROJECT {project_name}
                        FROM '{stage_path}'
                    """.format(
                        project_name = project_name,
                        stage_path   = dbt_stage_path      # <-- now wrapped in quotes inside the SQL
                    )).collect()
                    st.markdown(f"Project deployed: `{project_name}`")
                    st.session_state.project_deployed = True
                except Exception as e:
                    st.error(str(e))

    # ── dbt run ───────────────────────────────────────────────────────────────
    with col2:
        if st.button(":material/play_circle: dbt run", type="primary", disabled=deploy_disabled, key="btn_run"):
            with st.spinner("Running dbt models..."):
                try:
                    _, _, project_name, task_name, curr_wh = _get_project_and_task(dbt_stage_path)
                    state, error = _run_dbt_task(task_name, project_name, curr_wh, "run")
                    if state == "SUCCEEDED":
                        st.markdown("dbt run completed ✅")
                        st.session_state.pop("dbt_heal_suggestions", None)
                    elif state == "TIMEOUT":
                        st.warning("Task still running. Check Snowflake task history.")
                    else:
                        st.error(f"dbt run failed [{state}]: {error}")
                        if dbt_auto_heal_enabled:
                            parsed_errors = _parse_dbt_run_errors(error)
                            project_files = st.session_state.get("dbt_project_files", {})
                            if parsed_errors and project_files:
                                heal_suggestions = []
                                for pe in parsed_errors:
                                    model_file = pe["file"]
                                    model_sql = None
                                    for fp, content in project_files.items():
                                        if fp.endswith(f"{pe['model']}.sql") or fp == model_file:
                                            model_sql = content
                                            model_file = fp
                                            break
                                    if model_sql:
                                        best_fix = None
                                        current_sql = model_sql
                                        current_err = pe["error"]
                                        for attempt in range(dbt_heal_max_attempts):
                                            with st.spinner(f"⚡ Auto-healing `{pe['model']}` (attempt {attempt+1}/{dbt_heal_max_attempts})..."):
                                                fixed_sql = _dbt_auto_heal_model(current_sql, current_err, pe["model"])
                                            if fixed_sql and fixed_sql.strip() != current_sql.strip():
                                                best_fix = fixed_sql
                                                break
                                            current_sql = fixed_sql or current_sql
                                        if best_fix:
                                            heal_suggestions.append({
                                                "model": pe["model"],
                                                "file": model_file,
                                                "error": pe["error"],
                                                "original_sql": model_sql,
                                                "fixed_sql": best_fix,
                                                "approved": False,
                                            })
                                if heal_suggestions:
                                    _healed_cols = set()
                                    for hs in heal_suggestions:
                                        for m in re.finditer(r'column\s+(\w+)', hs["error"], re.IGNORECASE):
                                            _healed_cols.add(m.group(1).upper())
                                    if _healed_cols:
                                        _trunc_pattern = re.compile(
                                            r'CAST\s*\(\s*(' + '|'.join(re.escape(c) for c in _healed_cols) +
                                            r')\s+AS\s+VARCHAR\s*\(\s*(\d+)\s*\)\s*\)',
                                            re.IGNORECASE
                                        )
                                        for fp, content in project_files.items():
                                            if not fp.endswith(".sql"):
                                                continue
                                            if any(fp == hs["file"] for hs in heal_suggestions):
                                                continue
                                            _matches = list(_trunc_pattern.finditer(content))
                                            if _matches:
                                                _fixed = _trunc_pattern.sub(
                                                    lambda m: f"CAST({m.group(1)} AS VARCHAR(16777216))" if int(m.group(2)) < 512 else m.group(0),
                                                    content
                                                )
                                                if _fixed != content:
                                                    _model_name = fp.split("/")[-1].replace(".sql", "")
                                                    heal_suggestions.append({
                                                        "model": _model_name,
                                                        "file": fp,
                                                        "error": f"Preventive fix: narrow VARCHAR CAST on {', '.join(_healed_cols)} columns",
                                                        "original_sql": content,
                                                        "fixed_sql": _fixed,
                                                        "approved": False,
                                                    })
                                    st.session_state["dbt_heal_suggestions"] = heal_suggestions
                except Exception as e:
                    st.error(str(e))

    # ── dbt test ─────────────────────────────────────────────────────────────
    with col3:
        if st.button(":material/rule: dbt test", type="primary", key="btn_dbt_test", disabled=deploy_disabled):
            with st.spinner("Running dbt tests..."):
                try:
                    _, _, project_name, task_name, curr_wh = _get_project_and_task(dbt_stage_path)
                    state, error = _run_dbt_task(task_name, project_name, curr_wh, "test")
                    if state == "SUCCEEDED":
                        st.markdown("All dbt tests passed ✅")
                    elif state == "TIMEOUT":
                        st.warning("Tests still running. Check Snowflake task history.")
                    else:
                        st.error(f"dbt test failed [{state}]: {error}")
                except Exception as e:
                    st.error(str(e))

    # ── dbt docs ─────────────────────────────────────────────────────────────
    with col4:
        if st.button(":material/description: dbt docs", type="primary", key="btn_dbt_docs", disabled=deploy_disabled):
            with st.spinner("Generating dbt docs..."):
                try:
                    db, schema, project_name, task_name, curr_wh = _get_project_and_task(dbt_stage_path)
                    if not curr_wh:
                        st.error("No active warehouse. Set a warehouse before generating docs.")
                    else:
                        state, error = _run_dbt_task(task_name, project_name, curr_wh, "docs generate")
                        if state == "SUCCEEDED":
                            st.markdown("Docs generated. Fetching artifacts...")
                            try:
                                task_short = task_name.split(".")[-1]
                                qid_row = session.sql("""
                                    SELECT QUERY_ID
                                    FROM TABLE({db}.INFORMATION_SCHEMA.TASK_HISTORY(
                                        RESULT_LIMIT => 5,
                                        TASK_NAME => '{task_short}'
                                    ))
                                    WHERE STATE = 'SUCCEEDED'
                                    ORDER BY SCHEDULED_TIME DESC
                                    LIMIT 1
                                """.format(db=db, task_short=task_short)).collect()

                                if qid_row and qid_row[0]["QUERY_ID"]:
                                    docs_qid = qid_row[0]["QUERY_ID"]

                                    docs_stage = f"{db}.{schema}.DBT_DOCS_STAGE"

                                    sp_name = f"{db}.{schema}.SP_FETCH_DBT_DOCS"
                                    session.sql(f"""
                                        CREATE OR REPLACE PROCEDURE {sp_name}(QID STRING, DOCS_STAGE STRING)
                                        RETURNS STRING
                                        LANGUAGE PYTHON
                                        RUNTIME_VERSION = '3.9'
                                        PACKAGES = ('snowflake-snowpark-python')
                                        HANDLER = 'run'
                                        EXECUTE AS CALLER
                                        AS
                                        $$
import datetime, zipfile, io

def run(session, qid, docs_stage):
    session.sql(f"CREATE STAGE IF NOT EXISTS {{docs_stage}}").collect()

    loc = session.sql(f"SELECT SYSTEM$LOCATE_DBT_ARTIFACTS('{{qid}}')").collect()[0][0]

    # Timestamp for this run (used as the version folder name)
    ts = datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')

    # ── Clear previous "docs/" (latest) but leave versions/ intact ──────────
    try:
        old_docs = session.sql(f"LIST @{{docs_stage}}/docs/").collect()
        for f in old_docs:
            rel = "/".join(f["name"].split("/")[1:])
            try:
                session.sql(f"REMOVE @{{docs_stage}}/{{rel}}").collect()
            except Exception:
                pass
    except Exception:
        pass

    # ── Copy raw artifacts (zip, logs, target/) to docs/ and versions/{{ts}}/ ─
    session.sql(f"COPY FILES INTO @{{docs_stage}}/docs/ FROM '{{loc}}'").collect()
    session.sql(f"COPY FILES INTO @{{docs_stage}}/versions/{{ts}}/ FROM '{{loc}}'").collect()

    # ── Extract docs files from dbt_artifacts.zip ────────────────────────────
    zip_rel = None
    for f in session.sql(f"LIST @{{docs_stage}}/docs/").collect():
        if f["name"].endswith("dbt_artifacts.zip"):
            zip_rel = "/".join(f["name"].split("/")[1:])
            break

    if zip_rel:
        scoped = session.sql(
            f"SELECT BUILD_SCOPED_FILE_URL(@{{docs_stage}}, '{{zip_rel}}')"
        ).collect()[0][0]
        from snowflake.snowpark.files import SnowflakeFile
        with SnowflakeFile.open(scoped, 'rb') as sf:
            zf = zipfile.ZipFile(io.BytesIO(sf.read()))

        targets = {{'index.html': None, 'catalog.json': None, 'manifest.json': None}}
        for zname in zf.namelist():
            base = zname.split('/')[-1]
            if base in targets and targets[base] is None:
                targets[base] = zf.read(zname)

        for fname, content in targets.items():
            if content is not None:
                for dest in [f"@{{docs_stage}}/docs/{{fname}}",
                             f"@{{docs_stage}}/versions/{{ts}}/{{fname}}"]:
                    session.file.put_stream(io.BytesIO(content), dest,
                                            auto_compress=False, overwrite=True)

    # ── Prune: keep only the 1 most recent version folder ─────────────────
    try:
        all_ver = session.sql(f"LIST @{{docs_stage}}/versions/").collect()
        ver_dirs = sorted(set(
            f["name"].split("/")[2]
            for f in all_ver
            if len(f["name"].split("/")) > 2
        ))
        for old_ts in ver_dirs[:-1]:
            for vf in all_ver:
                parts = vf["name"].split("/")
                if len(parts) > 2 and parts[2] == old_ts:
                    rel = "/".join(parts[1:])
                    try:
                        session.sql(f"REMOVE @{{docs_stage}}/{{rel}}").collect()
                    except Exception:
                        pass
    except Exception:
        pass

    return ts
                                        $$
                                    """).collect()

                                    fetch_task = f"{db}.{schema}.DBT_DOCS_FETCH_TASK"
                                    try:
                                        session.sql(f"DROP TASK IF EXISTS {fetch_task}").collect()
                                    except Exception:
                                        pass
                                    session.sql("""
                                        CREATE OR REPLACE TASK {task}
                                        WAREHOUSE = {wh}
                                        AS
                                        CALL {sp}('{qid}', '{stage}')
                                    """.format(
                                        task=fetch_task, wh=curr_wh, sp=sp_name,
                                        qid=docs_qid, stage=docs_stage
                                    )).collect()
                                    session.sql(f"EXECUTE TASK {fetch_task}").collect()

                                    time.sleep(5)
                                    fetch_done = False
                                    fetch_error = ""
                                    for _ in range(20):
                                        time.sleep(3)
                                        try:
                                            fh = session.sql("""
                                                SELECT STATE, ERROR_MESSAGE
                                                FROM TABLE({db}.INFORMATION_SCHEMA.TASK_HISTORY(
                                                    RESULT_LIMIT => 3,
                                                    TASK_NAME => '{t}'
                                                ))
                                                ORDER BY SCHEDULED_TIME DESC LIMIT 1
                                            """.format(db=db, t=fetch_task.split(".")[-1])).collect()
                                            if fh and fh[0]["STATE"] == "SUCCEEDED":
                                                fetch_done = True
                                                break
                                            elif fh and fh[0]["STATE"] == "FAILED":
                                                fetch_error = fh[0].get("ERROR_MESSAGE", "")
                                                break
                                        except Exception:
                                            break

                                    try:
                                        session.sql(f"DROP TASK IF EXISTS {fetch_task}").collect()
                                    except Exception:
                                        pass

                                    if not fetch_done:
                                        st.warning(f"Could not fetch docs artifacts. {fetch_error}")
                                    else:
                                        # Determine the newest version by listing versions/ folder
                                        try:
                                            _vlist = session.sql(f"LIST @{docs_stage}/versions/").collect()
                                            _vdirs = sorted(set(
                                                r["name"].split("/")[2]
                                                for r in _vlist
                                                if len(r["name"].split("/")) > 2
                                            ), reverse=True)
                                            _new_version = _vdirs[0] if _vdirs else None
                                        except Exception as _vle:
                                            _new_version = None
                                            st.warning(f"Could not list versions folder: {_vle}")

                                        # Persist stage coordinates for the viewer
                                        st.session_state.dbt_docs_stage  = docs_stage
                                        st.session_state.dbt_docs_db     = db
                                        st.session_state.dbt_docs_schema = schema
                                        if "dbt_docs_cache" not in st.session_state:
                                            st.session_state.dbt_docs_cache = {}

                                        # ── Helper SP: reads any stage file via SnowflakeFile ──
                                        _read_sp = f"{db}.{schema}.SP_READ_STAGE_FILE"
                                        session.sql(f"""
                                            CREATE OR REPLACE PROCEDURE {_read_sp}(STAGE_NAME STRING, REL_PATH STRING)
                                            RETURNS STRING
                                            LANGUAGE PYTHON
                                            RUNTIME_VERSION = '3.9'
                                            PACKAGES = ('snowflake-snowpark-python')
                                            HANDLER = 'run'
                                            EXECUTE AS CALLER
                                            AS
                                            $$
def run(session, stage_name, rel_path):
    scoped = session.sql(
        f"SELECT BUILD_SCOPED_FILE_URL(@{{stage_name}}, '{{rel_path}}')"
    ).collect()[0][0]
    from snowflake.snowpark.files import SnowflakeFile
    with SnowflakeFile.open(scoped, 'rb') as fh:
        return fh.read().decode('utf-8', errors='replace')
                                            $$
                                        """).collect()
                                        st.session_state.dbt_docs_read_sp = _read_sp

                                        _read_errors = []

                                        def _read_stage_text(rel_path, default="{}"):
                                            try:
                                                result = session.sql(
                                                    f"CALL {_read_sp}('{docs_stage}', '{rel_path}')"
                                                ).collect()[0][0]
                                                return result if result else default
                                            except Exception as _re:
                                                _read_errors.append(f"{rel_path}: {_re}")
                                                return default

                                        def _load_version_html(ver_prefix):
                                            """Load and render docs HTML for a given stage path prefix."""
                                            _files = [
                                                r.as_dict() for r in
                                                session.sql(f"LIST @{docs_stage}/{ver_prefix}/").collect()
                                            ]
                                            _fmap = {}
                                            for _f in _files:
                                                _fn = _f.get("name", "")
                                                _rel = "/".join(_fn.split("/")[1:])
                                                _fmap[_rel] = _rel

                                            _manifest = "{}"
                                            _catalog  = "{}"
                                            _html     = ""

                                            for _mk in [f"{ver_prefix}/manifest.json", f"{ver_prefix}/target/manifest.json"]:
                                                if _mk in _fmap:
                                                    _manifest = _read_stage_text(_mk)
                                                    break
                                            for _ck in [f"{ver_prefix}/catalog.json", f"{ver_prefix}/target/catalog.json"]:
                                                if _ck in _fmap:
                                                    _catalog = _read_stage_text(_ck)
                                                    break
                                            if f"{ver_prefix}/index.html" in _fmap:
                                                _html = _read_stage_text(f"{ver_prefix}/index.html", default="")

                                            if not _html:
                                                return None

                                            _inject = (
                                                "<script>\n(function(){\n"
                                                "  var _manifest=" + _manifest + ";\n"
                                                "  var _catalog="  + _catalog  + ";\n"
                                                "  var _oF=window.fetch;\n"
                                                "  window.fetch=function(u,o){var s=(typeof u==='string')?u:(u&&u.url)||'';\n"
                                                "    if(s.indexOf('manifest.json')!==-1) return Promise.resolve(new Response(JSON.stringify(_manifest),{status:200,headers:{'Content-Type':'application/json'}}));\n"
                                                "    if(s.indexOf('catalog.json')!==-1)  return Promise.resolve(new Response(JSON.stringify(_catalog), {status:200,headers:{'Content-Type':'application/json'}}));\n"
                                                "    return _oF?_oF.apply(this,arguments):Promise.reject(new Error('no fetch'));};\n"
                                                "  var _oO=XMLHttpRequest.prototype.open,_oS=XMLHttpRequest.prototype.send;\n"
                                                "  XMLHttpRequest.prototype.open=function(m,u){this._u=u||'';return _oO.apply(this,arguments);};\n"
                                                "  XMLHttpRequest.prototype.send=function(){var self=this,u=this._u||'';\n"
                                                "    if(u.indexOf('manifest.json')!==-1||u.indexOf('catalog.json')!==-1){\n"
                                                "      var d=(u.indexOf('manifest.json')!==-1)?_manifest:_catalog,b=JSON.stringify(d);\n"
                                                "      setTimeout(function(){Object.defineProperty(self,'status',{value:200});Object.defineProperty(self,'readyState',{value:4});\n"
                                                "        Object.defineProperty(self,'responseText',{value:b});Object.defineProperty(self,'response',{value:b});\n"
                                                "        if(self.onreadystatechange)self.onreadystatechange();if(self.onload)self.onload();},0);return;}\n"
                                                "    return _oS.apply(this,arguments);}\n"
                                                "})();\n</script>"
                                            )
                                            if '<head>' in _html:
                                                return _html.replace('<head>', '<head>' + _inject, 1)
                                            elif '<HEAD>' in _html:
                                                return _html.replace('<HEAD>', '<HEAD>' + _inject, 1)
                                            return _inject + _html

                                        _rendered = _load_version_html(f"versions/{_new_version}") if _new_version else None
                                        if _rendered:
                                            st.session_state.dbt_docs_cache[_new_version] = _rendered
                                            st.session_state.dbt_docs_html = _rendered

                                        if st.session_state.get("dbt_docs_html"):
                                            st.success("dbt docs ready! See the **dbt Docs Viewer** below.")
                                            st.download_button(
                                                label="⬇️ Download dbt Docs HTML (open in browser)",
                                                data=st.session_state.dbt_docs_html.encode("utf-8"),
                                                file_name=f"dbt_docs_{_new_version}.html",
                                                mime="text/html",
                                                key="dl_docs_after_gen",
                                            )
                                        else:
                                            st.session_state.dbt_docs_html = None
                                            st.warning("index.html not found in artifacts. Check stage manually.")
                                else:
                                    st.warning("Could not find query ID for docs generation.")
                            except Exception as doc_err:
                                st.session_state.dbt_docs_html = None
                                st.warning(f"Docs generated but could not load artifacts: {doc_err}")
                                st.info("Run `SELECT SYSTEM$LOCATE_DBT_ARTIFACTS('<query_id>')` manually.")
                        elif state == "TIMEOUT":
                            st.warning("Docs generation still running. Check Snowflake task history.")
                        else:
                            st.error(f"dbt docs failed [{state}]: {error}")
                except Exception as e:
                    st.error(str(e))

    # ── dbt Docs Viewer ──────────────────────────────────────────────────────
    if st.session_state.get("dbt_docs_html") and st.session_state.get("dbt_docs_stage"):
        st.divider()
        st.markdown("""
        <div class="section-header">
            <span class="section-icon">📖</span>
            <h3 class="section-title">dbt Docs Viewer</h3>
        </div>""", unsafe_allow_html=True)

        _vdocs_stage  = st.session_state.dbt_docs_stage
        _vdocs_db     = st.session_state.dbt_docs_db
        _vdocs_schema = st.session_state.dbt_docs_schema
        _vdocs_read_sp = st.session_state.get("dbt_docs_read_sp",
                             f"{_vdocs_db}.{_vdocs_schema}.SP_READ_STAGE_FILE")
        _cache = st.session_state.get("dbt_docs_cache", {})

        try:
            _ver_files = session.sql(f"LIST @{_vdocs_stage}/versions/").collect()
            _ver_dirs  = sorted(set(
                r["name"].split("/")[2]
                for r in _ver_files
                if len(r["name"].split("/")) > 2
            ), reverse=True)
        except Exception:
            _ver_dirs = []

        _selected_ver = _ver_dirs[0] if _ver_dirs else None
        _ver_label = ""
        if _selected_ver and len(_selected_ver) >= 15:
            _ver_label = f"{_selected_ver[:4]}-{_selected_ver[4:6]}-{_selected_ver[6:8]}  {_selected_ver[9:11]}:{_selected_ver[11:13]}:{_selected_ver[13:15]} UTC"
        if _ver_label:
            st.caption(f"📅 Latest docs version: {_ver_label}")

        if _selected_ver and _selected_ver not in _cache:
            with st.spinner(f"Loading version {_ver_label or _selected_ver}..."):
                def _read_ver_text(rel_path, default="{}"):
                    try:
                        result = session.sql(
                            f"CALL {_vdocs_read_sp}('{_vdocs_stage}', '{rel_path}')"
                        ).collect()[0][0]
                        return result if result else default
                    except Exception:
                        return default

                _ver_prefix = f"versions/{_selected_ver}"
                _vf_list = [
                    r.as_dict() for r in
                    session.sql(f"LIST @{_vdocs_stage}/{_ver_prefix}/").collect()
                ]
                _vfmap = {"/".join(r["name"].split("/")[1:]): True for r in _vf_list}

                _vm, _vc, _vh = "{}", "{}", ""
                for _mk in [f"{_ver_prefix}/manifest.json", f"{_ver_prefix}/target/manifest.json"]:
                    if _mk in _vfmap:
                        _vm = _read_ver_text(_mk); break
                for _ck in [f"{_ver_prefix}/catalog.json", f"{_ver_prefix}/target/catalog.json"]:
                    if _ck in _vfmap:
                        _vc = _read_ver_text(_ck); break
                if f"{_ver_prefix}/index.html" in _vfmap:
                    _vh = _read_ver_text(f"{_ver_prefix}/index.html", default="")

                if _vh:
                    _vi = (
                        "<script>\n(function(){\n"
                        "  var _manifest=" + _vm + ";\n"
                        "  var _catalog="  + _vc + ";\n"
                        "  var _oF=window.fetch;\n"
                        "  window.fetch=function(u,o){var s=(typeof u==='string')?u:(u&&u.url)||'';\n"
                        "    if(s.indexOf('manifest.json')!==-1) return Promise.resolve(new Response(JSON.stringify(_manifest),{status:200,headers:{'Content-Type':'application/json'}}));\n"
                        "    if(s.indexOf('catalog.json')!==-1)  return Promise.resolve(new Response(JSON.stringify(_catalog), {status:200,headers:{'Content-Type':'application/json'}}));\n"
                        "    return _oF?_oF.apply(this,arguments):Promise.reject(new Error('no fetch'));};\n"
                        "  var _oO=XMLHttpRequest.prototype.open,_oS=XMLHttpRequest.prototype.send;\n"
                        "  XMLHttpRequest.prototype.open=function(m,u){this._u=u||'';return _oO.apply(this,arguments);};\n"
                        "  XMLHttpRequest.prototype.send=function(){var self=this,u=this._u||'';\n"
                        "    if(u.indexOf('manifest.json')!==-1||u.indexOf('catalog.json')!==-1){\n"
                        "      var d=(u.indexOf('manifest.json')!==-1)?_manifest:_catalog,b=JSON.stringify(d);\n"
                        "      setTimeout(function(){Object.defineProperty(self,'status',{value:200});Object.defineProperty(self,'readyState',{value:4});\n"
                        "        Object.defineProperty(self,'responseText',{value:b});Object.defineProperty(self,'response',{value:b});\n"
                        "        if(self.onreadystatechange)self.onreadystatechange();if(self.onload)self.onload();},0);return;}\n"
                        "    return _oS.apply(this,arguments);}\n"
                        "})();\n</script>"
                    )
                    _vh = _vh.replace('<head>', '<head>' + _vi, 1) if '<head>' in _vh else _vi + _vh
                    _cache[_selected_ver] = _vh
                    st.session_state.dbt_docs_cache = _cache
                else:
                    st.warning(f"index.html not found for version {_selected_ver}.")

        _display_html = _cache.get(_selected_ver, st.session_state.get("dbt_docs_html", ""))

        if _display_html:
            st.download_button(
                label="⬇️ Download HTML",
                data=_display_html.encode("utf-8"),
                file_name=f"dbt_docs_{_selected_ver}.html",
                mime="text/html",
                key="dl_docs_viewer",
            )

        st.caption("If the viewer below is blank, download the file above and open it in Chrome/Firefox.")
        import streamlit.components.v1 as components
        if _display_html:
            components.html(_display_html, height=1000, scrolling=True)

    # ── Cortex Test Data Generator ─────────────────────────────────────────
    _has_dbt_files = bool(st.session_state.get("dbt_project_files"))
    if _has_dbt_files:
        st.divider()
        st.markdown("""
        <div class="section-header">
            <span class="section-icon">🧪</span>
            <h3 class="section-title">Cortex Test Data Generator</h3>
        </div>""", unsafe_allow_html=True)
        st.caption("Generate synthetic test data for source tables using Cortex AI so dbt tests can pass (or intentionally fail).")

        td_col1, td_col2, td_col3 = st.columns([1, 1, 1])
        with td_col1:
            test_data_mode = st.selectbox(
                "Test Data Mode",
                ["All Pass ✅", "All Fail ❌", "Mixed ⚠️"],
                key="test_data_mode",
                help="All Pass: data satisfies every dbt test. All Fail: violates every test. Mixed: some pass, some fail."
            )
        with td_col2:
            test_data_rows = st.select_slider(
                "Rows per table",
                options=[5, 10, 20, 50],
                value=10,
                key="test_data_rows",
            )
        with td_col3:
            st.markdown("<br>", unsafe_allow_html=True)
            generate_test_data = st.button(
                "🧪 Generate Test Data",
                key="btn_gen_test_data",
                type="primary",
                disabled=not stage_ready,
            )

        if generate_test_data:
            project_files = st.session_state.get("dbt_project_files", {})
            _sources_yml = ""
            _schema_ymls = []
            _model_sqls = {}
            for fp, content in project_files.items():
                fname = fp.split("/")[-1]
                if fname == "sources.yml":
                    _sources_yml = content
                elif fname == "schema.yml":
                    _schema_ymls.append(content)
                elif fname.endswith(".sql"):
                    _model_sqls[fp] = content

            if not _sources_yml:
                st.warning("No sources.yml found in project files. Cannot determine source tables.")
            else:
                _mode_map = {
                    "All Pass ✅": "ALL_PASS",
                    "All Fail ❌": "ALL_FAIL",
                    "Mixed ⚠️": "MIXED",
                }
                _mode = _mode_map.get(test_data_mode, "ALL_PASS")

                _mode_instructions = {
                    "ALL_PASS": "Generate data that will SATISFY ALL dbt tests (not_null columns must have values, unique columns must have distinct values, accepted_values must match, relationships must be valid). Respect all NOT NULL constraints on physical tables.",
                    "ALL_FAIL": "Generate data that will VIOLATE EVERY dbt test. Include NULLs for columns with not_null dbt tests, duplicate values for unique tests, invalid values for accepted_values, broken foreign keys. The app will automatically drop NOT NULL constraints before inserting, so you CAN use NULL values freely.",
                    "MIXED": "Generate data where approximately HALF the tests pass and HALF fail. Include some NULLs, some duplicates, some invalid values. The app will automatically drop NOT NULL constraints before inserting, so you CAN use NULL values freely.",
                }

                _source_ddls = ""
                try:
                    _src_data = yaml.safe_load(_sources_yml) or {}
                    _src_list = _src_data.get("sources", [])
                    if _src_list:
                        _src_db = _src_list[0].get("database", "")
                        _src_schema = _src_list[0].get("schema", "")
                        _tables = [t.get("name", "") for t in _src_list[0].get("tables", [])]
                        for _tbl in _tables[:10]:
                            if _tbl:
                                try:
                                    _ddl_rows = session.sql(f"SELECT GET_DDL('TABLE', '{_src_db}.{_src_schema}.{_tbl}') AS DDL").collect()
                                    if _ddl_rows:
                                        _source_ddls += f"\n-- {_tbl}\n{_ddl_rows[0]['DDL']}\n"
                                except Exception:
                                    pass
                except Exception:
                    pass

                _schema_context = "\n---\n".join(_schema_ymls[:3])

                _bronze_models = ""
                for fp, sql in _model_sqls.items():
                    if "/staging/" in fp or "/bronze/" in fp:
                        _bronze_models += f"\n-- File: {fp}\n{sql}\n"

                _prompt = f"""You are a Snowflake SQL expert. Generate INSERT statements to populate source tables with synthetic test data.

SOURCES.YML (defines source tables, database, schema):
{_sources_yml[:3000]}

SOURCE TABLE DDLs (shows NOT NULL constraints — you MUST respect these):
{_source_ddls[:4000]}

SCHEMA.YML (defines dbt tests - not_null, unique, accepted_values, relationships):
{_schema_context[:4000]}

BRONZE/STAGING MODELS (shows column types and CASTs):
{_bronze_models[:4000]}

CRITICAL RULES:
- NEVER insert NULL into a column that has a NOT NULL constraint in the table DDL — Snowflake will reject the INSERT
- Every column value must be compatible with its data type in the DDL
- For hash columns, generate realistic 128-char hex strings

{_mode_instructions[_mode]}

- Generate exactly {test_data_rows} rows per source table
- Use INSERT INTO with fully qualified table names from sources.yml (database.schema.table_name)
- Include all columns referenced in the staging models
- Use realistic-looking data (addresses, names, IDs, etc.)
- For hash columns, generate realistic SHA-512 hex strings (128 chars)
- Wrap each table's INSERT in a separate statement
- Return ONLY the SQL INSERT statements — no explanation, no markdown fences

OUTPUT FORMAT:
INSERT INTO database.schema.table1 (col1, col2, ...) VALUES
(val1, val2, ...),
(val1, val2, ...);

INSERT INTO database.schema.table2 ...;"""

                with st.spinner(f"🧪 Generating {_mode.lower().replace('_', ' ')} test data via Cortex AI..."):
                    try:
                        _escaped = _prompt.replace("'", "''")
                        _result = session.sql(f"""
                            SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-opus-4-5', '{_escaped}') as response
                        """).collect()
                        _test_sql = clean_sql_response(_result[0]['RESPONSE'].strip())
                        st.session_state["generated_test_data"] = _test_sql
                        st.session_state["test_data_executed"] = False
                    except Exception as tde:
                        st.error(f"Test data generation failed: {tde}")

        _gen_sql = st.session_state.get("generated_test_data", "")
        if _gen_sql:
            st.markdown("**Generated SQL**")
            st.code(_gen_sql, language="sql")

            exec_col1, exec_col2 = st.columns(2)
            with exec_col1:
                if st.button("▶️ Execute INSERT Statements", key="btn_exec_test_data", type="primary"):
                    _mode = st.session_state.get("test_data_mode", "All Pass ✅")
                    _needs_nullable = _mode in ("All Fail ❌", "Mixed ⚠️")
                    _altered_cols = []
                    if _needs_nullable:
                        try:
                            _src_data = yaml.safe_load(st.session_state.get("dbt_project_files", {}).get(
                                next((fp for fp in st.session_state.get("dbt_project_files", {}) if fp.endswith("sources.yml")), ""), "")) or {}
                            _src_list = _src_data.get("sources", [])
                            if _src_list:
                                _src_db = _src_list[0].get("database", "")
                                _src_schema = _src_list[0].get("schema", "")
                                for _tbl_entry in _src_list[0].get("tables", []):
                                    _tbl = _tbl_entry.get("name", "")
                                    if _tbl:
                                        try:
                                            _col_rows = session.sql(f"""
                                                SELECT COLUMN_NAME, IS_NULLABLE
                                                FROM "{_src_db}".INFORMATION_SCHEMA.COLUMNS
                                                WHERE TABLE_SCHEMA = '{_src_schema}' AND TABLE_NAME = '{_tbl}'
                                                AND IS_NULLABLE = 'NO'
                                            """).collect()
                                            for _cr in _col_rows:
                                                _cn = _cr["COLUMN_NAME"]
                                                try:
                                                    session.sql(f'ALTER TABLE "{_src_db}"."{_src_schema}"."{_tbl}" ALTER COLUMN "{_cn}" DROP NOT NULL').collect()
                                                    _altered_cols.append(f"{_tbl}.{_cn}")
                                                except Exception:
                                                    pass
                                        except Exception:
                                            pass
                        except Exception:
                            pass
                        if _altered_cols:
                            st.caption(f"Temporarily dropped NOT NULL on {len(_altered_cols)} column(s) to allow test data insertion.")

                    _stmts = [s.strip() for s in _gen_sql.split(";") if s.strip()]
                    _ok = 0
                    _fail = 0
                    _errors = []
                    _prog = st.progress(0, text="Inserting test data...")
                    for si, stmt in enumerate(_stmts):
                        if not stmt.upper().startswith("INSERT"):
                            continue
                        try:
                            session.sql(stmt).collect()
                            _ok += 1
                        except Exception as ie:
                            _fail += 1
                            _errors.append(f"Statement {si+1}: {str(ie)[:200]}")
                        _prog.progress((si + 1) / len(_stmts), text=f"Executing {si+1}/{len(_stmts)}...")
                    _prog.empty()
                    if _fail == 0:
                        show_toast(f"All {_ok} INSERT statements executed successfully!", "success")
                        st.session_state["test_data_executed"] = True
                    else:
                        show_toast(f"{_ok} succeeded, {_fail} failed", "info", "⚠️")
                        with st.expander("Insert Errors", expanded=True):
                            for err in _errors:
                                st.error(err)
            with exec_col2:
                if st.button("🗑️ Clear Generated SQL", key="btn_clear_test_data"):
                    st.session_state.pop("generated_test_data", None)
                    st.session_state.pop("test_data_executed", None)
                    st.rerun()

    # ── dbt Auto-Heal Re-run Result (persisted across rerun) ─────────────────
    _heal_rerun_result = st.session_state.pop("dbt_heal_rerun_result", None)
    if _heal_rerun_result:
        if _heal_rerun_result["state"] == "SUCCEEDED":
            st.success("dbt run --full-refresh completed successfully after auto-heal! ✅")
            show_toast("dbt run completed after auto-heal!", "success")
        else:
            _err = _heal_rerun_result.get("error", "Unknown error")
            st.error(f"dbt run --full-refresh failed [{_heal_rerun_result['state']}]: {_err}")

    # ── dbt Auto-Heal Review Panel ──────────────────────────────────────────
    heal_suggestions = st.session_state.get("dbt_heal_suggestions", [])
    if heal_suggestions:
        st.divider()
        st.markdown("""
        <div class="section-header">
            <span class="section-icon">⚡</span>
            <h3 class="section-title">Auto-Heal Suggestions</h3>
        </div>""", unsafe_allow_html=True)
        st.caption(f"{len(heal_suggestions)} model(s) have suggested fixes from Cortex AI. Review and approve to apply.")

        for idx, hs in enumerate(heal_suggestions):
            _approved = hs.get("approved", False)
            _badge = "✅ Approved" if _approved else "⚡ Review"
            with st.expander(f"{_badge} — **{hs['model']}** (`{hs['file']}`)", expanded=not _approved):
                st.markdown(f"**Error:** `{hs['error'][:300]}`")

                diff_col1, diff_col2 = st.columns(2)
                with diff_col1:
                    st.markdown("**Original SQL**")
                    st.code(hs["original_sql"], language="sql")
                with diff_col2:
                    st.markdown("**Fixed SQL (Cortex AI)**")
                    st.code(hs["fixed_sql"], language="sql")

                if not _approved:
                    btn_col1, btn_col2 = st.columns(2)
                    with btn_col1:
                        if st.button(f"✅ Approve & Apply", key=f"heal_approve_{idx}", type="primary"):
                            st.session_state.dbt_project_files[hs["file"]] = hs["fixed_sql"]
                            st.session_state["dbt_heal_suggestions"][idx]["approved"] = True
                            _save_heal_knowledge(hs["error"], hs["original_sql"], hs["fixed_sql"], "DBT_MODEL", [])
                            show_toast(f"Fix applied to {hs['model']}", "success")
                            st.rerun()
                    with btn_col2:
                        if st.button(f"❌ Reject", key=f"heal_reject_{idx}"):
                            st.session_state["dbt_heal_suggestions"].pop(idx)
                            show_toast(f"Fix rejected for {hs['model']}", "info")
                            st.rerun()

        _n_approved = sum(1 for h in heal_suggestions if h.get("approved"))
        if _n_approved > 0 and _n_approved == len(heal_suggestions):
            st.success(f"All {_n_approved} fix(es) approved.")
            if st.button("🚀 Apply & Re-run", key="heal_apply_rerun", type="primary"):
                _rerun_ok = True
                with st.spinner("Uploading fixed files to stage..."):
                    try:
                        project_files = st.session_state.get("dbt_project_files", {})
                        for fp, content in project_files.items():
                            file_name = fp.split("/")[-1]
                            stage_folder = "/".join(fp.split("/")[:-1])
                            stage_target = f"{dbt_stage_path}/{stage_folder}" if stage_folder else dbt_stage_path
                            file_buffer = io.BytesIO(content.encode("utf-8"))
                            session.file.put_stream(
                                input_stream=file_buffer,
                                stage_location=f"{stage_target}/{file_name}",
                                auto_compress=False,
                                overwrite=True
                            )
                    except Exception as ue:
                        st.error(f"Upload failed: {ue}")
                        _rerun_ok = False
                if _rerun_ok:
                    with st.spinner("Re-deploying project..."):
                        try:
                            db, schema, project_name, _, _ = _get_project_and_task(dbt_stage_path)
                            session.sql("""
                                CREATE OR REPLACE DBT PROJECT {project_name}
                                FROM '{stage_path}'
                            """.format(
                                project_name=project_name,
                                stage_path=dbt_stage_path
                            )).collect()
                        except Exception as de:
                            st.error(f"Deploy failed: {de}")
                            _rerun_ok = False
                if _rerun_ok:
                    with st.spinner("Dropping failed model tables for clean rebuild..."):
                        for hs in heal_suggestions:
                            _model_name = hs["model"].upper()
                            _file_path = hs.get("file", "")
                            _layer = ""
                            if "/gold/" in _file_path:
                                _layer = "GOLD"
                            elif "/silver/" in _file_path:
                                _layer = "SILVER"
                            elif "/bronze/" in _file_path or "/staging/" in _file_path:
                                _layer = "STAGING"
                            _err_text = hs.get("error", "")
                            _fq_match = re.search(r'table\s+(\S+\.\S+\.\S+\.\S+)', _err_text, re.IGNORECASE)
                            if _fq_match:
                                _fq_table = _fq_match.group(1)
                            else:
                                db, schema, _, _, _ = _get_project_and_task(dbt_stage_path)
                                _target_schema = _layer if _layer else schema
                                _fq_table = f"{db}.{_target_schema}.{_model_name}"
                            try:
                                session.sql(f"DROP TABLE IF EXISTS {_fq_table}").collect()
                            except Exception:
                                try:
                                    session.sql(f"DROP VIEW IF EXISTS {_fq_table}").collect()
                                except Exception:
                                    pass
                if _rerun_ok:
                    with st.spinner("Running dbt run --full-refresh..."):
                        try:
                            _, _, project_name, task_name, curr_wh = _get_project_and_task(dbt_stage_path)
                            state, error = _run_dbt_task(task_name, project_name, curr_wh, "run --full-refresh")
                            if state == "SUCCEEDED":
                                st.session_state.pop("dbt_heal_suggestions", None)
                                st.cache_data.clear()
                                st.session_state["dbt_heal_rerun_result"] = {"state": "SUCCEEDED"}
                                st.rerun()
                            else:
                                st.session_state["dbt_heal_rerun_result"] = {"state": state, "error": error}
                                st.rerun()
                        except Exception as re_err:
                            st.session_state["dbt_heal_rerun_result"] = {"state": "ERROR", "error": str(re_err)}
                            st.rerun()
        elif _n_approved > 0:
            st.info(f"{_n_approved}/{len(heal_suggestions)} fix(es) approved. Review remaining suggestions.")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 8: DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════════
with tab8:
    import pandas as pd

    # ── Helper functions ─────────────────────────────────────────────────────
    @st.cache_data(ttl=30)
    def get_all_dbt_stages():
        stages_found = []
        try:
            databases = session.sql("SHOW DATABASES").collect()
            for db_row in databases:
                db_name = db_row["name"]
                try:
                    schemas = session.sql(
                        f"SHOW SCHEMAS LIKE 'DBT_PROJECTS' IN DATABASE {db_name}"
                    ).collect()
                    if schemas:
                        stages = session.sql(
                            f"SHOW STAGES LIKE 'DBT_PROJECT_STAGE' IN SCHEMA {db_name}.DBT_PROJECTS"
                        ).collect()
                        if stages:
                            stages_found.append({
                                "label":      f"{db_name}.DBT_PROJECTS.DBT_PROJECT_STAGE",
                                "stage_path": f"@{db_name}.DBT_PROJECTS.DBT_PROJECT_STAGE",
                                "database":   db_name,
                                "schema":     "DBT_PROJECTS",
                                "task_name":  f"{db_name}.DBT_PROJECTS.DBT_EXECUTION_TASK",
                            })
                except Exception:
                    continue
        except Exception:
            try:
                all_stages = session.sql(
                    "SHOW STAGES LIKE 'DBT_PROJECT_STAGE' IN ACCOUNT"
                ).collect()
                for stg in all_stages:
                    db_name  = stg.get("database_name", "")
                    sch_name = stg.get("schema_name",   "")
                    stg_name = stg.get("name",           "")
                    if db_name and sch_name and stg_name:
                        stages_found.append({
                            "label":      f"{db_name}.{sch_name}.{stg_name}",
                            "stage_path": f"@{db_name}.{sch_name}.{stg_name}",
                            "database":   db_name,
                            "schema":     sch_name,
                            "task_name":  f"{db_name}.{sch_name}.DBT_EXECUTION_TASK",
                        })
            except Exception:
                pass
        return stages_found

    # ════════════════════════════════════════════════════════════════════════
    # STAGE SELECTOR — everything below is scoped to this selection
    # ════════════════════════════════════════════════════════════════════════
    all_dbt_stages = get_all_dbt_stages()

    if not all_dbt_stages:
        st.info("No `DBT_PROJECT_STAGE` stages found. Deploy a project from the **Execute** tab first.")
        st.stop()

    stage_labels = [s["label"] for s in all_dbt_stages]

    hdr_col, ref_col = st.columns([5, 1])
    with hdr_col:
        selected_label = st.selectbox(
            "🗂️ Select DBT Project Stage",
            options = stage_labels,
            index   = 0,
            key     = "dash_stage_select",
            help    = "All dashboards below are scoped to the selected stage's database"
        )
    with ref_col:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button(":material/refresh: Refresh", type="primary", key="refresh_dashboard"):
            st.cache_data.clear()
            st.rerun()

    selected = next(s for s in all_dbt_stages if s["label"] == selected_label)
    sel_db         = selected["database"]
    sel_schema     = selected["schema"]
    sel_stage_path = selected["stage_path"]
    sel_task_name  = selected["task_name"]
    sel_task_short = sel_task_name.split(".")[-1]

    st.markdown(f"""
    <div style="background:#C2DDA6;border:1px solid #B3D561;
                border-radius:10px;padding:0.55rem 1rem;margin:0.25rem 0 1rem;">
        <span style="color:#3D5622;font-size:0.75rem;font-weight:700;
                     text-transform:uppercase;letter-spacing:0.8px;">Active Context</span>
        &nbsp;&nbsp;
        <code style="color:#272727;font-size:0.82rem;background:transparent;">{sel_stage_path}</code>
        &nbsp;&nbsp;
        <span style="color:#272727;font-size:0.75rem;">·  DB: {sel_db}  ·  Schema: {sel_schema}</span>
    </div>
    """, unsafe_allow_html=True)

    # ════════════════════════════════════════════════════════════════════════
    # PANEL 1 — MODELS CREATED & DEPLOYED
    # ════════════════════════════════════════════════════════════════════════
    st.markdown("""
    <div class="section-header">
        <span class="section-icon">📦</span>
        <h3 class="section-title">Models Created &amp; Deployed</h3>
    </div>""", unsafe_allow_html=True)

    @st.cache_data(ttl=60)
    def _list_stage_files(stage_path):
        try:
            result = session.sql(f"LIST {stage_path}").collect()
            return [row['name'] for row in result]
        except Exception:
            return []

    stage_files = _list_stage_files(sel_stage_path)
    
    proj_bronze, proj_silver, proj_gold = 0, 0, 0
    stage_model_names = {"bronze": [], "silver": [], "gold": []}
    
    for file_path in stage_files:
        if file_path.endswith(".sql") and "/models/" in file_path:
            model_name = file_path.split("/")[-1].replace(".sql", "")
            if "/bronze/" in file_path or "/staging/" in file_path:
                proj_bronze += 1
                stage_model_names["bronze"].append(model_name)
            elif "/silver/" in file_path or "/intermediate/" in file_path:
                proj_silver += 1
                stage_model_names["silver"].append(model_name)
            elif "/gold/" in file_path or "/marts/" in file_path:
                proj_gold += 1
                stage_model_names["gold"].append(model_name)
    proj_total = proj_bronze + proj_silver + proj_gold

    @st.cache_data(ttl=60)
    def _get_deployed_objects_from_stage(db, model_names_by_layer):
        counts = {"TABLE": 0, "VIEW": 0, "DYNAMIC TABLE": 0, "MATERIALIZED VIEW": 0}
        all_model_names = []
        for layer in ["bronze", "silver", "gold"]:
            all_model_names.extend([n.upper() for n in model_names_by_layer.get(layer, [])])
        
        if not all_model_names:
            return counts
        
        try:
            names_list = ",".join([f"'{n}'" for n in all_model_names])
            result = session.sql(f"""
                SELECT TABLE_NAME, TABLE_TYPE 
                FROM {db}.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA IN ('BRONZE', 'SILVER', 'GOLD')
                AND UPPER(TABLE_NAME) IN ({names_list})
            """).collect()
            
            for row in result:
                ttype = row['TABLE_TYPE']
                if ttype == 'BASE TABLE':
                    counts["TABLE"] += 1
                elif ttype == 'VIEW':
                    counts["VIEW"] += 1
                elif ttype == 'DYNAMIC TABLE':
                    counts["DYNAMIC TABLE"] += 1
                elif ttype == 'MATERIALIZED VIEW':
                    counts["MATERIALIZED VIEW"] += 1
        except Exception:
            pass
        return counts

    model_names_tuple = tuple((k, tuple(v)) for k, v in stage_model_names.items())
    deployed_counts = _get_deployed_objects_from_stage(sel_db, dict(model_names_tuple))
    
    mat_tables = deployed_counts["TABLE"]
    mat_views = deployed_counts["VIEW"]
    mat_dyn_tables = deployed_counts["DYNAMIC TABLE"]
    mat_views_mat = deployed_counts["MATERIALIZED VIEW"]
    total_materialized = mat_tables + mat_views + mat_dyn_tables + mat_views_mat

    st.markdown("**Models in Selected DBT Project Stage**")
    m1, m2, m3, m4 = st.columns(4)
    with m1: st.metric("🥉 Bronze", proj_bronze)
    with m2: st.metric("🥈 Silver", proj_silver)
    with m3: st.metric("🥇 Gold", proj_gold)
    with m4: st.metric("📊 Total Models", proj_total)

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("**Materialized Objects in Snowflake (from this project)**")
    o1, o2, o3, o4, o5 = st.columns(5)
    with o1: st.metric("📋 Tables", mat_tables)
    with o2: st.metric("👁️ Views", mat_views)
    with o3: st.metric("⚡ Dynamic Tables", mat_dyn_tables)
    with o4: st.metric("🔷 Materialized Views", mat_views_mat)
    with o5: st.metric("✅ Total Deployed", total_materialized)

    st.divider()

    # ════════════════════════════════════════════════════════════════════════
    # PANEL 2 — DBT RUN HISTORY
    # ════════════════════════════════════════════════════════════════════════
    st.markdown("""
    <div class="section-header">
        <span class="section-icon">🕐</span>
        <h3 class="section-title">DBT Run History</h3>
    </div>""", unsafe_allow_html=True)

    try:
        hist_df = session.sql(f"""
            SELECT
                SCHEDULED_TIME, QUERY_START_TIME, COMPLETED_TIME,
                STATE, ERROR_CODE, ERROR_MESSAGE, QUERY_ID,
                DATEDIFF('second', QUERY_START_TIME, COMPLETED_TIME) AS DURATION_SEC
            FROM TABLE({sel_db}.INFORMATION_SCHEMA.TASK_HISTORY(
                TASK_NAME => '{sel_task_short}',
                SCHEDULED_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP())
            ))
            ORDER BY SCHEDULED_TIME DESC
            LIMIT 50
        """).to_pandas()

        _reset_ts = st.session_state.get('reset_timestamp')
        if _reset_ts and len(hist_df) > 0:
            try:
                hist_df["SCHEDULED_TIME"] = pd.to_datetime(hist_df["SCHEDULED_TIME"]).dt.tz_localize(None)
            except Exception:
                pass
            hist_df = hist_df[hist_df["SCHEDULED_TIME"] > pd.Timestamp(_reset_ts)]

        if len(hist_df) > 0:
            succeeded = int((hist_df["STATE"] == "SUCCEEDED").sum())
            failed    = int((hist_df["STATE"] == "FAILED").sum())
            running   = int((~hist_df["STATE"].isin(["SUCCEEDED","FAILED","CANCELLED"])).sum())
            avg_dur   = hist_df["DURATION_SEC"].dropna()
            avg_str   = f"{int(avg_dur.mean())}s" if len(avg_dur) else "—"

            # ── KPI metrics ───────────────────────────────────────────
            h1, h2, h3, h4, h5 = st.columns(5)
            with h1: st.metric("Total Runs (7d)", len(hist_df))
            with h2: st.metric("✅ Succeeded",    succeeded)
            with h3: st.metric("❌ Failed",        failed)
            with h4: st.metric("⏳ In Progress",   running)
            with h5: st.metric("⏱ Avg Duration",  avg_str)

            st.markdown("<br>", unsafe_allow_html=True)

            # ── Build dropdown options ────────────────────────────────
            def _run_label(row):
                state    = str(row["STATE"])
                icon     = "✅" if state == "SUCCEEDED" else ("❌" if state == "FAILED" else "⏳")
                start_ts = str(row["QUERY_START_TIME"])[:19] if row["QUERY_START_TIME"] else "—"
                dur      = f"{int(row['DURATION_SEC'])}s" if pd.notna(row["DURATION_SEC"]) else "—"
                return f"{icon}  {start_ts}  ·  {state}  ·  ⏱ {dur}"

            run_labels  = [_run_label(row) for _, row in hist_df.iterrows()]
            selected_run = st.selectbox(
                "Select a Run",
                options  = run_labels,
                index    = 0,
                key      = "run_history_select"
            )

            # ── Detail panel for selected run ─────────────────────────
            selected_idx = run_labels.index(selected_run)
            row          = hist_df.iloc[selected_idx]

            state    = str(row["STATE"])
            start_ts = str(row["QUERY_START_TIME"])[:19] if row["QUERY_START_TIME"] else "—"
            end_ts   = str(row["COMPLETED_TIME"])[:19]   if row["COMPLETED_TIME"]   else "—"
            dur      = f"{int(row['DURATION_SEC'])}s"    if pd.notna(row["DURATION_SEC"]) else "—"
            query_id = str(row["QUERY_ID"])              if row["QUERY_ID"]         else ""
            err_msg  = str(row["ERROR_MESSAGE"])         if row["ERROR_MESSAGE"]    else ""
            err_code = str(row["ERROR_CODE"])            if row["ERROR_CODE"]       else ""

            state_color = "#7AB929" if state == "SUCCEEDED" else ("#DC2626" if state == "FAILED" else "#B8860B")

            with st.container(border=True):
                d1, d2, d3 = st.columns(3)

                # ── Column 1: Run Info ────────────────────────────────
                with d1:
                    st.markdown("**🗒 Run Info**")
                    st.markdown(
                        f'<div style="font-size:0.82rem;line-height:2;color:#6B7280;">'
                        f'<b style="color:#1A1A1A;">Status</b><br>'
                        f'<span style="background:rgba({("122,185,41" if state=="SUCCEEDED" else "220,38,38" if state=="FAILED" else "184,134,11")},0.15);'
                        f'color:{state_color};padding:2px 10px;border-radius:10px;font-weight:700;font-size:0.78rem;">'
                        f'{state}</span>'
                        f'<br><br>'
                        f'<b style="color:#1A1A1A;">Started</b><br>{start_ts}'
                        f'<br><b style="color:#1A1A1A;">Completed</b><br>{end_ts}'
                        f'<br><b style="color:#1A1A1A;">Duration</b><br>{dur}'
                        f'</div>',
                        unsafe_allow_html=True
                    )

                # ── Column 2: Query ID ────────────────────────────────
                with d2:
                    st.markdown("**🔑 Query ID**")
                    if query_id:
                        st.code(query_id, language="text")
                    else:
                        st.caption("No query ID available")

                    if err_code:
                        st.markdown(f"**Error Code:** `{err_code}`")

                # ── Column 3: Full Log Query ──────────────────────────
                with d3:
                    st.markdown("**📋 Full Log Query**")
                    if query_id:
                        st.code(
                            f"SELECT SYSTEM$GET_DBT_LOG('{query_id}')",
                            language="sql"
                        )
                        st.caption("Copy and run in Snowflake worksheet to see full dbt output")
                    else:
                        st.caption("No query ID — log unavailable")

                # ── Error detail (full width, only if failed) ─────────
                if err_msg:
                    st.markdown("**⚠️ Error Detail**")
                    model_match = re.search(r"model '(\w+)'", err_msg)
                    if model_match:
                        st.error(f"Failed Model: `{model_match.group(1)}`")
                    st.code(err_msg[:1000], language="text")
                elif state == "SUCCEEDED":
                    st.success("Run completed successfully with no errors.")

        else:
            st.info("No run history found for the last 7 days.")

    except Exception as e:
        st.warning(f"Run history unavailable: {e}")

    st.divider()

    # ════════════════════════════════════════════════════════════════════════
    # PANEL 3 — LATEST DBT TEST RESULTS
    # ════════════════════════════════════════════════════════════════════════
    st.markdown("""
    <div class="section-header">
        <span class="section-icon">🧪</span>
        <h3 class="section-title">Latest DBT Test Results</h3>
    </div>""", unsafe_allow_html=True)

    try:
        test_df = session.sql(f"""
            SELECT
                SCHEDULED_TIME, QUERY_START_TIME, COMPLETED_TIME,
                STATE, ERROR_MESSAGE, QUERY_ID,
                DATEDIFF('second', QUERY_START_TIME, COMPLETED_TIME) AS DURATION_SEC
            FROM TABLE({sel_db}.INFORMATION_SCHEMA.TASK_HISTORY(
                TASK_NAME => '{sel_task_short}',
                SCHEDULED_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP())
            ))
            ORDER BY SCHEDULED_TIME DESC
            LIMIT 5
        """).to_pandas()

        _reset_ts = st.session_state.get('reset_timestamp')
        if _reset_ts and len(test_df) > 0:
            try:
                test_df["SCHEDULED_TIME"] = pd.to_datetime(test_df["SCHEDULED_TIME"]).dt.tz_localize(None)
            except Exception:
                pass
            test_df = test_df[test_df["SCHEDULED_TIME"] > pd.Timestamp(_reset_ts)]

        if len(test_df) > 0:
            latest   = test_df.iloc[0]
            state    = str(latest["STATE"])
            start_ts = str(latest["QUERY_START_TIME"])[:19] if latest["QUERY_START_TIME"] else "—"
            dur      = f"{int(latest['DURATION_SEC'])}s"    if pd.notna(latest["DURATION_SEC"]) else "—"
            query_id = str(latest["QUERY_ID"])              if latest["QUERY_ID"]        else ""
            err_msg  = str(latest["ERROR_MESSAGE"])         if latest["ERROR_MESSAGE"]    else ""

            if err_msg:
                failed_tests  = re.findall(r"test '(\w+)'",   err_msg)
                failed_models = re.findall(r"model '(\w+)'",  err_msg)
                test_counts   = re.findall(r"(\d+) of (\d+) tests failed", err_msg)

                if test_counts:
                    tc1, tc2 = st.columns(2)
                    with tc1: st.metric("❌ Tests Failed", test_counts[0][0])
                    with tc2: st.metric("Total Tests",    test_counts[0][1])

                if failed_tests:
                    st.markdown("**Failed Tests:**")
                    for t in failed_tests:
                        st.markdown(f"<span class='status-badge status-error'>FAIL</span>&nbsp;`{t}`",
                                    unsafe_allow_html=True)
                if failed_models:
                    st.markdown("**Affected Models:**")
                    for m in failed_models:
                        st.markdown(f"<span class='status-badge status-error'>MODEL</span>&nbsp;`{m}`",
                                    unsafe_allow_html=True)

                with st.expander("Full Error Output"):
                    st.code(err_msg, language="text")

            # Tests defined in schema.ymls for this project
            if "dbt_project_files" in st.session_state:
                test_rows = []
                for file_path, content in st.session_state.dbt_project_files.items():
                    if "schema.yml" in file_path:
                        layer = file_path.split("/")[1].upper() if "/" in file_path else "UNKNOWN"
                        for ttype in re.findall(
                            r"\s+- (not_null|unique|accepted_values|relationships)", content
                        ):
                            test_rows.append({"Layer": layer, "Test Type": ttype})
                if test_rows:
                    summary = (pd.DataFrame(test_rows)
                               .groupby(["Layer","Test Type"])
                               .size().reset_index(name="Count"))
                    st.markdown("**Tests Defined in schema.yml**")
                    st.dataframe(summary, use_container_width=True, hide_index=True)

            if query_id:
                st.markdown("**📋 Latest Test Log**")
                st.code(f"SELECT SYSTEM$GET_DBT_LOG('{query_id}')", language="sql")
        else:
            st.info("No test history found.")

    except Exception as e:
        st.warning(f"Test results unavailable: {e}")

    st.divider()

    # ════════════════════════════════════════════════════════════════════════
    # PANEL 4 — MODEL LINEAGE (ALL DEPLOYED MODELS)
    # ════════════════════════════════════════════════════════════════════════
    st.markdown("""
    <div class="section-header">
        <span class="section-icon">🔗</span>
        <h3 class="section-title">Model Lineage</h3>
    </div>""", unsafe_allow_html=True)

    try:
        def _extract_deps(sql):
            refs    = re.findall(r'{{\s*ref\s*\(\s*["\'](\w+)["\']\s*\)\s*}}',   sql or "")
            sources = re.findall(
                r'{{\s*source\s*\(\s*["\'](\w+)["\']\s*,\s*["\'](\w+)["\']\s*\)\s*}}', sql or ""
            )
            return refs, [s[1] for s in sources]

        node_layer, edges = {}, []
        all_models = {"bronze": [], "silver": [], "gold": []}
        all_sources = []

        @st.cache_data(ttl=60)
        def _load_lineage_from_stage(stage_path):
            _models = {"bronze": [], "silver": [], "gold": []}
            _sources = []
            try:
                _files = [r.as_dict() for r in session.sql(f"LIST {stage_path}").collect()]
            except Exception:
                return _models, _sources

            for f in _files:
                fname = f.get("name", "")
                if fname.endswith(".sql") and "/models/" in fname:
                    rel_path = "/".join(fname.split("/")[1:])
                    model_name = fname.split("/")[-1].replace(".sql", "")
                    if "/bronze/" in fname or "/staging/" in fname:
                        layer = "bronze"
                    elif "/silver/" in fname or "/intermediate/" in fname:
                        layer = "silver"
                    elif "/gold/" in fname or "/marts/" in fname:
                        layer = "gold"
                    else:
                        continue
                    content = read_file_from_stage(stage_path, rel_path) or ""
                    _models[layer].append({"name": model_name, "sql": content, "description": ""})

                elif fname.endswith("sources.yml"):
                    rel_path = "/".join(fname.split("/")[1:])
                    content = read_file_from_stage(stage_path, rel_path)
                    if content:
                        try:
                            src_data = yaml.safe_load(content) or {}
                            for src_block in src_data.get("sources", []):
                                for tbl in src_block.get("tables", []):
                                    tbl_name = tbl.get("name", "")
                                    if tbl_name and tbl_name not in [s.get("name") for s in _sources]:
                                        _sources.append({"name": tbl_name, "description": tbl.get("description", "")})
                        except Exception:
                            pass
            return _models, _sources

        stage_models, stage_sources = _load_lineage_from_stage(sel_stage_path)
        for layer in ["bronze", "silver", "gold"]:
            all_models[layer].extend(stage_models[layer])
        all_sources.extend(stage_sources)

        # ── Fallback 1: session-state dbt_project_files (always available after upload) ──
        if 'dbt_project_files' in st.session_state:
            for fpath, fcontent in st.session_state.dbt_project_files.items():
                if fpath.endswith(".sql") and "models/" in fpath:
                    mname = fpath.split("/")[-1].replace(".sql", "")
                    if "/bronze/" in fpath or "/staging/" in fpath:
                        layer = "bronze"
                    elif "/silver/" in fpath or "/intermediate/" in fpath:
                        layer = "silver"
                    elif "/gold/" in fpath or "/marts/" in fpath:
                        layer = "gold"
                    else:
                        continue
                    if mname not in [em.get("name") for em in all_models[layer]]:
                        all_models[layer].append({"name": mname, "sql": fcontent, "description": ""})

                elif fpath.endswith("sources.yml"):
                    try:
                        src_data = yaml.safe_load(fcontent) or {}
                        for src_block in src_data.get("sources", []):
                            for tbl in src_block.get("tables", []):
                                tbl_name = tbl.get("name", "")
                                if tbl_name and tbl_name not in [s.get("name") for s in all_sources]:
                                    all_sources.append({"name": tbl_name, "description": tbl.get("description", "")})
                    except Exception:
                        pass

        # ── Fallback 2: converted_objects DBT_PROJECT entries ──────────────
        for dbt_proj in st.session_state.get("converted_objects", {}).get("DBT_PROJECT", []):
            for layer in ["bronze", "silver", "gold"]:
                for m in dbt_proj.get(f"{layer}_models", []):
                    if m.get("name") not in [em.get("name") for em in all_models[layer]]:
                        all_models[layer].append(m)

        has_models = any(len(all_models[layer]) > 0 for layer in ["bronze", "silver", "gold"])
        
        if not has_models:
            st.info("No dbt model files found in the selected stage. "
                     "Convert a stored procedure from the **SP → dbt** tab, "
                     "then upload the project from the **Execute** tab to see the lineage graph here.")
        else:
            for src in all_sources:
                src_name = src.get("name", "")
                if src_name:
                    node_layer[src_name] = "source"

            for layer in ["bronze", "silver", "gold"]:
                for model in all_models[layer]:
                    mname = model.get("name", "")
                    if not mname:
                        continue
                    node_layer[mname] = layer
                    refs, src_deps = _extract_deps(model.get("sql", ""))
                    for r in refs:
                        edges.append((r, mname))
                    for s in src_deps:
                        node_layer.setdefault(s, "source")
                        edges.append((s, mname))

            gv_style = {
                "source": 'style=filled fillcolor="#F3F4F6" color="#9CA3AF" fontcolor="#6B7280"',
                "bronze": 'style=filled fillcolor="#FFF8E1" color="#B8860B" fontcolor="#92700C"',
                "silver": 'style=filled fillcolor="#F0F9E8" color="#7AB929" fontcolor="#5C9A1E"',
                "gold":   'style=filled fillcolor="#E8F5E9" color="#2E7D32" fontcolor="#1B5E20"',
            }
            cluster_meta = {
                "source": ("#9CA3AF", "📥 SOURCES"),
                "bronze": ("#B8860B", "🥉 BRONZE"),
                "silver": ("#7AB929", "🥈 SILVER"),
                "gold":   ("#2E7D32", "🥇 GOLD"),
            }

            dot = [
                'digraph dbt_lineage {',
                '    rankdir=LR;',
                '    graph [bgcolor="#FFFFFF" pad="0.5"];',
                '    node  [fontname="Arial" fontsize=10 shape=box width=1.6 height=0.5];',
                '    edge  [color="#D1D5DB" arrowsize=0.7];',
            ]
            for layer in ["source", "bronze", "silver", "gold"]:
                nodes = [n for n, l in node_layer.items() if l == layer]
                if not nodes:
                    continue
                color, label = cluster_meta[layer]
                dot += [
                    f'    subgraph cluster_{layer} {{',
                    f'        label="{label}"; fontcolor="{color}";',
                    f'        fontsize=11; fontname="Arial Bold";',
                    f'        color="{color}"; style=rounded;',
                    *[f'        "{n}" [{gv_style[layer]}];' for n in nodes],
                    '    }',
                ]
            for src, tgt in edges:
                c = cluster_meta.get(node_layer.get(src, "source"), ("#484f58", ""))[0]
                dot.append(f'    "{src}" -> "{tgt}" [color="{c}"];')
            dot.append("}")

            st.graphviz_chart("\n".join(dot), use_container_width=True)

            with st.expander("📋 Lineage Details"):
                rows = []
                for layer in ["bronze", "silver", "gold"]:
                    for model in all_models[layer]:
                        mname = model.get("name", "")
                        refs, srcs = _extract_deps(model.get("sql", ""))
                        rows.append({
                            "Layer":       layer.upper(),
                            "Model":       mname,
                            "Depends On":  ", ".join(refs + srcs) or "—",
                            "Description": model.get("description", ""),
                        })
                if rows:
                    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"Lineage error: {e}")

    st.divider()

    # ════════════════════════════════════════════════════════════════════════
    # PANEL 5 — STAGE FILE EXPLORER
    # ════════════════════════════════════════════════════════════════════════
    st.markdown("""
    <div class="section-header">
        <span class="section-icon">🗂️</span>
        <h3 class="section-title">Stage File Explorer</h3>
    </div>""", unsafe_allow_html=True)

    def build_file_tree(file_rows: list) -> dict:
        tree = {}
        for row in file_rows:
            parts      = row["name"].split("/")
            path_parts = parts[1:] if len(parts) > 1 else parts
            file_size  = row.get("size", 0)
            node = tree
            for part in path_parts[:-1]:
                node = node.setdefault(part, {})
            if path_parts and path_parts[-1]:
                node[path_parts[-1]] = file_size
        return tree

    def render_tree_streamlit(tree: dict, parent_path: str = "", depth: int = 0, stage_path: str = ""):
        LAYER_COLORS = {
            "bronze": "#B8860B", "staging":      "#B8860B",
            "silver": "#7AB929", "intermediate": "#7AB929",
            "gold":   "#2E7D32", "marts":        "#2E7D32",
            "macros": "#7C3AED", "tests":        "#7C3AED",
        }
        FILE_ICONS = {
            ".sql": "📄", ".yml": "⚙️", ".yaml": "⚙️",
            ".md":  "📝", ".txt": "📝", ".zip":  "📦",
        }
        LAYER_EMOJIS = {
            "bronze": "🥉", "silver": "🥈", "gold": "🥇",
            "staging": "📥", "macros": "🔧", "tests": "🧪",
            "intermediate": "⚙️", "marts": "🏪",
        }

        sorted_items = sorted(tree.items(), key=lambda x: (isinstance(x[1], int), x[0]))

        for key, value in sorted_items:
            full_path = f"{parent_path}/{key}" if parent_path else key

            if isinstance(value, dict):
                color        = LAYER_COLORS.get(key.lower(), "#1A1A1A")
                emoji        = LAYER_EMOJIS.get(key.lower(), "📁")
                file_count   = sum(1 for v in value.values() if isinstance(v, int))
                folder_count = sum(1 for v in value.values() if isinstance(v, dict))

                summary_parts = []
                if folder_count: summary_parts.append(f"{folder_count} folder{'s' if folder_count>1 else ''}")
                if file_count:   summary_parts.append(f"{file_count} file{'s' if file_count>1 else ''}")
                summary = "  ·  ".join(summary_parts)

                if depth > 0:
                    _, content_col = st.columns([depth * 0.15, 10 - depth * 0.15])
                else:
                    content_col = st

                with content_col.expander(f"{emoji}  {key}   ({summary})",expanded=(depth == 0)):
                    render_tree_streamlit(value, full_path, depth + 1, stage_path=stage_path)

            else:
                ext        = "." + key.rsplit(".", 1)[-1] if "." in key else ""
                icon       = FILE_ICONS.get(ext.lower(), "📄")
                size_str   = f"{value/1024:.1f} KB" if value >= 1024 else f"{value} B"
                file_color = "#6B7280"
                for layer, color in LAYER_COLORS.items():
                    if layer in full_path.lower():
                        file_color = color
                        break
                indent_px = depth * 20
                st.markdown(
                    f'<div style="padding:4px 0 4px {indent_px}px;display:flex;align-items:center;'
                    f'gap:8px;border-bottom:1px solid rgba(229,231,235,0.6);">'
                    f'<span style="font-size:0.82rem;">{icon}</span>'
                    f'<span style="color:{file_color};font-size:0.82rem;">{key}</span>'
                    f'<span style="color:#9CA3AF;font-size:0.7rem;margin-left:auto;padding-right:4px;">'
                    f'{size_str}</span></div>',
                    unsafe_allow_html=True
                )
                if stage_path and ext.lower() in ('.sql', '.yml', '.yaml', '.md', '.txt'):
                    with st.expander(f"📖 View: {key}", expanded=False):
                        try:
                            _file_content = read_file_from_stage(stage_path, full_path)
                            if _file_content:
                                _lang = "sql" if ext.lower() == ".sql" else "yaml" if ext.lower() in (".yml", ".yaml") else "text"
                                st.code(_file_content, language=_lang)
                            else:
                                st.caption("File is empty or could not be read.")
                        except Exception as _fe:
                            st.caption(f"Could not read file: {_fe}")

    try:
        # Convert rows to dicts — required for .get() to work on Snowpark Row objects
        list_rows = [r.as_dict() for r in session.sql(f"LIST {sel_stage_path}").collect()]

        if not list_rows:
            st.warning(f"`{sel_stage_path}` is empty. Upload a project from the **Execute** tab.")
        else:
            total_files = len(list_rows)
            total_size  = sum(r.get("size", 0) for r in list_rows)
            sql_files   = sum(1 for r in list_rows if r.get("name", "").endswith(".sql"))
            yml_files   = sum(1 for r in list_rows if r.get("name", "").endswith((".yml", ".yaml")))

            # ── KPI metrics ───────────────────────────────────────────────
            fm1, fm2, fm3, fm4 = st.columns(4)
            with fm1: st.metric("Total Files",  total_files)
            with fm2: st.metric("SQL Models",   sql_files)
            with fm3: st.metric("YAML Configs", yml_files)
            with fm4: st.metric(
                "Total Size",
                f"{total_size/1024:.1f} KB" if total_size >= 1024 else f"{total_size} B"
            )

            st.markdown("<br>", unsafe_allow_html=True)

            # ── Stage header bar ──────────────────────────────────────────
            st.markdown(
                f'<div style="background:#F7F8F6;'
                f'border:1px solid #E5E7EB;'
                f'border-radius:12px 12px 0 0;'
                f'padding:0.7rem 1rem;">'
                f'<span style="color:#7AB929;font-size:0.82rem;font-weight:700;">'
                f'📦 {sel_stage_path}</span>'
                f'<span style="color:#9CA3AF;font-size:0.72rem;margin-left:10px;">'
                f'{total_files} files · '
                f'{sql_files} models · '
                f'{yml_files} configs</span>'
                f'</div>',
                unsafe_allow_html=True
            )

            # ── Collapsible tree ──────────────────────────────────────────
            with st.container(border=True):
                file_data = [
                    {"name": r.get("name", ""), "size": r.get("size", 0)}
                    for r in list_rows
                ]
                tree = build_file_tree(file_data)
                render_tree_streamlit(tree, stage_path=sel_stage_path)

            # ── Raw file list (collapsed by default) ──────────────────────
            # with st.expander("📋 Raw File List"):
            #     import pandas as pd
            #     raw_df = pd.DataFrame([{
            #         "File Path":     r.get("name", ""),
            #         "Size (bytes)":  r.get("size", 0),
            #         "Last Modified": r.get("last_modified", "")
            #     } for r in list_rows])
            #     st.dataframe(raw_df, use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"Could not list stage: {e}")

    st.divider()

    # ════════════════════════════════════════════════════════════════════════
    # PANEL 6 — CONVERSION TRENDS
    # ════════════════════════════════════════════════════════════════════════
    st.markdown("""
    <div class="section-header">
        <span class="section-icon">📊</span>
        <h3 class="section-title">Conversion Trends</h3>
    </div>""", unsafe_allow_html=True)

    try:
        conv_rows = session.sql("""
            SELECT CONVERSION_TYPE, COUNT(*) AS CNT,
                   MIN(CREATED_AT) AS FIRST_CONVERSION,
                   MAX(CREATED_AT) AS LAST_CONVERSION
            FROM CONVERTED_OBJECTS
            GROUP BY CONVERSION_TYPE
            ORDER BY CNT DESC
        """).collect()

        if conv_rows:
            total_conversions = sum(r["CNT"] for r in conv_rows)
            type_count = len(conv_rows)
            latest = max(r["LAST_CONVERSION"] for r in conv_rows)

            tc1, tc2, tc3 = st.columns(3)
            with tc1: st.metric("Total Conversions", total_conversions)
            with tc2: st.metric("Conversion Types", type_count)
            with tc3: st.metric("Latest", latest.strftime("%Y-%m-%d %H:%M") if latest else "—")

            chart_data = pd.DataFrame([
                {"Conversion Type": r["CONVERSION_TYPE"], "Count": r["CNT"]}
                for r in conv_rows
            ]).set_index("Conversion Type")
            st.bar_chart(chart_data, color="#7AB929")

            with st.expander("📋 Conversion Details"):
                detail_df = pd.DataFrame([{
                    "Type": r["CONVERSION_TYPE"],
                    "Count": r["CNT"],
                    "First": r["FIRST_CONVERSION"].strftime("%Y-%m-%d %H:%M") if r["FIRST_CONVERSION"] else "—",
                    "Latest": r["LAST_CONVERSION"].strftime("%Y-%m-%d %H:%M") if r["LAST_CONVERSION"] else "—",
                } for r in conv_rows])
                st.dataframe(detail_df, use_container_width=True, hide_index=True)
        else:
            st.info("No conversions recorded yet. Convert objects from the other tabs to see trends here.")
    except Exception as e:
        st.error(f"Conversion trends error: {e}")

    st.divider()

    # ════════════════════════════════════════════════════════════════════════
    # PANEL 7 — CoCo CONVERSION PERFORMANCE
    # ════════════════════════════════════════════════════════════════════════
    st.markdown("""
    <div class="section-header">
        <span class="section-icon">⚡</span>
        <h3 class="section-title">CoCo Conversion Performance</h3>
    </div>""", unsafe_allow_html=True)

    try:
        _perf_rows = session.sql(f"""
            SELECT
                CONVERSION_TYPE,
                SOURCE_NAME,
                DURATION_SECS,
                STATUS,
                CREATED_AT
            FROM {TARGET_SCHEMA}.{CONVERSION_TABLE}
            ORDER BY CREATED_AT DESC
        """).collect()

        if _perf_rows:
            _type_map = {
                "TABLE_DDL": "Tables",
                "VIEW_DDL": "Views",
                "FUNCTION": "Functions",
                "PROCEDURE": "Procedures",
                "STORED_PROCEDURE": "Procedures",
                "DBT_PROJECT": "SP → dbt Models",
                "SSIS_PACKAGE": "SSIS Packages",
            }

            _type_stats = {}
            _total_secs = 0.0
            _total_count = 0

            for r in _perf_rows:
                raw_type = r["CONVERSION_TYPE"]
                label = _type_map.get(raw_type, raw_type)
                dur = float(r["DURATION_SECS"] or 0)
                if label not in _type_stats:
                    _type_stats[label] = {"count": 0, "total_secs": 0.0, "min_secs": None, "max_secs": 0.0}
                _type_stats[label]["count"] += 1
                _type_stats[label]["total_secs"] += dur
                if _type_stats[label]["min_secs"] is None or dur < _type_stats[label]["min_secs"]:
                    _type_stats[label]["min_secs"] = dur
                if dur > _type_stats[label]["max_secs"]:
                    _type_stats[label]["max_secs"] = dur
                _total_secs += dur
                _total_count += 1

            def _fmt_dur(secs):
                if secs is None or secs == 0:
                    return "—"
                if secs < 60:
                    return f"{secs:.1f}s"
                m, s = divmod(int(secs), 60)
                return f"{m}m {s}s"

            pk1, pk2, pk3, pk4 = st.columns(4)
            with pk1: st.metric("Total Conversions", _total_count)
            with pk2: st.metric("Total Time", _fmt_dur(_total_secs))
            with pk3: st.metric("Avg per Conversion", _fmt_dur(_total_secs / _total_count) if _total_count else "—")
            with pk4: st.metric("Object Types", len(_type_stats))

            _type_order = ["Tables", "Views", "Functions", "Procedures", "SP → dbt Models", "SSIS Packages"]
            _sorted_types = sorted(_type_stats.keys(), key=lambda t: _type_order.index(t) if t in _type_order else 99)

            _chart_df = pd.DataFrame([
                {"Conversion Type": t, "Total Seconds": round(_type_stats[t]["total_secs"], 1)}
                for t in _sorted_types
            ]).set_index("Conversion Type")
            st.bar_chart(_chart_df, color="#7AB929")

            _breakdown = []
            for t in _sorted_types:
                s = _type_stats[t]
                avg = s["total_secs"] / s["count"] if s["count"] else 0
                _breakdown.append({
                    "Conversion Type": t,
                    "Count": s["count"],
                    "Total Time": _fmt_dur(s["total_secs"]),
                    "Avg Time": _fmt_dur(avg),
                    "Fastest": _fmt_dur(s["min_secs"]),
                    "Slowest": _fmt_dur(s["max_secs"]),
                })
            st.dataframe(pd.DataFrame(_breakdown), use_container_width=True, hide_index=True)

            with st.expander("📜 Recent Conversions"):
                _recent = []
                for r in _perf_rows[:30]:
                    _recent.append({
                        "Type": _type_map.get(r["CONVERSION_TYPE"], r["CONVERSION_TYPE"]),
                        "Object": r["SOURCE_NAME"],
                        "Duration": _fmt_dur(float(r["DURATION_SECS"] or 0)),
                        "Status": r["STATUS"],
                        "Converted At": r["CREATED_AT"].strftime("%Y-%m-%d %H:%M:%S") if r["CREATED_AT"] else "—",
                    })
                st.dataframe(pd.DataFrame(_recent), use_container_width=True, hide_index=True)
        else:
            st.info("No conversions recorded yet. Convert objects from the other tabs to see performance stats here.")
    except Exception as e:
        st.error(f"Conversion performance error: {e}")

    st.divider()

    # ════════════════════════════════════════════════════════════════════════
    # PANEL 8 — HEAL KNOWLEDGE BASE (Auto-Learned Patterns)
    # ════════════════════════════════════════════════════════════════════════
    st.markdown("""
    <div class="section-header">
        <span class="section-icon">🧠</span>
        <h3 class="section-title">Auto-Heal Knowledge Base</h3>
    </div>""", unsafe_allow_html=True)

    st.caption("Patterns learned automatically from successful Auto-Heal corrections. These are injected into future conversion prompts to prevent the same errors.")

    try:
        kb_rows = session.sql(f"""
            SELECT ERROR_PATTERN, ERROR_CODE, FIX_DESCRIPTION, OBJECT_TYPE,
                   OCCURRENCE_COUNT, CREATED_AT, LAST_SEEN_AT,
                   ORIGINAL_DDL, HEALED_DDL
            FROM {TARGET_SCHEMA}.{HEAL_KB_TABLE}
            ORDER BY OCCURRENCE_COUNT DESC
        """).collect()

        if kb_rows:
            total_patterns = len(kb_rows)
            total_heals = sum(r["OCCURRENCE_COUNT"] for r in kb_rows)
            top_pattern = kb_rows[0]["ERROR_PATTERN"][:60] + "..." if len(kb_rows[0]["ERROR_PATTERN"]) > 60 else kb_rows[0]["ERROR_PATTERN"]

            kb1, kb2, kb3 = st.columns(3)
            with kb1: st.metric("Learned Patterns", total_patterns)
            with kb2: st.metric("Total Heals Applied", total_heals)
            with kb3: st.metric("Most Common", f"{kb_rows[0]['OCCURRENCE_COUNT']}x")

            kb_data = []
            for r in kb_rows:
                kb_data.append({
                    "Pattern": r["ERROR_PATTERN"][:80],
                    "Code": r["ERROR_CODE"] or "—",
                    "Type": r["OBJECT_TYPE"],
                    "Seen": f"{r['OCCURRENCE_COUNT']}x",
                    "First Seen": r["CREATED_AT"].strftime("%Y-%m-%d %H:%M") if r["CREATED_AT"] else "—",
                    "Last Seen": r["LAST_SEEN_AT"].strftime("%Y-%m-%d %H:%M") if r["LAST_SEEN_AT"] else "—",
                })
            st.dataframe(pd.DataFrame(kb_data), use_container_width=True, hide_index=True)

            with st.expander("📋 Full Pattern Details"):
                for r in kb_rows:
                    st.markdown(
                        f'<div style="padding:10px;margin:8px 0;border-radius:8px;'
                        f'background:rgba(122,185,41,0.06);border-left:3px solid #7AB929;">'
                        f'<span style="color:#1A1A1A;font-weight:700;">🔁 [{r["OCCURRENCE_COUNT"]}x]</span> '
                        f'<span style="color:#6B7280;font-size:0.85rem;">{r["OBJECT_TYPE"]}</span>'
                        f'<br><span style="color:#DC2626;font-size:0.82rem;">Error: {r["ERROR_PATTERN"][:150]}</span>'
                        f'</div>', unsafe_allow_html=True
                    )
                    if r["FIX_DESCRIPTION"]:
                        st.markdown("**🔧 Resolution:**")
                        st.markdown(r["FIX_DESCRIPTION"][:1000])
                    if r["ORIGINAL_DDL"] and r["HEALED_DDL"]:
                        col_before, col_after = st.columns(2)
                        with col_before:
                            st.markdown("**❌ Before (failed):**")
                            st.code(r["ORIGINAL_DDL"][:800] + ("..." if len(r["ORIGINAL_DDL"] or "") > 800 else ""), language="sql")
                        with col_after:
                            st.markdown("**✅ After (fixed):**")
                            st.code(r["HEALED_DDL"][:800] + ("..." if len(r["HEALED_DDL"] or "") > 800 else ""), language="sql")
                    st.markdown("---")
        else:
            st.info("No patterns learned yet. Deploy objects with errors — Auto-Heal will learn and track fixes automatically.")
    except Exception as e:
        st.warning(f"Knowledge base unavailable: {e}")

    st.divider()

    # ════════════════════════════════════════════════════════════════════════
    # PANEL 9 — RESET APP
    # ════════════════════════════════════════════════════════════════════════
    st.markdown("""
    <div class="section-header">
        <span class="section-icon">🔄</span>
        <h3 class="section-title">Reset App</h3>
    </div>""", unsafe_allow_html=True)

    st.caption("Clears all converted files, deployed models, dbt project files, stage artifacts, docs, audit history, and KPIs. The app will restart as if freshly opened.")

    rc1, rc2, rc3 = st.columns([1, 1, 3])
    with rc1:
        if st.button("🔄 Reset Everything", type="primary", key="reset_app_full"):
            reset_progress = st.progress(0, text="Resetting...")

            RESET_KEYS = [
                'converted_objects', 'deployed_models',
                'dbt_project_files', 'dbt_docs_html', 'dbt_docs_cache', 'dbt_docs_stage',
                'dbt_docs_db', 'dbt_docs_schema', 'dbt_docs_read_sp',
                'ssis_content', 'converted_ssis',
                'medallion_models', 'current_project_name',
                'source_database', 'source_schema',
                'converted_table_ddl', 'converted_view_ddl', 'converted_sp', 'converted_udf',
                'multi_conversions_table', 'multi_conversions_view',
                'multi_conversions_udf', 'multi_conversions_sp',
                'multi_conversions_table_heal_results', 'multi_conversions_view_heal_results',
                'multi_conversions_udf_heal_results', 'multi_conversions_sp_heal_results',
                'stage_upload_done', 'project_deployed',
                'dbt_stage', 'dbt_stage_ready', 'dbt_stage_error',
            ]
            for k in RESET_KEYS:
                if k in st.session_state:
                    del st.session_state[k]
            extra_keys = [k for k in st.session_state if k.startswith(('multi_conversions_', 'dbt_', 'sp_', 'table_', 'view_', 'udf_'))]
            for k in extra_keys:
                del st.session_state[k]
            reset_progress.progress(10, text="Clearing audit table...")

            try:
                session.sql(f"TRUNCATE TABLE IF EXISTS {TARGET_SCHEMA}.{CONVERSION_TABLE}").collect()
            except Exception:
                pass
            # HEAL_KNOWLEDGE_BASE is preserved across resets so the system
            # retains learned fix patterns for future conversions.
            reset_progress.progress(20, text="Dropping deployed objects...")

            try:
                current_db = session.get_current_database().replace('"', '')
                for schema_name in ['BRONZE', 'SILVER', 'GOLD']:
                    try:
                        session.sql(f"DROP SCHEMA IF EXISTS {current_db}.{schema_name} CASCADE").collect()
                    except Exception:
                        pass
            except Exception:
                pass
            reset_progress.progress(40, text="Dropping dbt tasks and project...")

            try:
                current_db = session.get_current_database().replace('"', '')
                dbt_schema = "DBT_PROJECTS"
                fq_schema = f"{current_db}.{dbt_schema}"
                for obj in [
                    f"DROP TASK IF EXISTS {fq_schema}.DBT_EXECUTION_TASK",
                    f"DROP TASK IF EXISTS {fq_schema}.DBT_DOCS_FETCH_TASK",
                    f"DROP PROCEDURE IF EXISTS {fq_schema}.SP_FETCH_DBT_DOCS(STRING, STRING)",
                    f"DROP PROCEDURE IF EXISTS {fq_schema}.SP_READ_STAGE_FILE(STRING, STRING)",
                ]:
                    try:
                        session.sql(obj).collect()
                    except Exception:
                        pass
                try:
                    all_projects = session.sql(f"SHOW DBT PROJECTS IN SCHEMA {fq_schema}").collect()
                    for proj in all_projects:
                        pname = proj.get("name", "")
                        if pname:
                            try:
                                session.sql(f"DROP DBT PROJECT IF EXISTS {fq_schema}.{pname}").collect()
                            except Exception:
                                pass
                except Exception:
                    pass
            except Exception:
                pass
            reset_progress.progress(60, text="Purging stage files...")

            try:
                current_db = session.get_current_database().replace('"', '')
                dbt_schema = "DBT_PROJECTS"
                for stage_name in ['DBT_PROJECT_STAGE', 'DBT_DOCS_STAGE']:
                    fq_stage = f"{current_db}.{dbt_schema}.{stage_name}"
                    try:
                        session.sql(f"DROP STAGE IF EXISTS {fq_stage}").collect()
                        session.sql(f"CREATE STAGE IF NOT EXISTS {fq_stage} COMMENT = 'Recreated by Reset'").collect()
                    except Exception:
                        pass
            except Exception:
                pass
            reset_progress.progress(80, text="Storing reset timestamp...")

            st.session_state['reset_timestamp'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
            reset_progress.progress(90, text="Clearing caches...")

            st.cache_data.clear()
            reset_progress.progress(100, text="Done!")
            reset_progress.empty()
            st.rerun()

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 9: ROADMAP (Coming Soon)
# ═══════════════════════════════════════════════════════════════════════════════
with tab9:
    st.markdown("""
    <div class="roadmap-header">
        <div class="roadmap-badge">✦ Coming Soon</div>
        <div class="roadmap-title">What's Next</div>
        <div class="roadmap-subtitle">Features on the horizon — expanding Cortex-Orchestrated Data Automation into a complete migration &amp; modernization platform.</div>
    </div>
    <div class="roadmap-grid">
        <div class="roadmap-card">
            <div><span class="roadmap-card-icon">📦</span><span class="roadmap-card-num">01</span></div>
            <div class="roadmap-card-title">SSIS Package Migration</div>
            <div class="roadmap-card-desc">
                Upload .dtsx packages and convert SSIS data flows, control flows, and connection managers
                into Snowflake Tasks, Streams, and Stored Procedures — fully automated with Cortex AI.
            </div>
            <div class="roadmap-card-tags">
                <span class="roadmap-tag">SSIS</span>
                <span class="roadmap-tag">Tasks</span>
                <span class="roadmap-tag">Streams</span>
                <span class="roadmap-tag">ETL</span>
            </div>
        </div>
        <div class="roadmap-card">
            <div><span class="roadmap-card-icon">🤖</span><span class="roadmap-card-num">02</span></div>
            <div class="roadmap-card-title">RAG Powered Chatbot</div>
            <div class="roadmap-card-desc">
                Ask natural language questions about your migration — &ldquo;What tables failed?&rdquo;,
                &ldquo;Show me the dbt lineage for DIM_ADDRESS&rdquo; — powered by Cortex AI with
                retrieval-augmented generation over your conversion history.
            </div>
            <div class="roadmap-card-tags">
                <span class="roadmap-tag">Cortex AI</span>
                <span class="roadmap-tag">RAG</span>
                <span class="roadmap-tag">NL Query</span>
            </div>
        </div>
        <div class="roadmap-card">
            <div><span class="roadmap-card-icon">🌐</span><span class="roadmap-card-num">03</span></div>
            <div class="roadmap-card-title">Multi-Platform Source Support</div>
            <div class="roadmap-card-desc">
                Extend conversion beyond SQL Server — support Oracle PL/SQL, PostgreSQL, MySQL, and
                Teradata as source dialects with platform-specific type mappings, function translations,
                and syntax rules.
            </div>
            <div class="roadmap-card-tags">
                <span class="roadmap-tag">Oracle</span>
                <span class="roadmap-tag">PostgreSQL</span>
                <span class="roadmap-tag">MySQL</span>
                <span class="roadmap-tag">Teradata</span>
            </div>
        </div>
        <div class="roadmap-card">
            <div><span class="roadmap-card-icon">🔗</span><span class="roadmap-card-num">04</span></div>
            <div class="roadmap-card-title">GitHub CI/CD Integration</div>
            <div class="roadmap-card-desc">
                Push converted dbt projects directly to a GitHub repo, auto-generate CI/CD pipelines
                with GitHub Actions for automated testing, deployment, and version-controlled
                migration workflows.
            </div>
            <div class="roadmap-card-tags">
                <span class="roadmap-tag">GitHub</span>
                <span class="roadmap-tag">CI/CD</span>
                <span class="roadmap-tag">Actions</span>
                <span class="roadmap-tag">GitOps</span>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
