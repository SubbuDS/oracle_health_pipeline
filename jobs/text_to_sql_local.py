
"""
jobs/text_to_sql_local.py
───────────────────────────────────────────────────────────────────────────────
Same as text_to_sql.py but uses a local Ollama LLM instead of Claude API.

Nothing leaves your machine. No API key needed. No cost per query.
Schema is never sent to any third party.

Run:
  ollama serve                                      # start Ollama first
  python jobs/text_to_sql_local.py                  # interactive mode
  python jobs/text_to_sql_local.py -q "show ICU patients"
"""

import sys, os, argparse
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import ollama
from utils.spark_session import get_spark, GOLD_BASE, LOCAL_GOLD

# ── Config — change model here ────────────────────────────────────────────────
MODEL = os.getenv("OLLAMA_MODEL", "llama3.2")   # override with env var if needed

# ── Schema — identical to Claude version ─────────────────────────────────────
# The schema is the same. The only difference is it never leaves your Mac.

SCHEMAS = """
Table: patient_summary
Description: One row per patient. Latest vitals, risk scores, encounter counts.
Columns:
  patient_id          INT         -- unique patient identifier
  gender              STRING      -- M or F
  age_years           LONG        -- patient age in years
  age_group           STRING      -- SENIOR or PEDIATRIC
  latest_sbp          DOUBLE      -- latest systolic blood pressure mmHg
  latest_dbp          DOUBLE      -- latest diastolic blood pressure mmHg
  latest_hr           DOUBLE      -- latest heart rate beats per minute
  latest_spo2         DOUBLE      -- latest oxygen saturation percentage
  latest_temp_f       DOUBLE      -- latest temperature fahrenheit
  latest_news2_risk   STRING      -- early warning score: HIGH / MEDIUM / LOW
  icu_admissions      LONG        -- number of ICU admissions
  critical_labs_90d   LONG        -- critical lab results in last 90 days
  total_encounters    LONG        -- total number of encounters
  dq_complete         BOOLEAN     -- true if patient data is complete

Table: critical_alerts
Description: One row per critical lab result in the last 7 days.
Columns:
  result_id           INT         -- unique result identifier
  patient_id          INT         -- links to patient_summary.patient_id
  test_name           STRING      -- lab test name e.g. Hemoglobin, Creatinine
  result_numeric      DOUBLE      -- numeric result value
  unit                STRING      -- unit e.g. g/dL, umol/L
  abnormal_direction  STRING      -- HIGH or LOW
  result_time         TIMESTAMP   -- when the result was recorded

Table: encounter_metrics
Description: One row per patient encounter with aggregated vitals and risk.
Columns:
  patient_id          INT         -- links to patient_summary.patient_id
  event_type          STRING      -- ADMIT / DISCHARGE / TRANSFER
  event_time          TIMESTAMP   -- when the encounter occurred
  ward                STRING      -- ward name e.g. ICU, Cardiology, General
  is_icu              BOOLEAN     -- true if ICU encounter
  news2_risk          STRING      -- HIGH / MEDIUM / LOW
  min_spo2            DOUBLE      -- minimum SpO2 during encounter
  max_hr              DOUBLE      -- maximum heart rate during encounter

Table: daily_census
Description: Daily event counts by ward. Use for trend analysis.
Columns:
  event_date          DATE        -- the date
  ward                STRING      -- ward name
  event_type          STRING      -- ADMIT / DISCHARGE / TRANSFER
  event_count         LONG        -- number of events that day

Table: clinical_notes
Description: Free text notes written by doctors and nurses.
Columns:
  note_id             INT         -- unique note identifier
  patient_id          INT         -- links to patient_summary.patient_id
  note_type           STRING      -- ADMISSION / PROGRESS / NURSING / DISCHARGE
  authored_by         STRING      -- doctor or nurse name
  authored_at         TIMESTAMP   -- when note was written
  note_text           STRING      -- full text of the clinical note

Relationships:
  critical_alerts.patient_id   → patient_summary.patient_id
  encounter_metrics.patient_id → patient_summary.patient_id
  clinical_notes.patient_id    → patient_summary.patient_id
"""

SYSTEM_PROMPT = f"""You are a SQL expert for a healthcare EHR pipeline built on Apache Spark.
The user asks questions in plain English. You write Spark SQL queries.

IMPORTANT: Respond with ONLY the raw SQL query.
No explanation. No markdown. No backticks. No commentary. Just the SQL.

{SCHEMAS}

Rules:
  1. Use ONLY the tables and columns listed above
  2. For "this week"   → WHERE DATE(col) >= DATE_SUB(CURRENT_DATE(), 7)
  3. For "today"       → WHERE DATE(col) = CURRENT_DATE()
  4. Always add LIMIT 100 unless the question asks for counts or aggregations
  5. news2_risk values must be uppercase: HIGH, MEDIUM, LOW
  6. gender values: M or F
  7. event_type values: ADMIT, DISCHARGE, TRANSFER
  8. abnormal_direction values: HIGH or LOW
  9. For text search use: LOWER(note_text) LIKE \'%keyword%\'
  10. When question involves notes, JOIN clinical_notes ON patient_id
  11. Return ONLY the SQL — nothing else at all
"""


# ── Generate SQL using local Ollama ───────────────────────────────────────────

def generate_sql(question: str) -> str:
    """
    Send the question + schema to the local Ollama model.
    The schema never leaves your machine.
    Returns a clean SQL string.
    """
    response = ollama.chat(
        model=MODEL,
        messages=[
            {
                "role":    "system",
                "content": SYSTEM_PROMPT
            },
            {
                "role":    "user",
                "content": f"Write a Spark SQL query for: {question}"
            }
        ]
    )

    sql = response["message"]["content"].strip()

    # Local models sometimes ignore formatting instructions
    # Strip markdown more aggressively than with Claude
    lines = sql.split("\n")
    clean_lines = []
    in_sql = False

    for line in lines:
        # Skip markdown fences
        if line.strip().startswith("```"):
            in_sql = not in_sql
            continue
        # Skip explanatory lines (local models often add these)
        if any(line.lower().startswith(prefix) for prefix in
               ["here", "this query", "the query", "note:", "explanation",
                "this sql", "i\'ll", "this will", "the following"]):
            continue
        clean_lines.append(line)

    sql = "\n".join(clean_lines).strip()

    # If model returned nothing useful, raise early
    if not sql or len(sql) < 10:
        raise ValueError(f"Model returned empty or invalid SQL: {repr(sql)}")

    return sql


# ── Register Gold tables as Spark views ───────────────────────────────────────

def register_tables(spark) -> list:
    tables = [
        "patient_summary",
        "critical_alerts",
        "encounter_metrics",
        "daily_census",
    ]

    registered = []
    for table in tables:
        for path in [f"{GOLD_BASE}/{table}", f"{LOCAL_GOLD}/{table}"]:
            try:
                df = spark.read.format("delta").load(path)
                df.createOrReplaceTempView(table)
                print(f"  registered: {table} ({df.count()} rows)")
                registered.append(table)
                break
            except Exception:
                continue

    # clinical_notes via JDBC if it exists
    try:
        df = (spark.read
            .format("jdbc")
            .option("url", "jdbc:postgresql://localhost:5432/ehr_db")
            .option("dbtable", "clinical_notes")
            .option("user", "demo")
            .option("password", "demo")
            .option("driver", "org.postgresql.Driver")
            .load())
        df.createOrReplaceTempView("clinical_notes")
        print(f"  registered: clinical_notes via JDBC ({df.count()} rows)")
        registered.append("clinical_notes")
    except Exception:
        pass

    return registered


# ── Run SQL ───────────────────────────────────────────────────────────────────

def run_query(spark, sql: str):
    return spark.sql(sql)


def display(question: str, sql: str, df):
    count = df.count()
    print(f"""
{'='*65}
 Question : {question}
 Model    : {MODEL} (local — no data sent anywhere)
 SQL      :
{sql}
 Rows     : {count}
{'='*65}""")
    if count > 0:
        df.show(20, truncate=False)
    else:
        print(" (no results — try rephrasing)")


# ── Compare mode — run same question on local + Claude ───────────────────────

def compare_mode(question: str, spark):
    """
    Run the same question on local Ollama AND Claude API.
    Shows side by side which SQL each model generates.
    Useful for evaluating local model quality vs Claude.
    """
    print(f"\nComparing outputs for: {question}")
    print("=" * 65)

    # Local model
    print(f"\n[LOCAL: {MODEL}]")
    try:
        local_sql = generate_sql(question)
        print(local_sql)
        local_df  = run_query(spark, local_sql)
        print(f"Rows: {local_df.count()}")
    except Exception as e:
        print(f"Error: {e}")

    # Claude (if API key set)
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if api_key:
        print(f"\n[CLAUDE: claude-sonnet-4-6]")
        try:
            import anthropic
            client   = anthropic.Anthropic(api_key=api_key)
            response = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=600,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": question}]
            )
            claude_sql = response.content[0].text.strip()
            claude_sql = claude_sql.replace("```sql","").replace("```","").strip()
            print(claude_sql)
            claude_df  = run_query(spark, claude_sql)
            print(f"Rows: {claude_df.count()}")
        except Exception as e:
            print(f"Error: {e}")
    else:
        print("\n[CLAUDE] skipped — ANTHROPIC_API_KEY not set")


# ── Interactive REPL ──────────────────────────────────────────────────────────

def interactive(spark):
    print(f"""
 EHR Text-to-SQL (LOCAL)  |  model={MODEL}  |  no data leaves your Mac
═══════════════════════════════════════════════════════════════
 Try these:
   show me all ICU patients
   which patients have critical labs?
   how many admissions per ward this week?
   show high risk patients with SpO2 below 92
   count patients by news2 risk level
   compare <question>   → run same question on local + Claude
═══════════════════════════════════════════════════════════════
 Type exit to quit
""")

    while True:
        try:
            question = input("Ask: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye.")
            break

        if question.lower() in ("exit", "quit", "q"):
            break
        if not question:
            continue

        # Compare mode
        if question.lower().startswith("compare "):
            actual_q = question[8:].strip()
            compare_mode(actual_q, spark)
            continue

        try:
            print(f"  Generating SQL locally ({MODEL})...")
            sql = generate_sql(question)

            print("  Running query against Gold tables...")
            df  = run_query(spark, sql)

            display(question, sql, df)

        except Exception as e:
            print(f"  Error: {e}")
            print("  Try rephrasing — or run: compare <question> to see Claude vs local")


# ── Main ──────────────────────────────────────────────────────────────────────

def run(question: str = None, compare: bool = False):
    # Check Ollama is running
    try:
        models = ollama.list()
        available = [m["name"] for m in models.get("models", [])]
        if not any(MODEL in m for m in available):
            print(f"Model {MODEL!r} not found. Pull it first:")
            print(f"  ollama pull {MODEL}")
            print(f"Available models: {available}")
            sys.exit(1)
        print(f"Using local model: {MODEL}")
    except Exception:
        print("Ollama is not running. Start it with:")
        print("  ollama serve")
        sys.exit(1)

    print("Starting Spark session...")
    spark = get_spark("EHR_TextToSQL_Local", enable_minio=True, enable_delta=True)

    print("\nRegistering Gold tables...")
    registered = register_tables(spark)
    print(f"\nReady — {len(registered)} tables | model={MODEL} | fully local\n")

    try:
        if question and compare:
            compare_mode(question, spark)
        elif question:
            sql = generate_sql(question)
            df  = run_query(spark, sql)
            display(question, sql, df)
        else:
            interactive(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Local Text-to-SQL — no data leaves your machine"
    )
    parser.add_argument("--question", "-q", default=None)
    parser.add_argument("--model",    "-m", default=None,
                        help="Ollama model to use (default: llama3.2)")
    parser.add_argument("--compare",  "-c", action="store_true",
                        help="Compare local model output vs Claude API")
    args = parser.parse_args()

    if args.model:
        MODEL = args.model

    run(question=args.question, compare=args.compare)
