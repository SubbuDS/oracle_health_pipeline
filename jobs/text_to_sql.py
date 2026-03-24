import sys, os, argparse
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import yaml
from groq import Groq
from utils.spark_session import get_spark, GOLD_BASE, LOCAL_GOLD

# ── Load semantic layer from YAML ─────────────────────────────────────────────

def load_semantic_layer() -> dict:
    path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "config", "semantic_layer.yml"
    )
    with open(path) as f:
        return yaml.safe_load(f)


def build_schema_prompt(semantic: dict) -> str:
    """
    Convert the YAML semantic layer into a natural language prompt
    that the LLM can read and understand.
    """
    lines = []

    for table_name, table_def in semantic["tables"].items():
        lines.append(f"Table: {table_name}")
        lines.append(f"Description: {table_def['description']}")

        if table_def.get("notes"):
            for note in table_def["notes"]:
                lines.append(f"IMPORTANT: {note}")

        lines.append("Columns:")
        for col_name, col_desc in table_def["columns"].items():
            lines.append(f"  {col_name}: {col_desc}")
        lines.append("")

    lines.append("Relationships (use for JOINs):")
    for rel in semantic["relationships"]:
        lines.append(f"  {rel}")
    lines.append("")

    lines.append("Rules you must follow:")
    for i, rule in enumerate(semantic["sql_rules"], 1):
        lines.append(f"  {i}. {rule}")

    return "\n".join(lines)


def build_system_prompt(semantic: dict) -> str:
    schema_text = build_schema_prompt(semantic)
    return f"""You are a SQL expert for a healthcare EHR pipeline built on Apache Spark.
The user asks questions in plain English. You write Spark SQL queries.
Respond with ONLY the raw SQL. No explanation. No markdown. No backticks.

{schema_text}
"""


# ── Generate SQL ──────────────────────────────────────────────────────────────

def generate_sql(question: str, client, system_prompt: str) -> str:
    response = client.chat.completions.create(
        model="llama-3.1-8b-instant",
        max_tokens=600,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": question}
        ]
    )
    sql = response.choices[0].message.content.strip()
    sql = sql.replace("```sql", "").replace("```", "").strip()
    return sql


# ── Register Gold tables as Spark views ───────────────────────────────────────

def register_tables(spark, semantic: dict) -> list:
    registered = []
    for table in semantic["tables"].keys():
        # Try Gold Delta tables first
        for path in [f"{GOLD_BASE}/{table}", f"{LOCAL_GOLD}/{table}"]:
            try:
                df = spark.read.format("delta").load(path)
                df.createOrReplaceTempView(table)
                print(f"  registered: {table} ({df.count()} rows)")
                registered.append(table)
                break
            except Exception:
                continue

        # clinical_notes lives in PostgreSQL not Delta
        if table not in registered and table == "clinical_notes":
            try:
                df = (spark.read.format("jdbc")
                    .option("url", "jdbc:postgresql://localhost:5432/ehr_db")
                    .option("dbtable", "clinical_notes")
                    .option("user", "demo")
                    .option("password", "demo")
                    .option("driver", "org.postgresql.Driver")
                    .load())
                df.createOrReplaceTempView("clinical_notes")
                print(f"  registered: clinical_notes via JDBC ({df.count()} rows)")
                registered.append("clinical_notes")
            except Exception as e:
                print(f"  skipped: clinical_notes ({e})")

    return registered


# ── Display results ───────────────────────────────────────────────────────────

def display(question: str, sql: str, df):
    count = df.count()
    print(f"\n{'='*65}")
    print(f"Question : {question}")
    print(f"SQL      :\n{sql}")
    print(f"Rows     : {count}")
    print(f"{'='*65}")
    if count > 0:
        df.show(20, truncate=False)
    else:
        print("(no results — try rephrasing)")


# ── Interactive REPL ──────────────────────────────────────────────────────────

def interactive(spark, client, system_prompt: str):
    print("""
 EHR Text-to-SQL | Groq + Llama 3.1 | semantic_layer.yml
══════════════════════════════════════════════════════════
 Try these:
   show me all ICU patients
   which patients have critical labs?
   how many admissions per ward this week?
   show high risk patients with SpO2 below 92
   count patients by news2 risk level
   which patients have respiratory issues in their notes?
   which doctor wrote the most clinical notes?
══════════════════════════════════════════════════════════
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
        try:
            print("  Generating SQL...")
            sql = generate_sql(question, client, system_prompt)
            print("  Running query...")
            df  = spark.sql(sql)
            display(question, sql, df)
        except Exception as e:
            print(f"  Error: {e}")
            print("  Try rephrasing the question.")


# ── Main ──────────────────────────────────────────────────────────────────────

def run(question: str = None):
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        print("Set your API key: export GROQ_API_KEY=gsk_...")
        sys.exit(1)

    # Load semantic layer from YAML
    print("Loading semantic layer from config/semantic_layer.yml...")
    semantic      = load_semantic_layer()
    system_prompt = build_system_prompt(semantic)

    client = Groq(api_key=api_key)
    print("Using Groq — llama-3.1-8b-instant\n")

    spark = get_spark("EHR_TextToSQL", enable_minio=True, enable_delta=True)

    print("Registering tables...")
    register_tables(spark, semantic)

    try:
        if question:
            sql = generate_sql(question, client, system_prompt)
            df  = spark.sql(sql)
            display(question, sql, df)
        else:
            interactive(spark, client, system_prompt)
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--question", "-q", default=None)
    args = parser.parse_args()
    run(args.question)
