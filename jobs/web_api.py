import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import yaml
from groq import Groq
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from contextlib import asynccontextmanager
from utils.spark_session import get_spark, GOLD_BASE, LOCAL_GOLD

spark  = None
client = None
system_prompt = None

def load_semantic_layer():
    path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "config", "semantic_layer.yml"
    )
    with open(path) as f:
        return yaml.safe_load(f)

def build_system_prompt(semantic):
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
    schema_text = "\n".join(lines)
    return f"""You are a SQL expert for a healthcare EHR pipeline built on Apache Spark.
Respond with ONLY the raw SQL. No explanation. No markdown. No backticks.
{schema_text}
"""

def register_tables(spark, semantic):
    for table in semantic["tables"].keys():
        for path in [f"{GOLD_BASE}/{table}", f"{LOCAL_GOLD}/{table}"]:
            try:
                df = spark.read.format("delta").load(path)
                df.createOrReplaceTempView(table)
                break
            except Exception:
                continue
    try:
        df = (spark.read.format("jdbc")
            .option("url", "jdbc:postgresql://localhost:5432/ehr_db")
            .option("dbtable", "clinical_notes")
            .option("user", "demo").option("password", "demo")
            .option("driver", "org.postgresql.Driver").load())
        df.createOrReplaceTempView("clinical_notes")
    except Exception:
        pass

@asynccontextmanager
async def lifespan(app: FastAPI):
    global spark, client, system_prompt
    api_key = os.getenv("GROQ_API_KEY")
    client  = Groq(api_key=api_key)
    semantic      = load_semantic_layer()
    system_prompt = build_system_prompt(semantic)
    spark  = get_spark("EHR_WebAPI", enable_minio=True, enable_delta=True)
    register_tables(spark, semantic)
    global rag
    rag = ClinicalRAG().load(os.getenv("GROQ_API_KEY"))
    print("Ready at http://localhost:8000")
    yield
    spark.stop()

app = FastAPI(lifespan=lifespan)

static_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "static")
os.makedirs(static_dir, exist_ok=True)
app.mount("/static", StaticFiles(directory=static_dir), name="static")

from jobs.rag_api import ClinicalRAG

rag = None

class Question(BaseModel):
    question: str

@app.get("/")
def root():
    return FileResponse(os.path.join(static_dir, "index.html"))

@app.post("/ask")
def ask(body: Question):
    try:
        response = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            max_tokens=600,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": body.question}
            ]
        )
        sql = response.choices[0].message.content.strip()
        sql = sql.replace("```sql","").replace("```","").strip()

        df      = spark.sql(sql)
        columns = df.columns
        rows    = [[str(v) if v is not None else "—" for v in row] for row in df.limit(100).collect()]

        return {
            "success": True,
            "sql":     sql,
            "columns": columns,
            "rows":    rows,
            "count":   len(rows)
        }
    except Exception as e:
        return {"success": False, "error": str(e), "sql": ""}

@app.post("/rag")
def rag_ask(body: Question):
    try:
        result = rag.answer(body.question, spark)
        return {"success": True, **result}
    except Exception as e:
        return {"success": False, "error": str(e), "answer": ""}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
