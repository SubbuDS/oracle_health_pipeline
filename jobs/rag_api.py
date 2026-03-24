import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import psycopg2
from sentence_transformers import SentenceTransformer
from groq import Groq

DB_CONFIG = dict(host="localhost", port=5432,
                 database="ehr_db", user="demo", password="demo")

SYSTEM_PROMPT = """You are a clinical analyst with access to two data sources:

1. CLINICAL NOTES — free text written by doctors and nurses
2. STRUCTURED DATA — vital signs, lab results, NEWS2 risk scores

Answer questions by reasoning across BOTH sources together.
The notes give context. The numbers give precision.
Always reference specific patient IDs and specific values.
Be concise but clinically precise.
If data is insufficient to answer, say so clearly.
"""

class ClinicalRAG:
    def __init__(self):
        self.model      = None
        self.client     = None
        self.notes      = []
        self.embeddings = None

    def load(self, groq_api_key: str):
        print("Loading embedding model...")
        self.model  = SentenceTransformer("all-MiniLM-L6-v2")
        self.client = Groq(api_key=groq_api_key)
        self._load_and_embed_notes()
        print(f"RAG ready — {len(self.notes)} notes embedded")
        return self

    def _load_and_embed_notes(self):
        conn = psycopg2.connect(**DB_CONFIG)
        cur  = conn.cursor()
        cur.execute("""
            SELECT note_id, patient_id, note_type, authored_by, authored_at, note_text
            FROM clinical_notes ORDER BY note_id
        """)
        rows = cur.fetchall()
        conn.close()

        self.notes = [
            {
                "note_id":    r[0],
                "patient_id": r[1],
                "note_type":  r[2],
                "authored_by":r[3],
                "authored_at":str(r[4]),
                "text":       r[5]
            }
            for r in rows
        ]

        texts = [
            f"Patient {n['patient_id']} | {n['note_type']} by {n['authored_by']}: {n['text']}"
            for n in self.notes
        ]
        self.embeddings = self.model.encode(texts, show_progress_bar=False)

    def retrieve(self, question: str, top_k: int = 3) -> list:
        q_emb  = self.model.encode([question])
        norms  = np.linalg.norm(self.embeddings, axis=1, keepdims=True)
        normed = self.embeddings / (norms + 1e-10)
        q_norm = q_emb / (np.linalg.norm(q_emb) + 1e-10)
        scores = (normed @ q_norm.T).flatten()
        top    = np.argsort(scores)[::-1][:top_k]
        return [{"score": round(float(scores[i]), 3), **self.notes[i]} for i in top]

    def get_structured_context(self, spark, patient_ids: list) -> str:
        from utils.spark_session import GOLD_BASE, LOCAL_GOLD
        if not patient_ids:
            return ""
        id_list = ", ".join(str(p) for p in patient_ids)
        parts   = []
        for path in [f"{GOLD_BASE}/patient_summary", f"{LOCAL_GOLD}/patient_summary"]:
            try:
                rows = (spark.read.format("delta").load(path)
                            .filter(f"patient_id IN ({id_list})")
                            .collect())
                if rows:
                    parts.append("STRUCTURED DATA:")
                    for r in rows:
                        d = r.asDict()
                        parts.append(
                            f"  Patient {d.get('patient_id')}: "
                            f"age={d.get('age_years')} gender={d.get('gender')} "
                            f"NEWS2={d.get('latest_news2_risk')} "
                            f"SpO2={d.get('latest_spo2')}% "
                            f"HR={d.get('latest_hr')}bpm "
                            f"SBP={d.get('latest_sbp')}mmHg "
                            f"ICU={d.get('icu_admissions')} "
                            f"critical_labs={d.get('critical_labs_90d')}"
                        )
                break
            except Exception:
                continue
        for path in [f"{GOLD_BASE}/critical_alerts", f"{LOCAL_GOLD}/critical_alerts"]:
            try:
                rows = (spark.read.format("delta").load(path)
                            .filter(f"patient_id IN ({id_list})")
                            .collect())
                if rows:
                    parts.append("CRITICAL ALERTS:")
                    for r in rows:
                        d = r.asDict()
                        parts.append(
                            f"  Patient {d.get('patient_id')}: "
                            f"{d.get('test_name')} = {d.get('result_numeric')} "
                            f"{d.get('unit')} ({d.get('abnormal_direction')})"
                        )
                break
            except Exception:
                continue
        return "\n".join(parts)

    def answer(self, question: str, spark) -> dict:
        notes       = self.retrieve(question)
        patient_ids = list({n["patient_id"] for n in notes})
        structured  = self.get_structured_context(spark, patient_ids)

        notes_ctx = "\nCLINICAL NOTES (most relevant):\n"
        for n in notes:
            notes_ctx += (
                f"\n[Patient {n['patient_id']} | {n['note_type']} "
                f"by {n['authored_by']} | similarity={n['score']} "
                f"| {n['authored_at'][:16]}]\n{n['text']}\n"
            )

        context  = structured + "\n" + notes_ctx
        response = self.client.chat.completions.create(
            model="llama-3.1-8b-instant",
            max_tokens=800,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": f"Context:\n{context}\n\nQuestion: {question}"}
            ]
        )

        return {
            "answer":      response.choices[0].message.content,
            "notes_used":  [{"patient_id": n["patient_id"],
                             "note_type":  n["note_type"],
                             "authored_by":n["authored_by"],
                             "score":      n["score"],
                             "text":       n["text"][:200] + "..."}
                            for n in notes],
            "patient_ids": patient_ids
        }
