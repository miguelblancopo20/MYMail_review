from __future__ import annotations

import base64
import csv
import html as html_escape
import json
import os
import random
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

EXCEL_PATH = Path("Validados_V3.xlsx")
SHEET_NAME = "1 dic - 8 dic"
REVIEW_CSV = Path("revisiones.csv")
SKIP_CSV = Path("descartes.csv")

BASE_FIELDS: List[str] = [
    "@timestamp",
    "Validado",
    "Motivo",
    "Comentario",
    "Documento",
    "MatriculaAsesor",
    "PageName",
    "IdCorreo",
    "Automatismo",
    "Segmento",
    "Location",
    "Sublocation",
    "Subject",
    "Question",
    "MailToAgent",
    "Faltan datos?",
    "Comentario revisión",
]

REVIEW_FIELDS: List[str] = BASE_FIELDS + [
    "Estado revisión",
    "Nota de revisión",
    "Nota interna",
    "Fecha de revisión",
]


def format_timestamp(value: str) -> str:
    if not value:
        return ""
    try:
        dt = pd.to_datetime(value, errors="coerce")
        if pd.isna(dt):
            return str(value)
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(value)


def ensure_skip_csv() -> None:
    if SKIP_CSV.exists():
        return
    with SKIP_CSV.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=BASE_FIELDS + ["Fecha descartado"]) 
        writer.writeheader()


def append_skip(record: Dict[str, str]) -> None:
    ensure_skip_csv()
    row = {k: record.get(k, "") for k in BASE_FIELDS}
    row["Fecha descartado"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    with SKIP_CSV.open("a", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=BASE_FIELDS + ["Fecha descartado"]) 
        writer.writerow(row)


def load_dataset() -> pd.DataFrame:
    df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME, dtype=str)
    return df.fillna("")


def ensure_review_csv() -> None:
    if REVIEW_CSV.exists():
        return
    with REVIEW_CSV.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=REVIEW_FIELDS)
        writer.writeheader()


def append_review(record: Dict[str, str], review_status: str, reviewer_note: str, internal_note: str) -> None:
    ensure_review_csv()
    payload = {key: record.get(key, "") for key in BASE_FIELDS}
    payload.update(
        {
            "Estado revisión": review_status,
            "Nota de revisión": reviewer_note,
            "Nota interna": internal_note,
            "Fecha de revisión": datetime.now(timezone.utc).isoformat(),
        }
    )
    with REVIEW_CSV.open("a", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=REVIEW_FIELDS)
        writer.writerow(payload)


def persist_excel(df: pd.DataFrame) -> None:
    fd, tmp_path = tempfile.mkstemp(suffix=".xlsx")
    os.close(fd)
    try:
        df.to_excel(tmp_path, sheet_name=SHEET_NAME, index=False)
        Path(tmp_path).replace(EXCEL_PATH)
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass


def next_queue_record(df: pd.DataFrame) -> Dict[str, str]:
    if "queue" not in st.session_state or not st.session_state.queue:
        st.session_state.queue = list(df.index)
        random.shuffle(st.session_state.queue)
    # Pop until we find a valid index
    while st.session_state.queue:
        idx = st.session_state.queue.pop()
        if idx in df.index:
            st.session_state.current_idx = idx
            return df.loc[idx].to_dict()
    return {}


def skip_current() -> None:
    if "current_idx" in st.session_state:
        st.session_state.queue.insert(0, st.session_state.current_idx)
    st.session_state.current_idx = None
    st.session_state.current = None


def layout_sidebar(df: pd.DataFrame) -> None:
    pending = len(st.session_state.get("queue", [])) if "queue" in st.session_state else len(df.index)
    st.sidebar.metric("Pendientes", pending)


def layout_record(record: Dict[str, str]) -> None:
    st.subheader("Acciones Agente")

    # Segunda fila: Validado por agente | Motivo | Comentario
    second_cols = st.columns([1, 1, 1])
    with second_cols[0]:
        v = html_escape.escape(record.get("Validado", ""))
        components.html(f"""
        <div class='mymail-field'>
            <div class='field'><strong>Validado por agente:</strong> {v}</div>
        </div>
        """, height=48)
    with second_cols[1]:
        v = html_escape.escape(record.get("Motivo", ""))
        components.html(f"""
        <div class='mymail-field'>
            <div class='field'><strong>Motivo:</strong> {v}</div>
        </div>
        """, height=48)
    with second_cols[2]:
        v = html_escape.escape(record.get("Comentario", ""))
        components.html(f"""
        <div class='mymail-field'>
            <div class='field'><strong>Comentario:</strong> {v}</div>
        </div>
        """, height=48)

    st.markdown("---")
    st.subheader("Datos del correo")
    subj = html_escape.escape(record.get("Subject", ""))
    components.html(f"""
    <div class='mymail-field'>
        <div class='field'><strong>Asunto:</strong> {subj}</div>
    </div>
    """, height=48)
    # Normalizar saltos de línea
    question_text = record.get("Question", "") or ""
    question_text = question_text.replace('\r\n', '\n').replace('\r', '\n')
    question_text = re.sub(r"\n\s*\n+", "\n", question_text)

    q_key_suffix = str(st.session_state.get("current_idx", "none"))
    # Mostrar siempre el correo completo en contenedor con scroll (slider)
    height = 260
    safe_q = html_escape.escape(question_text)
    ta_html = f"""
    <div class='mymail-field'>
        <div class='field'><strong>Correo completo</strong>
            <div style="height:{height}px; overflow:auto; margin-top:8px; padding-right:8px;">
                <pre style="white-space:pre-wrap;margin:0;font-family: Arial, sans-serif;font-size:14px;">{safe_q}</pre>
            </div>
        </div>
    </div>
    """
    components.html(ta_html, height=height + 30)
    st.download_button("Descargar correo (.txt)", question_text, file_name=f"correo_{q_key_suffix}.txt", mime="text/plain", key=f"q_dl_{q_key_suffix}")

    mail_text = record.get("MailToAgent", "") or ""
    m_key_suffix = q_key_suffix
    with st.expander("Resumen IA", expanded=False):
        try:
            mail_obj = json.loads(mail_text)
            if isinstance(mail_obj, dict):
                for key, val in mail_obj.items():
                    if isinstance(val, (dict, list)):
                        pretty_val = json.dumps(val, ensure_ascii=False, indent=2)
                        safe_val = html_escape.escape(pretty_val)
                        ta_html = f"""
                        <div class='mymail-field'>
                            <label><strong>{html_escape.escape(str(key))}</strong></label>
                            <div class='field'>
                                <pre style="white-space:pre-wrap;margin:8px 0 0 0;font-family: Arial, sans-serif;font-size:14px;">{safe_val}</pre>
                            </div>
                        </div>
                        """
                        components.html(ta_html, height=150)
                    else:
                        safe_val = html_escape.escape(str(val))
                        inp_html = f"""
                        <div class='mymail-field'>
                            <div class='field'><strong>{html_escape.escape(str(key))}:</strong> {safe_val}</div>
                        </div>
                        """
                        components.html(inp_html, height=48)
                pretty = json.dumps(mail_obj, ensure_ascii=False, indent=2)
                st.download_button("Descargar resumen IA (.json)", pretty, file_name=f"mailtoagent_{m_key_suffix}.json", mime="application/json", key=f"mail_dl_{m_key_suffix}")
                faltan = html_escape.escape(record.get("Faltan datos?", ""))
                components.html(f"""
                <div class='mymail-field'>
                    <div class='field'><strong>¿Faltan datos?</strong> {faltan}</div>
                </div>
                """, height=48)
            else:
                st.text_area("", mail_text, height=180, key=f"mail_area_{m_key_suffix}")
                st.download_button("Descargar resumen IA (.txt)", mail_text, file_name=f"mailtoagent_{m_key_suffix}.txt", mime="text/plain", key=f"mail_dl_txt_{m_key_suffix}")
                faltan = html_escape.escape(record.get("Faltan datos?", ""))
                components.html(f"""
                <div class='mymail-field'>
                    <div class='field'><strong>¿Faltan datos?</strong> {faltan}</div>
                </div>
                """, height=48)
        except Exception:
            st.text_area("", mail_text, height=180, key=f"mail_area_{m_key_suffix}")
            st.download_button("Descargar resumen IA (.txt)", mail_text, file_name=f"mailtoagent_{m_key_suffix}.txt", mime="text/plain", key=f"mail_dl_txt_{m_key_suffix}")
            faltan = html_escape.escape(record.get("Faltan datos?", ""))
            components.html(f"""
            <div class='mymail-field'>
                <div class='field'><strong>¿Faltan datos?</strong> {faltan}</div>
            </div>
            """, height=48)


def review_form(record: Dict[str, str]):
    st.subheader("Resultado de revisión")
    with st.form("review_form", clear_on_submit=True):
        status = st.selectbox(
            "Estado final",
            ["OK", "KO MYM", "KO AGENTE", "DUDA"],
        )
        reviewer_note = st.text_input(
            "Comentario de revisión",
            placeholder="Describe por qué la etiqueta es correcta o qué ajuste necesita.",
        )
        internal_note = st.text_input(
            "Nota interna (opcional)",
            placeholder="Observaciones operativas o pasos siguientes.",
        )
        col1, col2 = st.columns([3, 1])
        submitted = col1.form_submit_button("Guardar revisión y pasar al siguiente")
        skip = col2.form_submit_button("Saltar sin guardar", type="secondary")
    return submitted, skip, status, reviewer_note, internal_note


def inject_styles() -> None:
    css = """
<style>
body{margin:0;}
.with-mymail-header .block-container{padding-top:140px !important;}
.mymail-header{position:fixed;top:0;left:0;right:0;z-index:120;display:flex;align-items:center;gap:12px;margin-bottom:12px;padding:12px 24px;background:var(--background-color,#fff);box-shadow:0 2px 8px rgba(0,0,0,0.12)}
.mymail-header .logo{height:40px;margin-right:8px}
.mymail-header .title{font-size:22px;font-weight:700;margin-right:12px}
.mymail-header .top-fields{margin-left:8px;display:flex;flex-wrap:wrap;gap:12px}
.mymail-header .field{font-size:13px;padding:4px 10px;border-radius:6px;background:#f5f7fb}
.mymail-field .field{font-size:13px;padding:2px 8px;border-radius:6px;background:#f5f7fb}
.mymail-overlay{display:none;position:fixed;inset:0;align-items:center;justify-content:center;z-index:9999;background:rgba(0,0,0,0.45);flex-direction:column}
.mymail-spinner{width:48px;height:48px;border-radius:50%;border:6px solid rgba(255,255,255,0.2);border-top-color:white;animation:mymail-spin 1s linear infinite;margin-bottom:8px}
@keyframes mymail-spin{to{transform:rotate(360deg)}}
.mymail-overlay-text{color:white;font-size:16px}
body.mymail-locked{pointer-events:none;user-select:none;}
body.mymail-locked #mymail-overlay{pointer-events:all}

/* Estilo para campos en modo solo lectura */
.mymail-readonly,
input[readonly], textarea[readonly], input[disabled], select[disabled] {
  background: #f5f7fb !important;
  padding: 2px 8px !important;
  border-radius: 6px !important;
  font-size: 13px !important;
  border: 1px solid transparent !important;
  box-shadow: none !important;
}
</style>

<script>
(function(){
  const injectedCss = `
    .mymail-readonly, input[readonly], textarea[readonly], input[disabled], select[disabled] {
      background: #f5f7fb !important;
      padding: 2px 8px !important;
      border-radius: 6px !important;
      font-size: 13px !important;
      border: 1px solid transparent !important;
      box-shadow: none !important;
    }
  `;

  function applyToDocument(doc){
    try{
      if(!doc) return;
      if(doc.getElementById('mymail-injected-styles')) return;
      const s = doc.createElement('style');
      s.id = 'mymail-injected-styles';
      s.innerHTML = injectedCss;
      (doc.head || doc.documentElement).appendChild(s);
    }catch(e){}
  }

  // Apply to main document
  applyToDocument(document);

  // Apply to iframes (components.html creates iframes)
  function applyToIframes(){
    document.querySelectorAll('iframe').forEach(iframe=>{
      try{
        const idoc = iframe.contentDocument || iframe.contentWindow.document;
        applyToDocument(idoc);
      }catch(e){}
    });
  }

  applyToIframes();
  const mo = new MutationObserver(()=>applyToIframes());
  mo.observe(document.body, {childList:true, subtree:true});
})();
</script>
"""
    try:
        st.markdown(css, unsafe_allow_html=True)
    except Exception:
        pass


def inject_overlay() -> None:
    # Try common locations for the overlay HTML (app bundle or static folder)
    candidates = [Path("app/static/js/overlay.html"), Path("static/js/overlay.html"), Path("app/static/js/overlay.html")]
    content = None
    for p in candidates:
        try:
            if p.exists():
                content = p.read_text(encoding="utf-8")
                break
        except Exception:
            continue
    if content:
        try:
            components.html(content, height=1)
        except Exception:
            try:
                st.components.v1.html(content, height=1)
            except Exception:
                pass
