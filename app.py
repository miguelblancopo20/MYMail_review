"""Streamlit UI para revisar los correos de Mayordomo Mail."""
from __future__ import annotations

import csv
import random
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
import re
import html as html_escape
import json
import base64


def inject_styles() -> None:
    """Inyecta estilos CSS para mejorar contraste y legibilidad."""
    st.markdown(
        """
        <style>
        /* Fuente y fondo global */
        :root { --mymail-font: 'Inter', 'Segoe UI', Arial, sans-serif; }
        html, body, [class^="css"], .stApp {
            font-family: var(--mymail-font) !important;
            background-color: #f6f7f9 !important;
            color: #0b1724 !important;
            font-size: 14px;
        }

        /* Ajuste de la columna principal para dejar espacio al header sticky */
        .stApp > .main .block-container {
            padding-top: 120px;
        }

        /* Entradas, textareas y selects */
        input[type='text'], textarea, select {
            background-color: #ffffff !important;
            color: #0b1724 !important;
            border: 1px solid #d1d5db !important;
            border-radius: 6px !important;
            padding: 6px 8px !important;
            font-family: var(--mymail-font) !important;
            font-size: 14px !important;
        }

        /* Botones */
        .stButton>button, button[role='button'] {
            background-color: #0b5fff !important;
            color: #ffffff !important;
            border: none !important;
            border-radius: 6px !important;
            padding: 8px 12px !important;
            font-family: var(--mymail-font) !important;
        }

        /* Métricas y textos */
        .stMetric, .stMetric > div { color: #0b1724 !important; font-family: var(--mymail-font) !important; }
        h1, h2, h3, .stCaption, label { color: #0b1724 !important; font-family: var(--mymail-font) !important; }
        label { font-size:14px; }

        /* Cabecera: logo y título */
        .mymail-header {
            display:flex;
            align-items:center;
            gap:12px;
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            background-color: #f6f7f9;
            z-index: 9999;
            padding: 12px 16px;
            border-bottom: 1px solid rgba(0,0,0,0.06);
        }
        .mymail-header .logo { height:56px; width:auto; }
        .mymail-header .title { margin:0; padding:0; font-size:28px; font-weight:700; color:#0b1724; }
        .mymail-header .top-fields { margin-left: 24px; display:flex; gap:12px; align-items:center; flex-wrap: wrap; }
        .mymail-header .top-fields .field { font-size:14px; color:#0b1724; background:#fff; padding:6px 8px; border-radius:6px; border:1px solid #e2e8f0; }

        /* Overlay de carga infinito */
        .mymail-overlay {
            position: fixed;
            inset: 0;
            background: rgba(255, 255, 255, 0.72);
            z-index: 12000;
            display: none;
            align-items: center;
            justify-content: center;
            flex-direction: column;
            gap: 12px;
        }
        .mymail-spinner {
            width: 54px;
            height: 54px;
            border: 6px solid #dbeafe;
            border-top-color: #0b5fff;
            border-radius: 50%;
            animation: mymail-spin 1s linear infinite;
        }
        @keyframes mymail-spin { to { transform: rotate(360deg); } }
        .mymail-overlay-text { font-size: 16px; font-weight: 600; color: #0b1724; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def inject_overlay() -> None:
    """Inserta overlay de carga y JS para bloquear acciones durante envíos."""
    st.markdown(
        """
        <div id="mymail-overlay" class="mymail-overlay">
            <div class="mymail-spinner"></div>
            <div class="mymail-overlay-text">Procesando...</div>
        </div>
        <script>
        (function() {
            const TARGET_TEXTS = ["Guardar revisión y pasar al siguiente", "Saltar sin guardar"];
            const showOverlay = () => {
                const overlay = document.getElementById('mymail-overlay');
                if (overlay) {
                    overlay.style.display = 'flex';
                }
            };

            const bindButtons = () => {
                const buttons = Array.from(document.querySelectorAll('button'))
                    .filter(btn => TARGET_TEXTS.includes(btn.innerText.trim()));
                buttons.forEach(btn => {
                    if (btn.dataset.mymailBound === '1') return;
                    btn.dataset.mymailBound = '1';
                    btn.addEventListener('click', () => {
                        showOverlay();
                        buttons.forEach(b => b.disabled = true);
                    });
                });
            };

            const observer = new MutationObserver(bindButtons);
            observer.observe(document.body, { childList: true, subtree: true });
            bindButtons();
        })();
        </script>
        """,
        unsafe_allow_html=True,
    )


def format_timestamp(value: str) -> str:
    """Try to parse a timestamp string and return formatted 'YYYY-mm-dd HH:MM'."""
    if not value:
        return ""
    try:
        dt = pd.to_datetime(value, errors="coerce")
        if pd.isna(dt):
            return str(value)
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(value)

EXCEL_PATH = Path("Validados_V3.xlsx")
SHEET_NAME = "1 dic - 8 dic"
REVIEW_CSV = Path("revisiones.csv")

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

SKIP_CSV = Path("descartes.csv")


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
    """Lee el Excel como DataFrame, devolviendo strings y sin nulos."""
    df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME, dtype=str)
    df = df.fillna("")
    return df


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
    """Guarda el DataFrame en el Excel original usando un archivo temporal."""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        df.to_excel(tmp.name, sheet_name=SHEET_NAME, index=False)
    Path(tmp.name).replace(EXCEL_PATH)


def next_queue_record(df: pd.DataFrame) -> Dict[str, str]:
    """Devuelve el siguiente registro (dict) y persiste el índice en sesión."""
    if "queue" not in st.session_state or not st.session_state.queue:
        st.session_state.queue = list(df.index)
        random.shuffle(st.session_state.queue)
    if not st.session_state.queue:
        return {}
    idx = st.session_state.queue.pop()
    if idx not in df.index:
        return next_queue_record(df)
    st.session_state.current_idx = idx
    return df.loc[idx].to_dict()


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

    # Segunda fila: Validado por agente | Motivo
    second_cols = st.columns(2)
    with second_cols[0]:
        v = html_escape.escape(record.get("Validado", ""))
        components.html(f"<label>Validado por agente</label><input type='text' readonly value='{v}' style='width:100%; font-family: Arial, sans-serif; font-size:14px;'>", height=48)
    with second_cols[1]:
        v = html_escape.escape(record.get("Motivo", ""))
        components.html(f"<label>Motivo</label><input type='text' readonly value='{v}' style='width:100%; font-family: Arial, sans-serif; font-size:14px;'>", height=48)

    st.markdown("---")
    st.subheader("Datos del correo")
    subj = html_escape.escape(record.get("Subject", ""))
    components.html(f"<label>Asunto</label><input type='text' readonly value='{subj}' style='width:100%; font-family: Arial, sans-serif; font-size:14px;'>", height=48)
    # Normalizar saltos de línea: reemplazar múltiples saltos consecutivos por uno solo
    question_text = record.get("Question", "") or ""
    # Unificar finales de línea y colapsar repeticiones (incluye líneas en blanco con espacios)
    question_text = question_text.replace('\r\n', '\n').replace('\r', '\n')
    question_text = re.sub(r"\n\s*\n+", "\n", question_text)

    # Mostrar Question en un expander con altura fija 260px para permitir copia y descarga
    q_key_suffix = str(st.session_state.get("current_idx", "none"))
    with st.expander("Correo completo", expanded=False):
        height = 260
        # Renderizar un textarea HTML readonly para permitir selección/copiado pero no edición
        safe_q = html_escape.escape(question_text)
        ta_html = f"""
        <textarea readonly style="width:100%; height:{height}px; white-space:pre-wrap; font-family: Arial, sans-serif; font-size:14px;">{safe_q}</textarea>
        """
        components.html(ta_html, height=height + 30)
        st.download_button("Descargar correo (.txt)", question_text, file_name=f"correo_{q_key_suffix}.txt", mime="text/plain", key=f"q_dl_{q_key_suffix}")

    # Descomponer MailToAgent (si es JSON) en campos individuales; permitir copiar y descargar
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
                        <label><strong>{html_escape.escape(str(key))}</strong></label>
                        <textarea readonly style="width:100%; height:120px; white-space:pre-wrap; font-family: Arial, sans-serif; font-size:14px;">{safe_val}</textarea>
                        """
                        components.html(ta_html, height=150)
                    else:
                        safe_val = html_escape.escape(str(val))
                        inp_html = f"<label><strong>{html_escape.escape(str(key))}</strong></label><input type=\"text\" readonly value=\"{safe_val}\" style=\"width:100%; font-family: Arial, sans-serif; font-size:14px;\">"
                        components.html(inp_html, height=48)
                pretty = json.dumps(mail_obj, ensure_ascii=False, indent=2)
                st.download_button("Descargar resumen IA (.json)", pretty, file_name=f"mailtoagent_{m_key_suffix}.json", mime="application/json", key=f"mail_dl_{m_key_suffix}")
                # Incluir aquí el campo '¿Faltan datos?' dentro del expander (readonly)
                faltan = html_escape.escape(record.get("Faltan datos?", ""))
                components.html(f"<label>¿Faltan datos?</label><input type='text' readonly value='{faltan}' style='width:100%; font-family: Arial, sans-serif; font-size:14px;'>", height=48)
            else:
                st.text_area("", mail_text, height=180, key=f"mail_area_{m_key_suffix}")
                st.download_button("Descargar resumen IA (.txt)", mail_text, file_name=f"mailtoagent_{m_key_suffix}.txt", mime="text/plain", key=f"mail_dl_txt_{m_key_suffix}")
                faltan = html_escape.escape(record.get("Faltan datos?", ""))
                components.html(f"<label>¿Faltan datos?</label><input type='text' readonly value='{faltan}' style='width:100%; font-family: Arial, sans-serif; font-size:14px;'>", height=48)
        except Exception:
            st.text_area("", mail_text, height=180, key=f"mail_area_{m_key_suffix}")
            st.download_button("Descargar resumen IA (.txt)", mail_text, file_name=f"mailtoagent_{m_key_suffix}.txt", mime="text/plain", key=f"mail_dl_txt_{m_key_suffix}")
            faltan = html_escape.escape(record.get("Faltan datos?", ""))
            components.html(f"<label>¿Faltan datos?</label><input type='text' readonly value='{faltan}' style='width:100%; font-family: Arial, sans-serif; font-size:14px;'>", height=48)


def review_form(record: Dict[str, str]):
    st.subheader("Resultado de revisión")
    with st.form("review_form", clear_on_submit=True):
        status = st.selectbox(
            "Estado final",
            ["Correcto", "Falso positivo", "Falso negativo", "Necesita seguimiento"],
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


def main():
    st.set_page_config(page_title="Revisor de Mayordomo Mail", layout="wide")

    inject_styles()
    inject_overlay()

    if not EXCEL_PATH.exists():
        st.error("No se encuentra el archivo Validados_V3.xlsx en la raíz del proyecto.")
        return

    if "df" not in st.session_state:
        st.session_state.df = load_dataset()
    df = st.session_state.df
    layout_sidebar(df)

    # Asegurar current antes de renderizar header para poder mostrar campos principales en el header
    if "current" not in st.session_state:
        st.session_state.current = next_queue_record(df)

    # Cabecera principal: logo (reducido), título y campos principales en sticky header
    logo_path = Path("app/static/img/ey.png")
    current = st.session_state.get("current")
    fecha_val = ""
    id_val = ""
    auto_val = ""
    if current:
        fecha_val = html_escape.escape(format_timestamp(current.get("@timestamp", "")))
        id_val = html_escape.escape(current.get("IdCorreo", ""))
        auto_val = html_escape.escape(current.get("Automatismo", ""))

    try:
        b64 = base64.b64encode(logo_path.read_bytes()).decode() if logo_path.exists() else None
    except Exception:
        b64 = None

    header_html = f"""
    <div class='mymail-header'>
        {f"<img class='logo' src='data:image/png;base64,{b64}' />" if b64 else ''}
        <div class='title'>Revisor de Mayordomo Mail</div>
        <div class='top-fields'>
            <div class='field'><strong>Fecha:</strong> {fecha_val}</div>
            <div class='field'><strong>ID Correo:</strong> {id_val}</div>
            <div class='field'><strong>Automatismo:</strong> {auto_val}</div>
        </div>
    </div>
    """
    st.markdown(header_html, unsafe_allow_html=True)
    st.caption("Revisa el feedback del agente para analizar mejoras sobre la solución")

    if not st.session_state.current:
        st.success("No quedan filas pendientes en el Excel. ¡Buen trabajo!")
        return

    layout_record(st.session_state.current)
    submitted, skip, status, reviewer_note, internal_note = review_form(st.session_state.current)

    if skip:
        # Guardar registro descartado en CSV y eliminar fila del Excel
        append_skip(st.session_state.current)
        idx = st.session_state.get("current_idx")
        if idx is not None:
            st.session_state.df = st.session_state.df.drop(index=idx)
            persist_excel(st.session_state.df)
        st.session_state.current = next_queue_record(st.session_state.df)
        st.success("Fila descartada y registrada en descartes.csv")
        return

    if submitted:
        append_review(st.session_state.current, status, reviewer_note, internal_note)
        idx = st.session_state.get("current_idx")
        if idx is not None:
            st.session_state.df = st.session_state.df.drop(index=idx)
            persist_excel(st.session_state.df)
        st.session_state.current = next_queue_record(st.session_state.df)
        st.success("Revisión guardada y fila eliminada del Excel.")
        return


if __name__ == "__main__":
    main()
