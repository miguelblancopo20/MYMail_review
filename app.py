"""Streamlit UI para revisar los correos de Mayordomo Mail."""
from __future__ import annotations

import csv
import random
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd
import streamlit as st

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
            "Fecha de revisión": datetime.utcnow().isoformat(),
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
    if "queue" not in st.session_state:
        st.session_state.queue = list(df.index)
        random.shuffle(st.session_state.queue)
    if not st.session_state.queue:
        return {}
    idx = st.session_state.queue.pop()
    st.session_state.current_idx = idx
    return df.loc[idx].to_dict()


def skip_current() -> None:
    if "current_idx" in st.session_state:
        st.session_state.queue.insert(0, st.session_state.current_idx)
    st.session_state.current_idx = None
    st.session_state.current = None


def layout_sidebar(df: pd.DataFrame) -> None:
    st.sidebar.header("Identidad visual")
    logo_file = st.sidebar.file_uploader("Logo (png/jpg)")
    if logo_file:
        st.sidebar.image(logo_file, use_column_width=True)

    st.sidebar.markdown("---")
    pending = len(st.session_state.get("queue", [])) if "queue" in st.session_state else len(df.index)
    st.sidebar.metric("Pendientes", pending)

    title = st.sidebar.text_input("Título", value="Revisor de Mayordomo Mail")
    st.title(title)
    st.caption(
        "Revisa cada envío de correo, valida la clasificación automática y deja constancia de los ajustes necesarios."
    )


def layout_record(record: Dict[str, str]) -> None:
    st.subheader("Datos del envío")
    meta_cols = st.columns(4)
    meta_cols[0].text_input("Fecha", record.get("@timestamp", ""), disabled=True)
    meta_cols[1].text_input("Validado por agente", record.get("Validado", ""), disabled=True)
    meta_cols[2].text_input("Motivo", record.get("Motivo", ""), disabled=True)
    meta_cols[3].text_input("Documento", record.get("Documento", ""), disabled=True)

    detail_cols = st.columns(4)
    detail_cols[0].text_input("Matrícula", record.get("MatriculaAsesor", ""), disabled=True)
    detail_cols[1].text_input("Page name", record.get("PageName", ""), disabled=True)
    detail_cols[2].text_input("ID Correo", record.get("IdCorreo", ""), disabled=True)
    detail_cols[3].text_input("Automatismo", record.get("Automatismo", ""), disabled=True)

    location_cols = st.columns(3)
    location_cols[0].text_input("Segmento", record.get("Segmento", ""), disabled=True)
    location_cols[1].text_input("Location", record.get("Location", ""), disabled=True)
    location_cols[2].text_input("Sublocation", record.get("Sublocation", ""), disabled=True)

    st.markdown("---")
    st.text_input("Asunto", record.get("Subject", ""), disabled=True)
    st.text_area(
        "Correo completo (Question)",
        record.get("Question", ""),
        height=260,
        disabled=True,
    )
    st.text_area(
        "Resumen IA (MailToAgent)",
        record.get("MailToAgent", ""),
        height=180,
        disabled=True,
    )
    st.text_input("¿Faltan datos?", record.get("Faltan datos?", ""), disabled=True)


def review_form(record: Dict[str, str]):
    st.subheader("Resultado de revisión")
    with st.form("review_form", clear_on_submit=True):
        status = st.selectbox(
            "Estado final",
            ["Correcto", "Falso positivo", "Falso negativo", "Necesita seguimiento"],
        )
        reviewer_note = st.text_area(
            "Comentario de revisión",
            placeholder="Describe por qué la etiqueta es correcta o qué ajuste necesita.",
            height=150,
        )
        internal_note = st.text_area(
            "Nota interna (opcional)",
            placeholder="Observaciones operativas o pasos siguientes.",
            height=120,
        )
        col1, col2 = st.columns([3, 1])
        submitted = col1.form_submit_button("Guardar revisión y pasar al siguiente")
        skip = col2.form_submit_button("Saltar sin guardar", type="secondary")
    return submitted, skip, status, reviewer_note, internal_note


def main():
    st.set_page_config(page_title="Revisor de Mayordomo Mail", layout="wide")

    if not EXCEL_PATH.exists():
        st.error("No se encuentra el archivo Validados_V3.xlsx en la raíz del proyecto.")
        return

    if "df" not in st.session_state:
        st.session_state.df = load_dataset()
    df = st.session_state.df
    layout_sidebar(df)

    if "current" not in st.session_state:
        st.session_state.current = next_queue_record(df)

    if not st.session_state.current:
        st.success("No quedan filas pendientes en el Excel. ¡Buen trabajo!")
        return

    layout_record(st.session_state.current)
    submitted, skip, status, reviewer_note, internal_note = review_form(st.session_state.current)

    if skip:
        skip_current()
        st.experimental_rerun()

    if submitted:
        append_review(st.session_state.current, status, reviewer_note, internal_note)
        idx = st.session_state.get("current_idx")
        if idx is not None:
            st.session_state.df = st.session_state.df.drop(index=idx)
            persist_excel(st.session_state.df)
        st.session_state.current = next_queue_record(st.session_state.df)
        st.success("Revisión guardada y fila eliminada del Excel.")
        st.experimental_rerun()


if __name__ == "__main__":
    main()
