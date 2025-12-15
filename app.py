"""Streamlit UI para revisar los correos de Mayordomo Mail."""
from __future__ import annotations

import base64
import html as html_escape
from pathlib import Path
from typing import Dict

import streamlit as st

from helpers import (
    EXCEL_PATH,
    append_review,
    append_skip,
    format_timestamp,
    inject_overlay,
    inject_styles,
    layout_record,
    layout_sidebar,
    load_dataset,
    next_queue_record,
    persist_excel,
    review_form,
)

 
def render_header(current: Dict[str, str] | None) -> None:
    """Renderiza el banner superior con el logo y los campos principales."""
    logo_path = Path("app/static/img/ey.png")
    fecha_val = html_escape.escape(format_timestamp(current.get("@timestamp", ""))) if current else ""
    id_val = html_escape.escape(current.get("IdCorreo", "")) if current else ""
    auto_val = html_escape.escape(current.get("Automatismo", "")) if current else ""

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
    inject_overlay()


def handle_skip(df):
    with st.spinner("Descartando registro y cargando siguiente..."):
        append_skip(st.session_state.current)
        idx = st.session_state.get("current_idx")
        if idx is not None and idx in st.session_state.df.index:
            st.session_state.df = st.session_state.df.drop(index=idx)
            persist_excel(st.session_state.df)
        if "queue" in st.session_state:
            st.session_state.queue = [i for i in st.session_state.queue if i != idx]
        st.session_state.current = next_queue_record(st.session_state.df)
    st.success("Fila descartada y registrada en descartes.csv")
    st.session_state["scroll_top"] = True


def handle_submit(df, status: str, reviewer_note: str, internal_note: str):
    with st.spinner("Guardando revisión y cargando siguiente..."):
        append_review(st.session_state.current, status, reviewer_note, internal_note)
        idx = st.session_state.get("current_idx")
        if idx is not None and idx in st.session_state.df.index:
            st.session_state.df = st.session_state.df.drop(index=idx)
            persist_excel(st.session_state.df)
        if "queue" in st.session_state:
            st.session_state.queue = [i for i in st.session_state.queue if i != idx]
        st.session_state.current = next_queue_record(st.session_state.df)
    st.success("Revisión guardada y fila eliminada del Excel.")
    st.session_state["scroll_top"] = True


def main():
    st.set_page_config(page_title="Revisor de Mayordomo Mail", layout="wide")
    inject_styles()

    if not EXCEL_PATH.exists():
        st.error("No se encuentra el archivo Validados_V3.xlsx en la raíz del proyecto.")
        return

    if "df" not in st.session_state:
        st.session_state.df = load_dataset()
    df = st.session_state.df
    layout_sidebar(df)

    if "current" not in st.session_state:
        st.session_state.current = next_queue_record(df)

    render_header(st.session_state.get("current"))
    layout_record(st.session_state.current)

    submitted, skip, status, reviewer_note, internal_note = review_form(st.session_state.current)

    if skip:
        handle_skip(df)
        return

    if submitted:
        handle_submit(df, status, reviewer_note, internal_note)
        return


if __name__ == "__main__":
    main()
