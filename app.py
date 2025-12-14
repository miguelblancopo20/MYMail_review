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
import os
from helpers import *


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

    # Segunda fila: Validado por agente | Motivo | Comentario
    second_cols = st.columns([1, 1, 1])
    with second_cols[0]:
        v = html_escape.escape(record.get("Validado", ""))
        components.html(f"<label>Validado por agente</label><input type='text' readonly value='{v}' style='width:100%; font-family: Arial, sans-serif; font-size:14px;'>", height=48)
    with second_cols[1]:
        v = html_escape.escape(record.get("Motivo", ""))
        components.html(f"<label>Motivo</label><input type='text' readonly value='{v}' style='width:100%; font-family: Arial, sans-serif; font-size:14px;'>", height=48)
    with second_cols[2]:
        v = html_escape.escape(record.get("Comentario", ""))
        components.html(f"<label>Comentario</label><input type='text' readonly value='{v}' style='width:100%; font-family: Arial, sans-serif; font-size:14px;'>", height=48)

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
    # Inject overlay JS (from static file)
    inject_overlay()

    layout_record(st.session_state.current)
    submitted, skip, status, reviewer_note, internal_note = review_form(st.session_state.current)

    if skip:
        # Mostrar spinner mientras se procesa el salto y se carga la siguiente fila
        with st.spinner("Descartando registro y cargando siguiente..."):
            append_skip(st.session_state.current)
            idx = st.session_state.get("current_idx")
            if idx is not None:
                if idx in st.session_state.df.index:
                    st.session_state.df = st.session_state.df.drop(index=idx)
                    persist_excel(st.session_state.df)
                else:
                    # índice ya no existe en el DataFrame (posible duplicado); ignorar
                    pass
                # Eliminar cualquier referencia al índice descartado en la cola
                if "queue" in st.session_state:
                    st.session_state.queue = [i for i in st.session_state.queue if i != idx]
            st.session_state.current = next_queue_record(st.session_state.df)
        st.success("Fila descartada y registrada en descartes.csv")
        st.session_state["scroll_top"] = True
        return

    if submitted:
        # Mostrar spinner mientras se guarda la revisión y se carga la siguiente fila
        with st.spinner("Guardando revisión y cargando siguiente..."):
            append_review(st.session_state.current, status, reviewer_note, internal_note)
            idx = st.session_state.get("current_idx")
            if idx is not None:
                if idx in st.session_state.df.index:
                    st.session_state.df = st.session_state.df.drop(index=idx)
                    persist_excel(st.session_state.df)
                else:
                    # índice ya no existe en el DataFrame (posible duplicado); ignorar
                    pass
                # Eliminar cualquier referencia al índice guardado en la cola
                if "queue" in st.session_state:
                    st.session_state.queue = [i for i in st.session_state.queue if i != idx]
            st.session_state.current = next_queue_record(st.session_state.df)
        st.success("Revisión guardada y fila eliminada del Excel.")
        st.session_state["scroll_top"] = True
        return


if __name__ == "__main__":
    main()
