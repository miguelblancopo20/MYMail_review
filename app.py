"""Streamlit UI para revisar los correos de Mayordomo Mail."""
from __future__ import annotations

import base64
import html as html_escape
from datetime import datetime
from pathlib import Path
from typing import Dict

import streamlit as st

from helpers import (
    EXCEL_PATH,
    append_review,
    append_skip,
    format_timestamp,
    layout_record,
    layout_sidebar,
    load_dataset,
    next_queue_record,
    persist_excel,
    review_form,
)


def load_logo_base64() -> str | None:
    """Carga el logo de EY en base64 si existe."""
    logo_path = Path("app/static/img/ey.png")
    try:
        if logo_path.exists():
            return base64.b64encode(logo_path.read_bytes()).decode()
    except Exception:
        return None
    return None


def inject_global_css() -> None:
    """Inyecta estilos y utilidades globales para header sticky y overlay."""
    css = """
    <style>
    :root {
        --mymail-header-height: 88px;
    }
    body { margin: 0; }
    .block-container { padding-top: calc(var(--mymail-header-height) + 8px) !important; }
    .mymail-header {
        position: fixed;
        top: 0;
        left: 0;
        right: 0;
        height: var(--mymail-header-height);
        z-index: 12000;
        display: flex;
        flex-direction: row;
        align-items: center;
        gap: 12px;
        padding: 12px 20px;
        background: var(--background-color, #fff);
        border-bottom: 1px solid rgba(0,0,0,0.08);
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08);
    }
    @media (max-width: 640px) {
        .mymail-header { flex-wrap: wrap; justify-content: center; text-align: center; gap: 8px; }
        .mymail-header .top-fields { width: 100%; justify-content: center; }
    }
    .mymail-header .logo { height: 40px; margin-right: 4px; }
    .mymail-header .title { font-size: 22px; font-weight: 700; margin-right: 12px; }
    .mymail-header .top-fields { margin-left: 8px; display: flex; flex-wrap: wrap; gap: 8px; }
    .mymail-header .field { font-size: 13px; padding: 4px 10px; border-radius: 6px; background: #f5f7fb; }
    .mymail-field .field { font-size: 13px; padding: 2px 8px; border-radius: 6px; background: #f5f7fb; }

    .loading-overlay { position: fixed; inset: 0; z-index: 20000; display: none; align-items: center; justify-content: center; background: rgba(255,255,255,0.65); backdrop-filter: blur(2px); pointer-events: none; }
    html.busy .loading-overlay { display: flex; pointer-events: all; }
    .loading-card { padding: 16px 20px; border-radius: 12px; background: white; border: 1px solid rgba(0,0,0,0.10); box-shadow: 0 10px 30px rgba(0,0,0,0.12); font-weight: 600; display: flex; gap: 10px; align-items: center; }
    .loading-spinner { width: 24px; height: 24px; border-radius: 50%; border: 4px solid rgba(0,0,0,0.08); border-top-color: #111827; animation: mymail-spin 1s linear infinite; }
    @keyframes mymail-spin { to { transform: rotate(360deg);} }
    html.busy, html.busy body { overflow: hidden; }
    
    .toast-container { position: fixed; top: 96px; right: 16px; z-index: 22000; display: flex; flex-direction: column; gap: 10px; }
    .toast { min-width: 220px; max-width: 320px; background: #111827; color: white; padding: 12px 14px; border-radius: 10px; box-shadow: 0 6px 18px rgba(0,0,0,0.18); font-weight: 600; }

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
    """
    st.markdown(css, unsafe_allow_html=True)


def render_sticky_header(current: Dict[str, str] | None) -> None:
    """Renderiza el banner superior sticky con datos de contexto."""
    fecha_val = html_escape.escape(format_timestamp(current.get("@timestamp", ""))) if current else ""
    id_val = html_escape.escape(current.get("IdCorreo", "")) if current else ""
    auto_val = html_escape.escape(current.get("Automatismo", "")) if current else ""

    b64 = load_logo_base64()

    header_html = f"""
    <div class='mymail-header'>
        {f"<img class='logo' src='data:image/png;base64,{b64}' alt='EY logo' />" if b64 else ''}
        <div class='title'>Revisor de Mayordomo Mail</div>
        <div class='top-fields'>
            <div class='field'><strong>Fecha:</strong> {fecha_val}</div>
            <div class='field'><strong>ID Correo:</strong> {id_val}</div>
            <div class='field'><strong>Automatismo:</strong> {auto_val}</div>
        </div>
    </div>
    """
    st.markdown(header_html, unsafe_allow_html=True)


def render_loading_overlay() -> None:
    """Inserta overlay de carga que bloquea la UI cuando busy=True."""
    overlay_html = """
    <div class='loading-overlay'>
        <div class='loading-card'>
            <div class='loading-spinner'></div>
            <div>Cargando…</div>
        </div>
    </div>
    <script>
    (function() {
        const root = document.documentElement;
        if (%(busy)s) {
            root.classList.add('busy');
        } else {
            root.classList.remove('busy');
        }
    })();
    </script>
    """ % {"busy": "true" if st.session_state.get("busy") else "false"}
    st.markdown(overlay_html, unsafe_allow_html=True)


def render_login() -> None:
    """Pantalla de autenticación simple para acceder a la app."""
    b64 = load_logo_base64()
    st.title("Revisor de Mayordomo Mail")
    if b64:
        st.markdown(
            f"""
            <div style='display:flex;justify-content:center;margin:24px 0;'>
                <img src='data:image/png;base64,{b64}' alt='EY logo' style='height:120px;'>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.subheader("Inicio de sesión")
    with st.form("login_form"):
        username = st.text_input("Usuario")
        password = st.text_input("Contraseña", type="password")
        submitted = st.form_submit_button("Acceder")

    if submitted:
        if username == "admin" and password == "admin":
            st.session_state.authenticated = True
            st.success("Acceso concedido. Cargando la aplicación…")
            st.experimental_rerun()
        else:
            st.error("Usuario o contraseña incorrectos. Inténtalo de nuevo.")


def show_success_toast():
    toast_cfg = st.session_state.get("toast")
    if not toast_cfg:
        return
    msg = toast_cfg.get("msg")
    ts = toast_cfg.get("ts", "")
    toast_html = """
    <div class='toast-container' id='toast-container'>
        <div class='toast' data-ts='{ts}'>{msg}</div>
    </div>
    <script>
    (function(){{
        const tc = document.getElementById('toast-container');
        if(!tc) return;
        setTimeout(()=>{{ tc.remove(); }}, 5000);
    }})();
    </script>
    """.format(ts=html_escape.escape(str(ts)), msg=html_escape.escape(str(msg)))
    st.markdown(toast_html, unsafe_allow_html=True)
    st.session_state.toast = None


def scroll_to_top() -> None:
    st.markdown("<script>window.parent.scrollTo({top:0, behavior:'auto'});</script>", unsafe_allow_html=True)


def run_pending_action() -> None:
    action = st.session_state.get("pending_action")
    if not action:
        return

    render_loading_overlay()

    try:
        st.session_state.busy = True
        if action == "save_next":
            status = st.session_state.get("pending_status", "")
            reviewer_note = st.session_state.get("pending_reviewer_note", "")
            internal_note = st.session_state.get("pending_internal_note", "")
            append_review(st.session_state.current, status, reviewer_note, internal_note)
            idx = st.session_state.get("current_idx")
            if idx is not None and idx in st.session_state.df.index:
                st.session_state.df = st.session_state.df.drop(index=idx)
                persist_excel(st.session_state.df)
            if "queue" in st.session_state:
                st.session_state.queue = [i for i in st.session_state.queue if i != idx]
            st.session_state.current = next_queue_record(st.session_state.df)
            st.session_state.toast = {"msg": "Guardado y cargado correctamente", "ts": datetime.now().isoformat()}
        elif action == "skip_next":
            append_skip(st.session_state.current)
            idx = st.session_state.get("current_idx")
            if idx is not None and idx in st.session_state.df.index:
                st.session_state.df = st.session_state.df.drop(index=idx)
                persist_excel(st.session_state.df)
            if "queue" in st.session_state:
                st.session_state.queue = [i for i in st.session_state.queue if i != idx]
            st.session_state.current = next_queue_record(st.session_state.df)
            st.session_state.toast = {"msg": "Saltado correctamente", "ts": datetime.now().isoformat()}
        elif action == "next":
            if "current_idx" in st.session_state:
                st.session_state.queue.insert(0, st.session_state.current_idx)
            st.session_state.current = next_queue_record(st.session_state.df)
            st.session_state.toast = {"msg": "Avanzado al siguiente registro", "ts": datetime.now().isoformat()}
        st.session_state.scroll_top = True
    finally:
        st.session_state.busy = False
        st.session_state.pending_action = None
        st.session_state.pending_status = None
        st.session_state.pending_reviewer_note = None
        st.session_state.pending_internal_note = None


def ensure_base_state():
    st.session_state.setdefault("busy", False)
    st.session_state.setdefault("pending_action", None)
    st.session_state.setdefault("pending_status", None)
    st.session_state.setdefault("pending_reviewer_note", None)
    st.session_state.setdefault("pending_internal_note", None)
    st.session_state.setdefault("toast", None)
    st.session_state.setdefault("scroll_top", False)
    st.session_state.setdefault("queue", [])


def main():
    st.set_page_config(page_title="Revisor de Mayordomo Mail", layout="wide")
    ensure_base_state()
    inject_global_css()
    render_loading_overlay()

    if st.session_state.get("pending_action"):
        run_pending_action()

    if not st.session_state.get("authenticated"):
        render_login()
        return

    if not EXCEL_PATH.exists():
        st.error("No se encuentra el archivo Validados_V3.xlsx en la raíz del proyecto.")
        return

    if "df" not in st.session_state:
        st.session_state.df = load_dataset()
    df = st.session_state.df
    layout_sidebar(df)

    if "current" not in st.session_state:
        st.session_state.current = next_queue_record(df)

    render_sticky_header(st.session_state.get("current"))
    layout_record(st.session_state.current)

    submitted, skip, go_next, status, reviewer_note, internal_note = review_form(st.session_state.current)

    if submitted:
        st.session_state.pending_status = status
        st.session_state.pending_reviewer_note = reviewer_note
        st.session_state.pending_internal_note = internal_note
        st.session_state.pending_action = "save_next"
        st.session_state.busy = True
        st.experimental_rerun()

    if go_next:
        st.session_state.pending_action = "next"
        st.session_state.busy = True
        st.experimental_rerun()

    if skip:
        st.session_state.pending_action = "skip_next"
        st.session_state.busy = True
        st.experimental_rerun()

    if st.session_state.get("scroll_top"):
        scroll_to_top()
        st.session_state["scroll_top"] = False

    show_success_toast()


if __name__ == "__main__":
    main()
