from __future__ import annotations

import json
import os
import re
import unicodedata
from datetime import datetime, timedelta, timezone
from pathlib import Path

from flask import Flask, redirect, render_template, request, send_from_directory, session, url_for

import config
from mymail.state import get_state, reset_state
from mymail.tables import log_click, verify_user
from mymail.tables import _list_by_days


def create_app() -> Flask:
    app = Flask(__name__, static_folder="static", template_folder="templates")
    app.secret_key = (
        (getattr(config, "FLASK_SECRET_KEY", "") or "").strip()
        or (os.environ.get("FLASK_SECRET_KEY", "") or "").strip()
        or "dev-secret-change-me"
    )

    def normalize_multiline(value: str) -> str:
        if not value:
            return ""
        value = value.replace("\r\n", "\n").replace("\r", "\n")
        return re.sub(r"\n{2,}", "\n", value)

    def norm_key(value: str) -> str:
        value = "".join(
            ch for ch in unicodedata.normalize("NFKD", value) if not unicodedata.combining(ch)
        ).strip()
        value = re.sub(r"[\s_-]+", " ", value).strip().lower()
        return value

    def parse_mailtoagent(value: str):
        if not value:
            return None
        try:
            obj = json.loads(value)
        except Exception:
            return None

        if isinstance(obj, dict):
            items = []
            for key, val in obj.items():
                if isinstance(val, (dict, list)):
                    items.append((str(key), json.dumps(val, ensure_ascii=False, indent=2)))
                else:
                    items.append((str(key), "" if val is None else str(val)))
            return items

        if isinstance(obj, list):
            return [("root", json.dumps(obj, ensure_ascii=False, indent=2))]
        return [("value", str(obj))]

    @app.get("/assets/<path:filename>")
    def assets(filename: str):
        base = Path("app/static").resolve()
        target = (base / filename).resolve()
        if base not in target.parents and target != base:
            return ("Not found", 404)
        if not target.exists() or not target.is_file():
            return ("Not found", 404)
        return send_from_directory(base, filename)

    @app.get("/")
    def index():
        if not session.get("authenticated"):
            return redirect(url_for("login"))
        return redirect(url_for("review"))

    @app.get("/login")
    def login():
        if session.get("authenticated"):
            return redirect(url_for("review"))
        return render_template("login.html", error=None, title="Revisor de Mayordomo Mail")

    @app.post("/login")
    def login_post():
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""

        auth = verify_user(username, password)
        if auth.ok:
            session["authenticated"] = True
            session["user"] = username
            log_click(action="login", username=username, result="ok")
            reset_state()
            return redirect(url_for("review"))

        log_click(action="login", username=username, result=f"fail:{auth.reason}")
        return render_template(
            "login.html",
            error="Usuario o contraseña incorrectos. Inténtalo de nuevo.",
            title="Revisor de Mayordomo Mail",
        )

    @app.get("/logout")
    def logout():
        if session.get("authenticated") and session.get("user"):
            log_click(action="logout", username=session.get("user", ""), result="ok")
        session.clear()
        reset_state()
        return redirect(url_for("login"))

    @app.get("/review")
    def review():
        if not session.get("authenticated"):
            return redirect(url_for("login"))

        state = get_state()
        if state.excel_missing:
            return render_template("done.html", message="No se puede acceder a la entrada (tabla 'entrada'). Revisa Azure y config.py.")

        record = state.current_record()
        if not record:
            return render_template("done.html", message="No quedan registros pendientes.")

        record = dict(record)
        record["Question"] = normalize_multiline(record.get("Question", ""))

        mail_items = parse_mailtoagent(record.get("MailToAgent", ""))
        mail_norm = {norm_key(str(k)): v for k, v in (mail_items or [])}

        def get_mail(*norm_keys: str) -> str:
            for k in norm_keys:
                if k in mail_norm and str(mail_norm.get(k, "")).strip():
                    return str(mail_norm.get(k, ""))
            return ""

        mail_meta = {
            "From": get_mail("from", "remitente"),
            "Ficha": get_mail("ficha", "ficha cliente"),
            "Categorización": get_mail("categorizacion", "categoria", "categoria final"),
            "Acción": get_mail("accion", "accion final"),
        }

        def first_by_prefix(*prefixes: str) -> str:
            for prefix in prefixes:
                for nk, val in mail_norm.items():
                    if nk.startswith(prefix) and str(val).strip():
                        return str(val)
            return ""

        act_summary = first_by_prefix("resumen")
        act_proposal = first_by_prefix("propuesta de actuacion", "propuesta actuacion")
        if not act_proposal:
            for nk, val in mail_norm.items():
                if nk.startswith("propuesta") and nk != "propuesta respuesta" and str(val).strip():
                    act_proposal = str(val)
                    break
        act_params = first_by_prefix("parametros")

        act_items = []
        if mail_items:
            skip_norm = {
                "from",
                "remitente",
                "ficha",
                "ficha cliente",
                "categorizacion",
                "categoria",
                "categoria final",
                "accion",
                "accion final",
                "resumen",
                "propuesta",
                "propuesta respuesta",
                "propuesta de actuacion",
                "propuesta actuacion",
                "parametros",
            }
            for k, v in mail_items:
                nk = norm_key(str(k))
                if nk in skip_norm:
                    continue
                act_items.append((str(k), str(v)))

        return render_template(
            "review.html",
            record=record,
            mail_meta=mail_meta,
            act_summary=act_summary,
            act_proposal=act_proposal,
            act_params=act_params,
            act_items=act_items,
            pending=state.pending_count(),
            current_user=session.get("user", ""),
            title="Revisor de Mayordomo Mail",
            error=request.args.get("error"),
        )

    @app.get("/stats")
    def stats():
        if not session.get("authenticated"):
            return redirect(url_for("login"))

        days = 14
        now = datetime.now(timezone.utc)
        day_keys = [(now - timedelta(days=i)).strftime("%Y%m%d") for i in range(days)]

        try:
            resultados = _list_by_days(getattr(config, "TABLE_RESULTADOS", "resultados"), day_keys)
            descartes = _list_by_days(getattr(config, "TABLE_DESCARTES", "descartes"), day_keys)
        except Exception as exc:
            return render_template(
                "stats.html",
                title="Stats",
                current_user=session.get("user", ""),
                error=str(exc),
                days=days,
                total_resultados=0,
                total_descartes=0,
                ko_rate="0%",
                duda_count=0,
                by_status=[],
                by_user=[],
                by_day=[],
                top_automatismos=[],
            )

        def count_by(items, key):
            out = {}
            for it in items:
                val = str(it.get(key, "") or "")
                out[val] = out.get(val, 0) + 1
            return sorted(out.items(), key=lambda kv: kv[1], reverse=True)

        total_resultados = len(resultados)
        total_descartes = len(descartes)
        by_status = count_by(resultados, "status")
        by_user = count_by(resultados, "user")
        by_day = count_by(resultados, "day")
        top_automatismos = count_by(resultados, "automatismo")[:10]

        ko_count = sum(v for k, v in by_status if k.startswith("KO"))
        duda_count = sum(v for k, v in by_status if k == "DUDA")
        ko_rate = f"{round((ko_count / total_resultados) * 100)}%" if total_resultados else "0%"

        return render_template(
            "stats.html",
            title="Stats",
            current_user=session.get("user", ""),
            error=None,
            days=days,
            total_resultados=total_resultados,
            total_descartes=total_descartes,
            ko_rate=ko_rate,
            duda_count=duda_count,
            by_status=by_status,
            by_user=by_user,
            by_day=by_day,
            top_automatismos=top_automatismos,
        )

    @app.post("/action")
    def action():
        if not session.get("authenticated"):
            return redirect(url_for("login"))

        state = get_state()
        username = session.get("user", "")

        action_type = request.form.get("action") or ""
        status = request.form.get("status") or "Pendiente"
        reviewer_note = request.form.get("reviewer_note") or ""
        internal_note = request.form.get("internal_note") or ""

        if status == "Pendiente" and action_type != "skip":
            log_click(action=action_type or "action", username=username, result="blocked:pendiente")
            return redirect(url_for("review", error="Selecciona un estado final para continuar."))

        record_id = state.current_record().get("IdCorreo", "")

        if action_type == "save":
            if (status.startswith("KO") or status == "DUDA") and not reviewer_note.strip():
                log_click(
                    action="save",
                    username=username,
                    record_id=record_id,
                    result="blocked:missing_comment",
                    extra={"status": status},
                )
                return redirect(
                    url_for(
                        "review",
                        error="Para un KO o DUDA es obligatorio indicar un comentario de revisión.",
                    )
                )

            state.submit_current(
                username=username,
                status=status,
                reviewer_note=reviewer_note,
                internal_note=internal_note,
            )
            log_click(action="save", username=username, record_id=record_id, result="ok", extra={"status": status})
            return redirect(url_for("review"))

        if action_type == "skip":
            state.skip_current(username=username)
            log_click(action="skip", username=username, record_id=record_id, result="ok", extra={"status": status})
            return redirect(url_for("review"))

        log_click(action="action", username=username, record_id=record_id, result="fail:unknown_action")
        return redirect(url_for("review", error="Acción no válida."))

    return app


app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8000")), debug=True)
