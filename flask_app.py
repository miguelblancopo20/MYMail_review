from __future__ import annotations

import json
import os
import re
import unicodedata
from datetime import datetime, timedelta, timezone

from flask import Flask, jsonify, redirect, render_template, request, session, url_for

import config
from mymail.entrada import EntradaKey, clear_expired_locks, refresh_lock, release_lock, validate_lock
from mymail.entrada import get_record as entrada_get_record
from mymail.entrada import delete_record as entrada_delete_record
from mymail.state import get_state, reset_state
from mymail.revisiones_blob import list_revisions
from mymail.tables import ROLE_ADMIN, list_users, log_click, verify_user
from mymail.tables import _list_by_days
from mymail.tables import write_descarte, write_resultado


def create_app() -> Flask:
    app = Flask(__name__, static_folder="static", template_folder="templates")
    app_version = (getattr(config, "APP_VERSION", "") or "").strip() or "0.0.0"
    app.secret_key = (
        (getattr(config, "FLASK_SECRET_KEY", "") or "").strip()
        or (os.environ.get("FLASK_SECRET_KEY", "") or "").strip()
        or "dev-secret-change-me"
    )

    def normalize_multiline(value: str) -> str:
        if not value:
            return ""
        value = value.replace("\r\n", "\n").replace("\r", "\n")
        value = re.sub(r"\n{2,}", "\n", value)
        lines = value.split("\n")
        out = []
        prev_quote_only = False
        for ln in lines:
            quote_only = ln.strip() == ">"
            if quote_only and prev_quote_only:
                continue
            out.append(ln)
            prev_quote_only = quote_only
        return "\n".join(out)

    def format_ts(value: str) -> str:
        if not value:
            return ""
        try:
            v = value.strip()
            if v.endswith("Z"):
                v = v[:-1] + "+00:00"
            dt = datetime.fromisoformat(v)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dt = dt.astimezone(timezone.utc)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return value

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

    @app.get("/")
    def index():
        if not session.get("authenticated"):
            return redirect(url_for("login"))
        return redirect(url_for("menu"))

    @app.get("/menu")
    def menu():
        if not session.get("authenticated"):
            return redirect(url_for("login"))
        can_stats = (session.get("role") or "") == ROLE_ADMIN
        return render_template(
            "menu.html",
            title="Menú",
            current_user=session.get("user", ""),
            app_version=app_version,
            can_stats=can_stats,
            error=None,
        )

    @app.get("/login")
    def login():
        if session.get("authenticated"):
            return redirect(url_for("menu"))
        return render_template("login.html", error=None, title="Revisor de Mayordomo Mail", app_version=app_version)

    @app.post("/login")
    def login_post():
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""

        auth = verify_user(username, password)
        if auth.ok:
            session["authenticated"] = True
            session["user"] = username
            session["role"] = auth.role or ""
            log_click(action="login", username=username, result="ok")
            try:
                cleared = clear_expired_locks()
                if cleared:
                    log_click(action="lock_cleanup", username=username, result="ok", extra={"cleared": cleared})
            except Exception:
                pass
            reset_state()
            return redirect(url_for("menu"))

        log_click(action="login", username=username, result=f"fail:{auth.reason}")
        return render_template(
            "login.html",
            error="Usuario o contraseña incorrectos. Inténtalo de nuevo.",
            title="Revisor de Mayordomo Mail",
            app_version=app_version,
        )

    @app.get("/logout")
    def logout():
        if session.get("authenticated") and session.get("user"):
            log_click(action="logout", username=session.get("user", ""), result="ok")
            try:
                lock = session.get("_lock") or {}
                pk = str(lock.get("pk", "") or "")
                rk = str(lock.get("rk", "") or "")
                token = str(lock.get("token", "") or "")
                if pk and rk and token:
                    release_lock(EntradaKey(partition_key=pk, row_key=rk), owner=session.get("user", ""), token=token)
                session.pop("_lock", None)
            except Exception:
                pass
            reset_state()
        session.clear()
        return redirect(url_for("login"))

    @app.get("/review")
    def review():
        if not session.get("authenticated"):
            return redirect(url_for("login"))

        state = get_state()
        if state.excel_missing:
            return render_template("done.html", message="No se puede acceder a la entrada (tabla 'entrada'). Revisa Azure y config.py.")

        username = session.get("user", "")
        error = session.pop("_error", None)
        lock_until_ms = session.get("_lock_until_ms")
        record = state.current_record(owner=username)
        if not record:
            return render_template("done.html", message="No quedan registros pendientes o disponibles.")

        try:
            if state.current_key and state.lock_token:
                session["_lock"] = {
                    "pk": state.current_key.partition_key,
                    "rk": state.current_key.row_key,
                    "token": state.lock_token,
                }
                new_until = refresh_lock(state.current_key, owner=username, token=state.lock_token)
                if new_until:
                    lock_until_ms = int(new_until.timestamp() * 1000)
                    session["_lock_until_ms"] = lock_until_ms
        except Exception:
            pass

        record = dict(record)
        record["@timestamp"] = format_ts(str(record.get("@timestamp", "") or ""))
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
            can_stats=(session.get("role") or "") == ROLE_ADMIN,
            app_version=app_version,
            title="Revisor de Mayordomo Mail",
            error=error,
            lock_until_ms=lock_until_ms,
        )

    @app.get("/refresh")
    def refresh():
        if not session.get("authenticated"):
            return redirect(url_for("login"))
        username = session.get("user", "")
        lock = session.get("_lock") or {}
        pk = str(lock.get("pk", "") or "")
        rk = str(lock.get("rk", "") or "")
        token = str(lock.get("token", "") or "")
        if pk and rk and token:
            try:
                release_lock(EntradaKey(partition_key=pk, row_key=rk), owner=username, token=token)
            except Exception:
                pass
        session.pop("_lock", None)
        session.pop("_lock_until_ms", None)
        reset_state()
        return redirect(url_for("review"))

    @app.get("/stats")
    def stats():
        if not session.get("authenticated"):
            return redirect(url_for("login"))
        if (session.get("role") or "") != ROLE_ADMIN:
            session["_error"] = "No autorizado: solo Administrador puede ver Estadisticas."
            return redirect(url_for("review"))

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
            app_version=app_version,
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

    @app.get("/stats/listado")
    def stats_listado():
        if not session.get("authenticated"):
            return redirect(url_for("login"))
        if (session.get("role") or "") != ROLE_ADMIN:
            session["_error"] = "No autorizado: solo Administrador puede ver Estadisticas."
            return redirect(url_for("review"))

        def _clean_record(record: dict) -> dict:
            out = {}
            for k, v in (record or {}).items():
                key = str(k or "")
                if key.startswith("Unnamed"):
                    txt = "" if v is None else str(v)
                    if not txt.strip() or txt.strip().lower() == "nan":
                        continue
                out[key] = v
            if "@timestamp" in out:
                out["@timestamp"] = format_ts("" if out["@timestamp"] is None else str(out["@timestamp"]))
            if "Question" in out:
                out["Question"] = normalize_multiline("" if out["Question"] is None else str(out["Question"]))
            return out

        def _parse_internal_note(text: str) -> list[tuple[str, str]] | None:
            raw = (text or "").strip()
            if not raw:
                return None
            if (raw.startswith("{") and raw.endswith("}")) or (raw.startswith("[") and raw.endswith("]")):
                try:
                    obj = json.loads(raw)
                    if isinstance(obj, dict):
                        return [(str(k), "" if v is None else str(v)) for k, v in obj.items()]
                    if isinstance(obj, list):
                        return [(f"Item {i+1}", "" if v is None else str(v)) for i, v in enumerate(obj)]
                except Exception:
                    pass
            lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
            pairs: list[tuple[str, str]] = []
            for ln in lines:
                if ":" in ln:
                    k, v = ln.split(":", 1)
                    pairs.append((k.strip(), v.strip()))
                elif "=" in ln:
                    k, v = ln.split("=", 1)
                    pairs.append((k.strip(), v.strip()))
            return pairs or None

        def _status_options(items: list[dict]) -> list[str]:
            preferred = ["OK", "KO MYM", "KO AGENTE", "DUDA", "FDS", "Pendiente"]
            found = {str((it or {}).get("status", "") or "").strip() for it in (items or [])}
            found = {s for s in found if s}
            order = {s: i for i, s in enumerate(preferred)}
            return sorted(found, key=lambda s: (order.get(s, 10_000), s))

        def _act_params_from_record(record: dict) -> str:
            items = parse_mailtoagent((record or {}).get("MailToAgent", "") or "")
            if not items:
                return ""
            mail_norm = {norm_key(str(k)): v for k, v in (items or [])}
            for nk, val in mail_norm.items():
                if nk.startswith("parametros") and str(val).strip():
                    return str(val).strip()
            return ""

        def _record_items(record: dict) -> list[tuple[str, str]]:
            preferred = [
                "@timestamp",
                "IdCorreo",
                "Subject",
                "From",
                "Question",
                "Location",
                "Sublocation",
                "Automatismo",
                "Validado",
                "Motivo",
                "Comentario",
            ]

            labels = {
                "@timestamp": "Fecha",
                "IdCorreo": "ID Correo",
                "Subject": "Asunto",
                "From": "From",
                "Question": "Correo completo",
                "Location": "Temática",
                "Sublocation": "Subtemática",
                "Accion": "Acción",
                "Acción": "Acción",
                "Ficha": "Ficha",
                "Categorizacion": "Categorización",
                "Categorización": "Categorización",
                "Automatismo": "Automatismo",
                "Validado": "Validado por agente",
                "Motivo": "Motivo",
                "Comentario": "Comentario",
            }

            def norm(v: object) -> str:
                if v is None:
                    return ""
                if isinstance(v, (dict, list)):
                    try:
                        return json.dumps(v, ensure_ascii=False, indent=2)
                    except Exception:
                        return str(v)
                return str(v)

            items: list[tuple[str, str]] = []
            used_labels: set[str] = set()
            for k, v in (record or {}).items():
                key = str(k or "").strip()
                if not key:
                    continue
                value = norm(v)
                if key == "@timestamp":
                    value = format_ts(value)
                if key == "Question":
                    value = normalize_multiline(value)
                value = value.strip()
                if not value or value.lower() == "nan":
                    continue
                label = labels.get(key, key)
                if label in used_labels:
                    label = f"{label} ({key})"
                used_labels.add(label)
                items.append((label, value))

            index = {k: i for i, k in enumerate(preferred)}
            preferred_labels = {k: labels.get(k, k) for k in preferred}
            order = {preferred_labels[k]: i for i, k in enumerate(preferred)}
            items.sort(key=lambda kv: (order.get(kv[0], 10_000), kv[0].lower()))
            return items

        def _group_record_items(items: list[tuple[str, str]]) -> list[dict[str, object]]:
            by_label = {k: v for k, v in items}

            def pick(key: str, title: str, labels: list[str]) -> dict[str, object]:
                out = []
                for lab in labels:
                    if lab in by_label and str(by_label.get(lab, "")).strip():
                        out.append((lab, str(by_label[lab])))
                return {"key": key, "title": title, "items": out}

            used = set()
            groups = [
                pick("mail", "Datos del correo", ["Fecha", "ID Correo", "Asunto", "From", "Correo completo"]),
                pick("act", "Actuación MY", ["Temática", "Subtemática", "Acción", "Automatismo", "Ficha"]),
                pick("agent", "Feedback Agente", ["Validado por agente", "Motivo", "Comentario"]),
            ]
            for g in groups:
                if g.get("key") == "act":
                    g["items"] = [
                        (k, v)
                        for k, v in (g.get("items") or [])
                        if k not in {"Temática", "Subtemática", "Automatismo"}
                    ]
            for g in groups:
                for k, _ in (g.get("items") or []):
                    used.add(k)

            others = [(k, v) for k, v in items if k not in used]
            if others:
                groups.append({"key": "otros", "title": "Otros", "items": others})
            return [g for g in groups if g.get("items")]

        selected_user = (request.args.get("revisor") or "").strip()
        selected_status = (request.args.get("estado") or "").strip()
        selected_id = (request.args.get("idcorreo") or "").strip()
        try:
            rows = list_revisions(username=selected_user, limit=500)
        except Exception as exc:
            return render_template(
                "stats_listado.html",
                title="Listado",
                current_user=session.get("user", ""),
                app_version=app_version,
                error=str(exc),
                users=list_users(),
                selected_user=selected_user,
                statuses=[],
                selected_status=selected_status,
                selected_id=selected_id,
                rows=[],
            )

        def sort_key(it: dict) -> str:
            return str(it.get("timestamp", "") or "")

        rows.sort(key=sort_key, reverse=True)

        statuses = _status_options(rows)
        if selected_status:
            rows = [r for r in rows if str(r.get("status", "") or "").strip() == selected_status]
        if selected_id:
            needle = selected_id.lower()
            rows = [
                r
                for r in rows
                if needle in str(r.get("record_id", "") or "").lower()
                or needle in str((r.get("record") or {}).get("IdCorreo", "") or "").lower()
            ]

        for r in rows:
            r["timestamp"] = format_ts("" if r.get("timestamp") is None else str(r.get("timestamp")))
            record = r.get("record") if isinstance(r.get("record"), dict) else {}
            r["_record_clean"] = _clean_record(record)
            r["_record_items"] = _record_items(r["_record_clean"])
            r["_record_groups"] = _group_record_items(r["_record_items"])
            r["_internal_note_kv"] = _parse_internal_note(str(r.get("internal_note", "") or ""))
            if r["_internal_note_kv"]:
                lines = []
                for k, v in r["_internal_note_kv"]:
                    k = str(k or "").strip()
                    v = str(v or "").strip()
                    if k or v:
                        if k and v:
                            lines.append(f"{k}: {v}")
                        else:
                            lines.append(k or v)
                r["_internal_note_text"] = "\n".join(lines).strip()
            else:
                r["_internal_note_text"] = str(r.get("internal_note", "") or "").strip()
            r["_act_params"] = _act_params_from_record(r["_record_clean"])

        return render_template(
            "stats_listado.html",
            title="Listado",
            current_user=session.get("user", ""),
            app_version=app_version,
            error=None,
            users=list_users(),
            selected_user=selected_user,
            statuses=statuses,
            selected_status=selected_status,
            selected_id=selected_id,
            rows=rows,
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
        ko_mym_reason = request.form.get("ko_mym_reason") or ""
        elapsed_seconds_raw = request.form.get("elapsed_seconds") or ""
        elapsed_seconds = None
        try:
            if elapsed_seconds_raw.strip():
                elapsed_seconds = int(float(elapsed_seconds_raw))
        except Exception:
            elapsed_seconds = None

        if status == "Pendiente" and action_type != "skip":
            log_click(action=action_type or "action", username=username, result="blocked:pendiente")
            session["_error"] = "Selecciona un estado final para continuar."
            return redirect(url_for("review"))

        if action_type in {"save", "skip"}:
            lock = session.get("_lock") or {}
            pk = str(lock.get("pk", "") or "")
            rk = str(lock.get("rk", "") or "")
            token = str(lock.get("token", "") or "")
            if not pk or not rk or not token:
                log_click(action=action_type, username=username, result="blocked:missing_lock")
                session["_error"] = "Sesion caducada. Pulsa refrescar para cargar otro registro."
                return redirect(url_for("review"))

            key = EntradaKey(partition_key=pk, row_key=rk)
            if not validate_lock(key, owner=username, token=token):
                record_id = ""
                try:
                    rec = entrada_get_record(key)
                    record_id = rec.get("IdCorreo", "") or ""
                except Exception:
                    record_id = ""
                log_click(
                    action=action_type,
                    username=username,
                    record_id=record_id,
                    result="blocked:lock_expired_or_taken",
                    extra={"status": status, "elapsed_seconds": elapsed_seconds},
                )
                session.pop("_lock", None)
                try:
                    state.abandon_current()
                except Exception:
                    pass
                session["_error"] = "Sesion caducada (10 min) o registro ya procesado por otro usuario."
                return redirect(url_for("review"))

        record_id = ""
        if action_type in {"save", "skip"}:
            try:
                record_id = entrada_get_record(EntradaKey(partition_key=pk, row_key=rk)).get("IdCorreo", "") or ""
            except Exception:
                record_id = ""

        if action_type == "save":
            if status == "KO MYM" and not ko_mym_reason.strip():
                log_click(
                    action="save",
                    username=username,
                    record_id=record_id,
                    result="blocked:missing_ko_mym_reason",
                    extra={"status": status, "elapsed_seconds": elapsed_seconds},
                )
                session["_error"] = "Para KO MYM selecciona el detalle del KO."
                return redirect(url_for("review"))
            if (status.startswith("KO") or status in {"DUDA", "FDS"}) and not reviewer_note.strip():
                log_click(
                    action="save",
                    username=username,
                    record_id=record_id,
                    result="blocked:missing_comment",
                    extra={"status": status, "elapsed_seconds": elapsed_seconds},
                )
                session["_error"] = "Para un KO, DUDA o FDS es obligatorio indicar un comentario de revision."
                return redirect(url_for("review"))

            key = EntradaKey(partition_key=pk, row_key=rk)
            record = entrada_get_record(key)
            multitematica = (request.form.get("multitematica") or "").strip() in {"1", "on", "true", "True"}
            write_resultado(
                username=username,
                record=record,
                status=status,
                ko_mym_reason=ko_mym_reason,
                reviewer_note=reviewer_note,
                internal_note=internal_note,
                multitematica=multitematica,
            )
            entrada_delete_record(key)
            session.pop("_lock", None)
            reset_state()
            log_click(
                action="save",
                username=username,
                record_id=record_id,
                result="ok",
                extra={"status": status, "elapsed_seconds": elapsed_seconds, "ko_mym_reason": ko_mym_reason},
            )
            return redirect(url_for("review"))

        if action_type == "skip":
            key = EntradaKey(partition_key=pk, row_key=rk)
            record = entrada_get_record(key)
            write_descarte(username=username, record=record)
            entrada_delete_record(key)
            session.pop("_lock", None)
            reset_state()
            log_click(
                action="skip",
                username=username,
                record_id=record_id,
                result="ok",
                extra={"status": status, "elapsed_seconds": elapsed_seconds},
            )
            return redirect(url_for("review"))

        log_click(action="action", username=username, record_id=record_id, result="fail:unknown_action")
        session["_error"] = "Accion no valida."
        return redirect(url_for("review"))

    @app.post("/heartbeat")
    def heartbeat():
        if not session.get("authenticated"):
            return ("", 401)
        username = session.get("user", "")
        lock = session.get("_lock") or {}
        pk = str(lock.get("pk", "") or "")
        rk = str(lock.get("rk", "") or "")
        token = str(lock.get("token", "") or "")
        if not pk or not rk or not token:
            return ("", 204)

        new_until = refresh_lock(EntradaKey(partition_key=pk, row_key=rk), owner=username, token=token)
        if not new_until:
            session.pop("_lock", None)
            session.pop("_lock_until_ms", None)
            return ("", 409)

        lock_until_ms = int(new_until.timestamp() * 1000)
        session["_lock_until_ms"] = lock_until_ms
        return jsonify({"lock_until_ms": lock_until_ms})

    return app


app = create_app()

if __name__ == "__main__":
    host = (os.environ.get("FLASK_HOST", "") or "").strip() or "0.0.0.0"
    port = int(os.environ.get("PORT", "8000"))
    debug = (os.environ.get("FLASK_DEBUG", "") or "1").strip() not in {"0", "false", "False"}
    app.run(host=host, port=port, debug=debug)
