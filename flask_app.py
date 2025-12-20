from __future__ import annotations

import json
import os
import re
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from io import BytesIO, StringIO
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import csv

from flask import Flask, jsonify, redirect, render_template, request, session, url_for, send_file

import config
from mymail.entrada import EntradaKey, clear_expired_locks, refresh_lock, release_lock, validate_lock
from mymail.entrada import get_record as entrada_get_record
from mymail.entrada import delete_record as entrada_delete_record
from mymail.entrada import list_pending_meta
from mymail.entrada import list_pending_payloads_for_stats, record_from_payload
from mymail.state import get_state, reset_state
from mymail.revisiones import get_revision, list_revisions, save_revision
from mymail.tables import ROLE_ADMIN, create_user, list_users, log_click, set_user_password, set_user_role, verify_user
from mymail.tables import _list_by_day_range, _list_by_days
from mymail.tables import write_descarte, write_resultado


def create_app() -> Flask:
    app = Flask(__name__, static_folder="static", template_folder="templates")
    app_version = (getattr(config, "APP_VERSION", "") or "").strip() or "0.0.0"
    app.secret_key = (
        (getattr(config, "FLASK_SECRET_KEY", "") or "").strip()
        or (os.environ.get("FLASK_SECRET_KEY", "") or "").strip()
        or "dev-secret-change-me"
    )

    # Cosmos-only: no hacemos "ensure" automático (puede bloquear si no hay permisos de creación).

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

    def version_at_least(current: str, minimum: str) -> bool:
        def parse(v: str) -> tuple[int, int, int]:
            parts = (v or "").strip().split(".")
            nums = []
            for p in parts[:3]:
                try:
                    nums.append(int(re.sub(r"[^0-9].*$", "", p) or "0"))
                except Exception:
                    nums.append(0)
            while len(nums) < 3:
                nums.append(0)
            return tuple(nums)  # type: ignore[return-value]

        return parse(current) >= parse(minimum)

    _CACHE = {}

    def _cache_get(key: str, *, ttl_seconds: int):
        try:
            item = _CACHE.get(key)
            if not item:
                return None
            if (datetime.now(timezone.utc) - item["ts"]).total_seconds() > ttl_seconds:
                _CACHE.pop(key, None)
                return None
            return item["value"]
        except Exception:
            return None

    def _cache_set(key: str, value, *, ttl_seconds: int):
        try:
            _CACHE[key] = {"ts": datetime.now(timezone.utc), "value": value, "ttl": int(ttl_seconds)}
        except Exception:
            pass
        return value

    def azure_openai_responses(messages: list[dict], *, temperature: float = 0.2, max_output_tokens: int = 350) -> str:
        endpoint_raw = (getattr(config, "AZURE_OPENAI_ENDPOINT", "") or "").strip()
        api_key = (getattr(config, "AZURE_OPENAI_API_KEY", "") or "").strip()
        deployment = (getattr(config, "AZURE_OPENAI_DEPLOYMENT", "") or "").strip()
        api_version = (getattr(config, "AZURE_OPENAI_API_VERSION", "") or "").strip() or "2024-02-15-preview"
        if not endpoint_raw or not api_key or not deployment:
            raise RuntimeError("Faltan credenciales de Azure OpenAI (endpoint/api_key/deployment) en config.py o entorno.")

        endpoint = endpoint_raw.rstrip("/")
        for suffix in ("/openai/v1/responses", "/openai/v1/responses/", "/openai/v1", "/openai", "/openai/v1/"):
            if endpoint.lower().endswith(suffix):
                endpoint = endpoint[: -len(suffix)].rstrip("/")

        q = urllib.parse.urlencode({"api-version": api_version})
        url = f"{endpoint}/openai/deployments/{urllib.parse.quote(deployment)}/chat/completions?{q}"
        payload = {
            "messages": messages,
            "max_completion_tokens": int(max_output_tokens),
        }
        if (deployment or "").strip().lower().startswith("gpt-5"):
            payload["reasoning_effort"] = "minimal"
        if float(temperature) == 1.0:
            payload["temperature"] = 1.0
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "api-key": api_key,
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            msg = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else str(exc)
            raise RuntimeError(f"Azure OpenAI error: {exc.code} {msg}") from exc
        except Exception as exc:
            raise RuntimeError(f"No se pudo conectar con Azure OpenAI: {exc}") from exc

        try:
            obj = json.loads(body)
            choices = obj.get("choices") or []
            if isinstance(choices, list) and choices:
                content = (((choices[0] or {}).get("message") or {}).get("content") or "").strip()
                if content:
                    return str(content)
            raise RuntimeError("Respuesta vacía de Azure OpenAI.")
        except Exception as exc:
            raise RuntimeError(f"Respuesta inválida de Azure OpenAI: {body[:500]}") from exc

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

    def format_ts_madrid(value: str) -> str:
        if not value:
            return ""
        try:
            v = value.strip()
            if v.endswith("Z"):
                v = v[:-1] + "+00:00"
            dt = datetime.fromisoformat(v)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dt = dt.astimezone(ZoneInfo("Europe/Madrid"))
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
            reset_state()
            session.clear()
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

    @app.get("/admin")
    def admin_menu():
        if not session.get("authenticated"):
            return redirect(url_for("login"))
        if (session.get("role") or "") != ROLE_ADMIN:
            session["_error"] = "No autorizado: solo Administrador puede gestionar usuarios."
            return redirect(url_for("menu"))

        msg = (request.args.get("msg") or "").strip()
        return render_template(
            "admin.html",
            title="Administrador",
            current_user=session.get("user", ""),
            app_version=app_version,
            error=session.pop("_error", None),
            message=msg,
            users=list_users(),
        )

    @app.post("/admin/users")
    def admin_users_post():
        if not session.get("authenticated"):
            return redirect(url_for("login"))
        if (session.get("role") or "") != ROLE_ADMIN:
            session["_error"] = "No autorizado: solo Administrador puede gestionar usuarios."
            return redirect(url_for("menu"))

        action_type = (request.form.get("action") or "").strip()
        username = (request.form.get("username") or "").strip()

        try:
            if action_type == "add":
                password = request.form.get("password") or ""
                role = (request.form.get("role") or "").strip() or "Revisor"
                create_user(username=username, password=password, role=role)
                log_click(action="admin_user_add", username=session.get("user", ""), record_id=username, result="ok")
                return redirect(url_for("admin_menu", msg=f"Usuario '{username}' creado/actualizado."))

            if action_type == "set_role":
                role = (request.form.get("role") or "").strip() or "Revisor"
                set_user_role(username=username, role=role)
                log_click(action="admin_user_role", username=session.get("user", ""), record_id=username, result="ok", extra={"role": role})
                return redirect(url_for("admin_menu", msg=f"Rol actualizado para '{username}'."))

            if action_type == "set_password":
                password = request.form.get("password") or ""
                set_user_password(username=username, password=password)
                log_click(action="admin_user_password", username=session.get("user", ""), record_id=username, result="ok")
                return redirect(url_for("admin_menu", msg=f"Contraseña actualizada para '{username}'."))
        except Exception as exc:
            session["_error"] = str(exc)
            log_click(action="admin_user", username=session.get("user", ""), record_id=username, result="fail", extra={"action": action_type, "error": str(exc)[:200]})
            return redirect(url_for("admin_menu"))

        session["_error"] = "Acción no válida."
        return redirect(url_for("admin_menu"))

    @app.get("/review")
    def review():
        if not session.get("authenticated"):
            return redirect(url_for("login"))

        state = get_state()

        username = session.get("user", "")
        error = session.pop("_error", None)
        lock_until_ms = session.get("_lock_until_ms")
        try:
            record = state.current_record(owner=username)
        except TimeoutError as exc:
            return render_template("done.html", message=str(exc))
        except Exception as exc:
            return render_template("done.html", message=f"Error cargando un registro desde CosmosDB: {exc}")
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
            can_ai=version_at_least(app_version, "1.0.0") and (session.get("role") or "") == ROLE_ADMIN,
            app_version=app_version,
            title="Revisor de Mayordomo Mail",
            error=error,
            lock_until_ms=lock_until_ms,
        )

    @app.post("/ai/tematica")
    def ai_tematica():
        if not session.get("authenticated"):
            return jsonify({"ok": False, "error": "No autenticado."}), 401
        if not version_at_least(app_version, "1.0.0"):
            return jsonify({"ok": False, "error": "Funcionalidad no disponible para esta versión."}), 404

        if (session.get("role") or "") != ROLE_ADMIN:
            return jsonify({"ok": False, "error": "No autorizado: solo Administrador."}), 403

        state = get_state()
        username = session.get("user", "")
        try:
            record = state.current_record(owner=username)
        except TimeoutError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 409
        if not record:
            return jsonify({"ok": False, "error": "No hay correo activo para analizar."}), 409

        try:
            from helpers.prompts import build_tematica_messages
        except Exception as exc:
            return jsonify({"ok": False, "error": f"No se pudo cargar prompts: {exc}"}), 500

        subject = str(record.get("Subject", "") or "")
        body_text = normalize_multiline(str(record.get("Question", "") or ""))

        items = parse_mailtoagent(str(record.get("MailToAgent", "") or ""))
        mail_norm = {norm_key(str(k)): v for k, v in (items or [])} if items else {}
        from_ = str(mail_norm.get("from") or mail_norm.get("remitente") or "")
        provided_intent = str(mail_norm.get("intencion") or mail_norm.get("intención") or "")
        provided_summary = str(mail_norm.get("resumen") or "")

        messages = build_tematica_messages(
            subject=subject,
            from_=from_,
            body=body_text,
            provided_intent=provided_intent,
            provided_summary=provided_summary,
            theme_catalog=[],
        )
        try:
            suggestion = azure_openai_responses(messages, temperature=0.2, max_output_tokens=600)
            return jsonify({"ok": True, "suggestion": suggestion})
        except Exception as exc:
            return jsonify({"ok": False, "error": str(exc)}), 500

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

    @app.get("/pendientes")
    def pendientes():
        if not session.get("authenticated"):
            return redirect(url_for("login"))

        selected_id = (request.args.get("idcorreo") or "").strip()
        selected_tematica = (request.args.get("tematica") or "").strip()
        selected_subtematica = (request.args.get("subtematica") or "").strip()
        selected_automatismo = (request.args.get("automatismo") or "").strip()
        selected_validado = (request.args.get("validado") or "").strip()
        selected_motivo = (request.args.get("motivo") or "").strip()
        selected_comentario = (request.args.get("comentario") or "").strip()

        page = 1
        per_page = 10
        try:
            page = int(str(request.args.get("page") or page).strip())
        except Exception:
            page = 1
        try:
            per_page = int(str(request.args.get("per_page") or per_page).strip())
        except Exception:
            per_page = 10
        page = max(1, page)
        per_page = max(1, min(100, per_page))

        def contains(value: str, needle: str) -> bool:
            if not needle:
                return True
            return needle.lower() in (value or "").lower()

        try:
            metas = list_pending_meta(limit=5000)
        except Exception as exc:
            return render_template(
                "done.html",
                message=f"No se pudo cargar la lista de pendientes (Cosmos container 'entrada'): {exc}",
            )

        if selected_id:
            metas = [m for m in metas if contains(m.get("record_id", ""), selected_id)]
        if selected_automatismo:
            metas = [m for m in metas if contains(m.get("automatismo", ""), selected_automatismo)]

        requires_full = any(
            bool(v)
            for v in (
                selected_tematica,
                selected_subtematica,
                selected_validado,
                selected_motivo,
                selected_comentario,
            )
        )

        def meta_sort_key(m: dict) -> str:
            return str(m.get("timestamp", "") or "")

        metas.sort(key=meta_sort_key, reverse=True)

        rows: list[dict] = []
        if requires_full:
            for ent in entities:
                try:
                    rec = record_from_payload(
                        record_json=str(ent.get("record_json", "") or ""),
                        record_blob=str(ent.get("record_blob", "") or ""),
                    )
                except Exception:
                    continue

                if selected_tematica and not contains(rec.get("Location", ""), selected_tematica):
                    continue
                if selected_subtematica and not contains(rec.get("Sublocation", ""), selected_subtematica):
                    continue
                if selected_validado and not contains(rec.get("Validado", ""), selected_validado):
                    continue
                if selected_motivo and not contains(rec.get("Motivo", ""), selected_motivo):
                    continue
                if selected_comentario and not contains(rec.get("Comentario", ""), selected_comentario):
                    continue

                rows.append({"meta": m, "record": rec})
        else:
            rows = [{"meta": m} for m in metas]

        total = len(rows)
        total_pages = max(1, (total + per_page - 1) // per_page)
        if page > total_pages:
            page = total_pages

        start = (page - 1) * per_page
        end = start + per_page
        rows_page = rows[start:end]

        if not requires_full:
            for r in rows_page:
                m = r.get("meta") if isinstance(r.get("meta"), dict) else {}
                pk = str(m.get("pk", "") or "")
                rk = str(m.get("rk", "") or "")
                if not pk or not rk:
                    r["record"] = {}
                    continue
                try:
                    r["record"] = entrada_get_record(EntradaKey(partition_key=pk, row_key=rk))
                except Exception:
                    r["record"] = {}

        for r in rows_page:
            m = r.get("meta") if isinstance(r.get("meta"), dict) else {}
            rec = r.get("record") if isinstance(r.get("record"), dict) else {}
            r["pk"] = str(m.get("pk", "") or "")
            r["rk"] = str(m.get("rk", "") or "")
            r["record_id"] = str(m.get("record_id", "") or "") or str(rec.get("IdCorreo", "") or "")
            r["timestamp"] = format_ts(str(m.get("timestamp", "") or "") or str(rec.get("@timestamp", "") or ""))
            r["automatismo"] = str(m.get("automatismo", "") or "") or str(rec.get("Automatismo", "") or "")
            r["tematica"] = str(rec.get("Location", "") or "")
            r["subtematica"] = str(rec.get("Sublocation", "") or "")
            r["validado"] = str(rec.get("Validado", "") or "")
            r["motivo"] = str(rec.get("Motivo", "") or "")
            r["comentario"] = str(rec.get("Comentario", "") or "")
            r["lock_owner"] = str(m.get("lock_owner", "") or "")
            r["lock_until"] = format_ts(str(m.get("lock_until", "") or ""))

        return render_template(
            "pendientes.html",
            title="Pendientes",
            current_user=session.get("user", ""),
            app_version=app_version,
            error=session.pop("_error", None),
            can_stats=(session.get("role") or "") == ROLE_ADMIN,
            selected_id=selected_id,
            selected_tematica=selected_tematica,
            selected_subtematica=selected_subtematica,
            selected_automatismo=selected_automatismo,
            selected_validado=selected_validado,
            selected_motivo=selected_motivo,
            selected_comentario=selected_comentario,
            per_page=per_page,
            page=page,
            total=total,
            total_pages=total_pages,
            rows=rows_page,
        )

    @app.get("/pendientes/abrir")
    def pendientes_abrir():
        if not session.get("authenticated"):
            return redirect(url_for("login"))

        pk = (request.args.get("pk") or "").strip()
        rk = (request.args.get("rk") or "").strip()
        if not pk or not rk:
            session["_error"] = "Faltan parámetros para abrir el registro (pk/rk)."
            return redirect(url_for("pendientes"))

        state = get_state()
        username = session.get("user", "")
        ok = False
        try:
            ok = state.select_specific(EntradaKey(partition_key=pk, row_key=rk), owner=username)
        except Exception:
            ok = False

        if not ok:
            session["_error"] = "No se pudo abrir el registro: puede estar bloqueado por otro usuario o no disponible."
            return redirect(url_for("pendientes"))

        try:
            if state.current_key and state.lock_token:
                session["_lock"] = {"pk": state.current_key.partition_key, "rk": state.current_key.row_key, "token": state.lock_token}
                new_until = refresh_lock(state.current_key, owner=username, token=state.lock_token)
                if new_until:
                    session["_lock_until_ms"] = int(new_until.timestamp() * 1000)
        except Exception:
            pass

        return redirect(url_for("review"))

    @app.get("/stats")
    def stats():
        if not session.get("authenticated"):
            return redirect(url_for("login"))
        if (session.get("role") or "") != ROLE_ADMIN:
            session["_error"] = "No autorizado: solo Administrador puede ver Estadisticas."
            return redirect(url_for("review"))

        days = 14
        try:
            days = int(str(request.args.get("days") or days).strip())
        except Exception:
            days = 14
        days = max(1, min(90, days))
        return render_template(
            "stats.html",
            title="Stats",
            current_user=session.get("user", ""),
            app_version=app_version,
            error=None,
            days=days,
            total_resultados=0,
            total_descartes=0,
            ko_rate="—",
            duda_count=0,
            by_status=[],
            by_user=[],
            by_day=[],
            top_automatismos=[],
            pending_error=None,
            pending_total=0,
            pending_loaded=0,
            pending_by_tematica=[],
            pending_top_tematica=[],
            pending_top_matriculas=[],
            pending_ranking_motivo=[],
            stats_deferred=True,
        )

    @app.get("/stats/my")
    def stats_my():
        if not session.get("authenticated"):
            return redirect(url_for("login"))
        if (session.get("role") or "") != ROLE_ADMIN:
            session["_error"] = "No autorizado: solo Administrador puede ver Estadísticas MY."
            return redirect(url_for("review"))

        selected_week_start = (request.args.get("week_start") or "").strip()

        week_options = []
        try:
            metas = list_pending_meta(limit=5000)
            week_starts = set()
            tz = ZoneInfo("Europe/Madrid")
            for m in metas:
                ts = str((m or {}).get("timestamp", "") or "").strip()
                if not ts:
                    continue
                try:
                    v = ts
                    if v.endswith("Z"):
                        v = v[:-1] + "+00:00"
                    dt = datetime.fromisoformat(v)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    dt = dt.astimezone(tz)
                    ws = (dt.date() - timedelta(days=dt.weekday()))
                    week_starts.add(ws)
                except Exception:
                    continue

            for ws in sorted(week_starts, reverse=True):
                we = ws + timedelta(days=6)
                week_no = ws.isocalendar().week
                week_options.append(
                    {
                        "value": ws.isoformat(),
                        "label": f"SEMANA{week_no}: {ws.strftime('%d/%m/%Y')} - {we.strftime('%d/%m/%Y')}",
                    }
                )
        except Exception:
            week_options = []

        return render_template(
            "stats_my.html",
            title="Estadísticas MY",
            current_user=session.get("user", ""),
            app_version=app_version,
            error=session.pop("_error", None),
            week_options=week_options,
            selected_week_start=selected_week_start,
        )

    @app.get("/api/stats/revisiones")
    def api_stats_revisiones():
        if not session.get("authenticated"):
            return jsonify({"ok": False, "error": "No autenticado."}), 401
        if (session.get("role") or "") != ROLE_ADMIN:
            return jsonify({"ok": False, "error": "No autorizado."}), 403

        days = 14
        try:
            days = int(str(request.args.get("days") or days).strip())
        except Exception:
            days = 14
        days = max(1, min(90, days))

        cache_key = f"stats:rev:{days}"
        cached = _cache_get(cache_key, ttl_seconds=60)
        if cached is not None:
            return jsonify({"ok": True, "data": cached})

        def count_by(items, key, *, skip_empty: bool = False):
            out = {}
            for it in items:
                val = str(it.get(key, "") or "")
                if skip_empty and not val.strip():
                    continue
                out[val] = out.get(val, 0) + 1
            return sorted(out.items(), key=lambda kv: kv[1], reverse=True)

        def with_pct(items: list[tuple[str, int]], *, total: int) -> list[tuple[str, int, str]]:
            out = []
            for k, v in items:
                pct = f"{round((v / total) * 100)}%" if total else "0%"
                out.append((k, v, pct))
            return out

        now = datetime.now(timezone.utc)
        start_day = (now - timedelta(days=days - 1)).strftime("%Y%m%d")
        end_day = now.strftime("%Y%m%d")

        try:
            resultados = _list_by_day_range(
                getattr(config, "COSMOS_CONTAINER_RESULTADOS", "resultados"), start_day=start_day, end_day=end_day
            )
            descartes = _list_by_day_range(
                getattr(config, "COSMOS_CONTAINER_DESCARTES", "descartes"), start_day=start_day, end_day=end_day
            )
        except Exception as exc:
            return jsonify({"ok": False, "error": str(exc)}), 500

        total_resultados = len(resultados)
        total_descartes = len(descartes)
        by_status_raw = count_by(resultados, "status", skip_empty=True)
        top_automatismos_raw = count_by(resultados, "automatismo", skip_empty=True)[:10]
        by_user = count_by(resultados, "user", skip_empty=True)
        by_day = count_by(resultados, "day", skip_empty=True)

        by_status = with_pct(by_status_raw, total=total_resultados)[:10]
        top_automatismos = with_pct(top_automatismos_raw, total=total_resultados)

        ko_count = sum(v for k, v in by_status_raw if k.startswith("KO"))
        duda_count = sum(v for k, v in by_status_raw if k == "DUDA")
        ko_rate = f"{round((ko_count / total_resultados) * 100)}%" if total_resultados else "0%"

        data = {
            "total_resultados": total_resultados,
            "total_descartes": total_descartes,
            "ko_rate": ko_rate,
            "duda_count": duda_count,
            "by_status": by_status,
            "top_automatismos": top_automatismos,
            "by_user": by_user,
            "by_day": by_day,
        }
        _cache_set(cache_key, data, ttl_seconds=60)
        return jsonify({"ok": True, "data": data})

    @app.get("/api/stats/pendientes")
    def api_stats_pendientes():
        if not session.get("authenticated"):
            return jsonify({"ok": False, "error": "No autenticado."}), 401
        if (session.get("role") or "") != ROLE_ADMIN:
            return jsonify({"ok": False, "error": "No autorizado."}), 403

        week_start = (request.args.get("week_start") or "").strip()
        cache_key = f"stats:pendientes:{week_start or 'all'}"
        cached = _cache_get(cache_key, ttl_seconds=60)
        if cached is not None:
            return jsonify({"ok": True, "data": cached})

        def count_by(items, key, *, skip_empty: bool = False):
            out = {}
            for it in items:
                val = str(it.get(key, "") or "")
                if skip_empty and not val.strip():
                    continue
                out[val] = out.get(val, 0) + 1
            return sorted(out.items(), key=lambda kv: kv[1], reverse=True)

        def with_pct(items: list[tuple[str, int]], *, total: int) -> list[tuple[str, int, str]]:
            out = []
            for k, v in items:
                pct = f"{round((v / total) * 100)}%" if total else "0%"
                out.append((k, v, pct))
            return out

        def pending_matricula(record: dict) -> str:
            direct_keys = [
                "MatriculaAsesor",
                "MatrículaAsesor",
                "matriculaAsesor",
                "matrículaAsesor",
                "Matricula",
                "Matrícula",
                "matricula",
                "matrícula",
                "Ficha",
                "Ficha cliente",
                "Ficha Cliente",
                "IdEmpleado",
                "EmployeeId",
            ]
            for k in direct_keys:
                v = str((record or {}).get(k, "") or "").strip()
                if v:
                    return v

            items = parse_mailtoagent((record or {}).get("MailToAgent", "") or "")
            if not items:
                return ""
            mail_norm = {norm_key(str(k)): v for k, v in items}
            for k in ("matricula asesor", "matrícula asesor", "matricula", "matrícula", "ficha", "ficha cliente"):
                v = str(mail_norm.get(k, "") or "").strip()
                if v:
                    return v
            return ""

        pending_items_cache = _cache_get("pending_items:v1", ttl_seconds=60)
        if pending_items_cache is None:
            try:
                entities = list_pending_payloads_for_stats(limit=5000)
            except Exception as exc:
                return jsonify({"ok": False, "error": str(exc)}), 500
            pending_total = len(entities)
            pending_loaded = 0
            pending_items_all: list[dict] = []
            for ent in entities:
                try:
                    rec = record_from_payload(
                        record_json=str(ent.get("record_json", "") or ""),
                        record_blob=str(ent.get("record_blob", "") or ""),
                    )
                except Exception:
                    continue
                pending_loaded += 1
                tematica = str(rec.get("Location", "") or "").strip() or "—"
                motivo = str(rec.get("Motivo", "") or "").strip() or "—"
                matricula = pending_matricula(rec).strip() or "—"
                ts = str(ent.get("timestamp", "") or "").strip() or str(rec.get("@timestamp", "") or "").strip()
                pending_items_all.append({"tematica": tematica, "motivo": motivo, "matricula": matricula, "timestamp": ts})
            pending_items_cache = _cache_set(
                "pending_items:v1",
                {"pending_total": pending_total, "pending_loaded": pending_loaded, "items": pending_items_all},
                ttl_seconds=60,
            )

        pending_total = int((pending_items_cache or {}).get("pending_total") or 0)
        pending_loaded = int((pending_items_cache or {}).get("pending_loaded") or 0)
        pending_items = list((pending_items_cache or {}).get("items") or [])

        if week_start:
            try:
                tz = ZoneInfo("Europe/Madrid")
                ws = datetime.fromisoformat(week_start).date()
                start_local = datetime(ws.year, ws.month, ws.day, tzinfo=tz)
                end_local = start_local + timedelta(days=7)
                start_dt = start_local.astimezone(timezone.utc)
                end_dt = end_local.astimezone(timezone.utc)
            except Exception:
                start_dt = None
                end_dt = None
            if start_dt and end_dt:
                def _in_week(ts_raw: str) -> bool:
                    try:
                        v = (ts_raw or "").strip()
                        if v.endswith("Z"):
                            v = v[:-1] + "+00:00"
                        dt = datetime.fromisoformat(v)
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        dt = dt.astimezone(timezone.utc)
                        return start_dt <= dt < end_dt
                    except Exception:
                        return False

                pending_items = [it for it in pending_items if _in_week(str(it.get("timestamp", "") or ""))]
                pending_loaded = len(pending_items)

        pending_by_tematica_raw = count_by(pending_items, "tematica", skip_empty=False)
        pending_by_tematica = with_pct(pending_by_tematica_raw, total=pending_loaded)
        pending_top_matriculas_raw = count_by(pending_items, "matricula", skip_empty=True)[:10]
        pending_top_matriculas = with_pct(pending_top_matriculas_raw, total=pending_loaded)

        pending_motivo_raw = count_by(pending_items, "motivo", skip_empty=True)[:20]
        pending_ranking_motivo = with_pct(pending_motivo_raw, total=pending_loaded)

        data = {
            "pending_total": pending_total,
            "pending_loaded": pending_loaded,
            "pending_by_tematica": pending_by_tematica,
            "pending_top_matriculas": pending_top_matriculas,
            "pending_ranking_motivo": pending_ranking_motivo,
        }
        _cache_set(cache_key, data, ttl_seconds=60)
        return jsonify({"ok": True, "data": data})

    @app.get("/listado")
    def listado():
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

        def _act_summary_proposal_from_record(record: dict) -> tuple[str, str]:
            items = parse_mailtoagent((record or {}).get("MailToAgent", "") or "")
            if not items:
                return "", ""
            mail_norm = {norm_key(str(k)): v for k, v in (items or [])}

            def first_by_prefix(*prefixes: str) -> str:
                for prefix in prefixes:
                    for nk, val in mail_norm.items():
                        if nk.startswith(prefix) and str(val).strip():
                            return str(val).strip()
                return ""

            summary = first_by_prefix("resumen")
            proposal = first_by_prefix("propuesta de actuacion", "propuesta actuacion")
            if not proposal:
                for nk, val in mail_norm.items():
                    if nk.startswith("propuesta") and nk != "propuesta respuesta" and str(val).strip():
                        proposal = str(val).strip()
                        break
            return summary, proposal

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
        per_page = (request.args.get("per_page") or "").strip()
        page = (request.args.get("page") or "").strip()
        try:
            per_page_i = int(per_page) if per_page else 10
        except Exception:
            per_page_i = 10
        if per_page_i not in {10, 25, 50}:
            per_page_i = 10
        try:
            page_i = int(page) if page else 1
        except Exception:
            page_i = 1
        page_i = max(1, page_i)
        try:
            rows = list_revisions(username=selected_user, limit=5000)
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
                per_page=per_page_i,
                page=page_i,
                total=0,
                total_pages=1,
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

        total = len(rows)
        total_pages = max(1, (total + per_page_i - 1) // per_page_i)
        page_i = min(page_i, total_pages)
        start = (page_i - 1) * per_page_i
        end = start + per_page_i
        rows_page = rows[start:end]

        for r in rows_page:
            r["timestamp"] = format_ts_madrid("" if r.get("timestamp") is None else str(r.get("timestamp")))
            record = r.get("record") if isinstance(r.get("record"), dict) else {}
            r["_record_clean"] = _clean_record(record)
            r["_record_items"] = _record_items(r["_record_clean"])
            r["_record_groups"] = _group_record_items(r["_record_items"])
            r["_otros_items"] = []
            for g in r["_record_groups"]:
                if str(g.get("key", "") or "") == "otros":
                    r["_otros_items"] = list(g.get("items") or [])
                    break
            history = r.get("history") if isinstance(r.get("history"), list) else []
            history_rows: list[tuple[str, str, str]] = []
            for h in history:
                if not isinstance(h, dict):
                    continue
                ts = format_ts_madrid(str(h.get("timestamp", "") or ""))
                user = str(h.get("user", "") or "") or str(h.get("edited_by", "") or "") or "—"
                changes = h.get("changes") if isinstance(h.get("changes"), dict) else {}
                parts = []
                for k, v in changes.items():
                    if not isinstance(v, dict):
                        continue
                    from_ = v.get("from")
                    to_ = v.get("to")
                    if from_ == to_:
                        continue
                    parts.append(f"{k}: {from_} → {to_}")
                summary = "; ".join(parts)[:180] if parts else (str(h.get("action", "") or "") or "—")
                history_rows.append((ts or "—", user, summary))
            r["_history_rows"] = history_rows
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
            summary, proposal = _act_summary_proposal_from_record(r["_record_clean"])
            r["_act_summary"] = summary
            r["_act_proposal"] = proposal

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
            per_page=per_page_i,
            page=page_i,
            total=total,
            total_pages=total_pages,
            rows=rows_page,
        )

    @app.get("/listado/download")
    def listado_download():
        if not session.get("authenticated"):
            return redirect(url_for("login"))
        if (session.get("role") or "") != ROLE_ADMIN:
            session["_error"] = "No autorizado: solo Administrador puede ver Estadisticas."
            return redirect(url_for("review"))

        selected_user = (request.args.get("revisor") or "").strip()
        selected_status = (request.args.get("estado") or "").strip()
        selected_id = (request.args.get("idcorreo") or "").strip()
        fmt = (request.args.get("format") or "csv").strip().lower()
        if fmt not in {"csv", "xlsx"}:
            fmt = "csv"

        rows = list_revisions(username=selected_user, limit=5000)
        rows.sort(key=lambda it: str(it.get("timestamp", "") or ""), reverse=True)
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

        def row_to_dict(r: dict) -> dict:
            rec = r.get("record") if isinstance(r.get("record"), dict) else {}
            return {
                "fecha_revision_madrid": format_ts_madrid("" if r.get("timestamp") is None else str(r.get("timestamp"))),
                "revisor": str(r.get("user", "") or ""),
                "id_correo": str(r.get("record_id", "") or "") or str(rec.get("IdCorreo", "") or ""),
                "estado": str(r.get("status", "") or ""),
                "automatismo": str(r.get("automatismo", "") or "") or str(rec.get("Automatismo", "") or ""),
                "multitematica": "Sí" if bool(r.get("multitematica")) else "No",
                "detalle_ko_mtm": str(r.get("ko_mym_reason", "") or ""),
                "comentario_revision": str(r.get("reviewer_note", "") or ""),
                "nota_interna": str(r.get("internal_note", "") or ""),
                "fecha_correo": format_ts("" if rec.get("@timestamp") is None else str(rec.get("@timestamp"))),
                "asunto": str(rec.get("Subject", "") or ""),
                "tematica": str(rec.get("Location", "") or ""),
                "subtematica": str(rec.get("Sublocation", "") or ""),
            }

        data_rows = [row_to_dict(r) for r in rows]
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        base = f"listado_{stamp}"
        if fmt == "csv":
            buf = StringIO()
            fieldnames = list(data_rows[0].keys()) if data_rows else [
                "fecha_revision_madrid",
                "revisor",
                "id_correo",
                "estado",
                "automatismo",
                "multitematica",
                "detalle_ko_mtm",
                "comentario_revision",
                "nota_interna",
                "fecha_correo",
                "asunto",
                "tematica",
                "subtematica",
            ]
            w = csv.DictWriter(buf, fieldnames=fieldnames)
            w.writeheader()
            for it in data_rows:
                w.writerow(it)
            out = BytesIO(buf.getvalue().encode("utf-8"))
            out.seek(0)
            return send_file(out, as_attachment=True, download_name=f"{base}.csv", mimetype="text/csv; charset=utf-8")

        try:
            import pandas as pd
        except Exception as exc:
            raise RuntimeError("Falta pandas para exportar Excel (pip install -r requirements.txt)") from exc

        df = pd.DataFrame(data_rows)
        out = BytesIO()
        df.to_excel(out, index=False)
        out.seek(0)
        return send_file(
            out,
            as_attachment=True,
            download_name=f"{base}.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    @app.get("/stats/listado")
    def stats_listado_redirect():
        return redirect(url_for("listado", **request.args))

    @app.get("/stats/listado/download")
    def stats_listado_download_redirect():
        return redirect(url_for("listado_download", **request.args))

    @app.get("/listado/editar")
    def listado_editar():
        if not session.get("authenticated"):
            return redirect(url_for("login"))
        if (session.get("role") or "") != ROLE_ADMIN:
            session["_error"] = "No autorizado: solo Administrador puede editar revisiones."
            return redirect(url_for("review"))

        blob = (request.args.get("blob") or "").strip()
        next_url = (request.args.get("next") or "").strip()
        if not blob:
            session["_error"] = "Falta el identificador de la revisión (blob)."
            return redirect(url_for("listado"))

        try:
            rev = get_revision(blob)
        except Exception as exc:
            session["_error"] = f"No se pudo cargar la revisión: {exc}"
            return redirect(url_for("listado"))

        record = rev.get("record") if isinstance(rev.get("record"), dict) else {}
        record = {k: ("" if v is None else str(v)) for k, v in record.items()}
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
            "Categorizaci\u00f3n": get_mail("categorizacion", "categoria", "categoria final"),
            "Acci\u00f3n": get_mail("accion", "accion final"),
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

        history = rev.get("history") if isinstance(rev.get("history"), list) else []

        return render_template(
            "review.html",
            record=record,
            mail_meta=mail_meta,
            act_summary=act_summary,
            act_proposal=act_proposal,
            act_params=act_params,
            act_items=act_items,
            pending=0,
            current_user=session.get("user", ""),
            can_stats=True,
            can_ai=False,
            app_version=app_version,
            title="Editar revisión",
            error=session.pop("_error", None),
            lock_until_ms="",
            edit_mode=True,
            form_action=url_for("listado_editar_post"),
            back_url=next_url or url_for("listado"),
            edit_blob=blob,
            edit_original_user=str(rev.get("user", "") or ""),
            history=history,
            initial_status=str(rev.get("status", "") or "Pendiente"),
            initial_multitematica=bool(rev.get("multitematica")),
            initial_ko_mym_reason=str(rev.get("ko_mym_reason", "") or ""),
            initial_reviewer_note=str(rev.get("reviewer_note", "") or ""),
            initial_internal_note=str(rev.get("internal_note", "") or ""),
        )

    @app.post("/listado/editar")
    def listado_editar_post():
        if not session.get("authenticated"):
            return redirect(url_for("login"))
        if (session.get("role") or "") != ROLE_ADMIN:
            session["_error"] = "No autorizado: solo Administrador puede editar revisiones."
            return redirect(url_for("review"))

        blob = (request.form.get("edit_blob") or "").strip()
        original_user = (request.form.get("edit_original_user") or "").strip()
        back_url = (request.form.get("back_url") or "").strip() or url_for("listado")
        if not blob:
            session["_error"] = "Falta el identificador de la revisión (blob)."
            return redirect(back_url)

        try:
            rev = get_revision(blob)
        except Exception as exc:
            session["_error"] = f"No se pudo cargar la revisión: {exc}"
            return redirect(back_url)

        status_new = (request.form.get("status") or "Pendiente").strip()
        reviewer_note_new = request.form.get("reviewer_note") or ""
        internal_note_new = request.form.get("internal_note") or ""
        ko_mym_reason_new = request.form.get("ko_mym_reason") or ""
        multitematica_new = (request.form.get("multitematica") or "").strip() in {"1", "on", "true", "True"}

        status_old = str(rev.get("status", "") or "")
        reviewer_note_old = str(rev.get("reviewer_note", "") or "")
        internal_note_old = str(rev.get("internal_note", "") or "")
        ko_mym_reason_old = str(rev.get("ko_mym_reason", "") or "")
        multitematica_old = bool(rev.get("multitematica"))

        changes = {}
        if status_new != status_old:
            changes["status"] = {"from": status_old, "to": status_new}
        if ko_mym_reason_new != ko_mym_reason_old:
            changes["ko_mym_reason"] = {"from": ko_mym_reason_old, "to": ko_mym_reason_new}
        if multitematica_new != multitematica_old:
            changes["multitematica"] = {"from": multitematica_old, "to": multitematica_new}
        if reviewer_note_new != reviewer_note_old:
            changes["reviewer_note"] = {"from": reviewer_note_old, "to": reviewer_note_new}
        if internal_note_new != internal_note_old:
            changes["internal_note"] = {"from": internal_note_old, "to": internal_note_new}

        if not changes:
            return redirect(url_for("listado_editar_done", msg="No hay cambios que guardar.", next=back_url))

        now = datetime.now(timezone.utc)
        history = rev.get("history") if isinstance(rev.get("history"), list) else []
        history.append(
            {
                "timestamp": now.isoformat(),
                "user": session.get("user", ""),
                "action": "edited",
                "source_blob": blob,
                "changes": changes,
            }
        )

        payload = dict(rev)
        payload.pop("_blob_name", None)
        payload["timestamp"] = now.isoformat()
        payload["edited_by"] = session.get("user", "")
        payload["status"] = status_new
        payload["ko_mym_reason"] = ko_mym_reason_new
        payload["multitematica"] = bool(multitematica_new)
        payload["reviewer_note"] = reviewer_note_new
        payload["internal_note"] = internal_note_new
        payload["history"] = history

        record_id = str(payload.get("record_id", "") or "") or str(((payload.get("record") or {}) if isinstance(payload.get("record"), dict) else {}).get("IdCorreo", "") or "")
        log_click(action="edit_revision", username=session.get("user", ""), record_id=record_id, result="ok", extra={"source_blob": blob, "changes": changes})

        try:
            save_revision(blob, payload)
        except Exception as exc:
            session["_error"] = f"No se pudo guardar la edición: {exc}"
            return redirect(back_url)

        return redirect(url_for("listado_editar_done", msg="Cambios guardados.", next=back_url))

    @app.get("/listado/editar/done")
    def listado_editar_done():
        if not session.get("authenticated"):
            return redirect(url_for("login"))
        msg = (request.args.get("msg") or "").strip() or "OK"
        next_url = (request.args.get("next") or "").strip() or url_for("listado")
        return render_template(
            "edit_done.html",
            title="Edición",
            current_user=session.get("user", ""),
            app_version=app_version,
            error=None,
            message=msg,
            next_url=next_url,
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
                session["_error"] = "Bloqueo caducado. Pulsa refrescar para cargar otro registro."
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
                session["_error"] = "Bloqueo caducado (10 min) o registro ya procesado por otro usuario."
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
