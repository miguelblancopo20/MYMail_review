from __future__ import annotations

import secrets
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from flask import session

from mymail.entrada import (
    LOCK_TTL_SECONDS,
    EntradaKey,
    clear_expired_locks,
    delete_record,
    get_record,
    list_keys,
    list_pending_meta,
    refresh_lock,
    release_lock,
)
from mymail.entrada import try_acquire_lock, validate_lock
from mymail.tables import write_descarte, write_resultado

_LOCK = threading.Lock()
_WRITE_LOCK = threading.Lock()
_STATES: Dict[str, "ReviewState"] = {}


def _session_id() -> str:
    sid = session.get("_sid")
    if not sid:
        sid = secrets.token_urlsafe(16)
        session["_sid"] = sid
    return sid


def reset_state() -> None:
    sid = session.get("_sid")
    if not sid:
        return
    with _LOCK:
        state = _STATES.pop(sid, None)
    if state is not None:
        state.release_current_lock(owner=session.get("user", ""))


def get_state() -> "ReviewState":
    sid = _session_id()
    with _LOCK:
        state = _STATES.get(sid)
        if state is None:
            state = ReviewState()
            _STATES[sid] = state
    state.ensure_loaded()
    return state


@dataclass
class ReviewState:
    queue: List[EntradaKey] = field(default_factory=list)
    current_key: Optional[EntradaKey] = None
    current: Dict[str, str] = field(default_factory=dict)
    lock_token: str = ""
    excel_missing: bool = False
    _refreshed_once: bool = False

    def ensure_loaded(self, *, force: bool = False) -> None:
        if not force and (self.queue or self.current_key or self.excel_missing):
            return
        try:
            metas = list_pending_meta(limit=20000)
        except Exception:
            metas = []

        keys: List[EntradaKey] = []
        if metas:
            now = datetime.now(timezone.utc)

            def parse_dt(value: str):
                v = (value or "").strip()
                if not v:
                    return None
                try:
                    if v.endswith("Z"):
                        v = v[:-1] + "+00:00"
                    dt = datetime.fromisoformat(v)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt.astimezone(timezone.utc)
                except Exception:
                    return None

            available: List[EntradaKey] = []
            all_keys: List[EntradaKey] = []
            for m in metas:
                pk = str((m or {}).get("pk", "") or "").strip()
                rk = str((m or {}).get("rk", "") or "").strip()
                if not pk or not rk:
                    continue
                k = EntradaKey(partition_key=pk, row_key=rk)
                all_keys.append(k)

                lock_owner = str((m or {}).get("lock_owner", "") or "").strip()
                until = parse_dt(str((m or {}).get("lock_until", "") or ""))
                is_free = (not lock_owner) or (until is None) or (until <= now)
                if is_free:
                    available.append(k)

            keys = available or all_keys

        if not keys:
            try:
                keys = list_keys()
            except Exception:
                self.excel_missing = True
                self.queue = []
                self.current_key = None
                self.current = {}
                return

        self.excel_missing = False
        self.queue = keys
        self.current_key = None
        self.current = {}
        self.lock_token = ""
        self._refreshed_once = False
        import random

        random.shuffle(keys)

    def pending_count(self) -> int:
        return len(self.queue) + (1 if self.current_key else 0)

    def select_specific(self, key: EntradaKey, *, owner: str) -> bool:
        owner = (owner or "").strip()
        if not owner:
            return False

        self.ensure_loaded()
        if self.current_key and self.current_key == key and self.lock_token and self.current:
            return True

        try:
            self.release_current_lock(owner=owner)
        except Exception:
            pass

        acquired = try_acquire_lock(key, owner=owner, ttl_seconds=LOCK_TTL_SECONDS)
        if not acquired:
            return False
        token, _ = acquired
        try:
            record = get_record(key)
        except Exception:
            try:
                release_lock(key, owner=owner, token=token)
            except Exception:
                pass
            return False

        self.queue = [k for k in self.queue if not (k.partition_key == key.partition_key and k.row_key == key.row_key)]
        self.current_key = key
        self.current = record
        self.lock_token = token
        return True

    def _next_key(self) -> Optional[EntradaKey]:
        return self.queue.pop() if self.queue else None

    def current_record(self, *, owner: str) -> Dict[str, str]:
        owner = (owner or "").strip()
        start_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        attempts = 0
        while True:
            attempts += 1
            if attempts > 250:
                raise RuntimeError("No se pudo seleccionar un correo (demasiados intentos adquiriendo lock).")
            if int(datetime.now(timezone.utc).timestamp() * 1000) - start_ms > 25_000:
                raise TimeoutError("Timeout seleccionando un correo pendiente en CosmosDB (25s).")
            if not self.current_key:
                self.current_key = self._next_key()
                self.current = {}
                self.lock_token = ""

            if not self.current_key:
                if not self._refreshed_once:
                    self._refreshed_once = True
                    try:
                        clear_expired_locks()
                    except Exception:
                        pass
                    self.ensure_loaded(force=True)
                    continue
                return {}

            if not self.lock_token:
                acquired = try_acquire_lock(self.current_key, owner=owner, ttl_seconds=LOCK_TTL_SECONDS)
                if not acquired:
                    self.current_key = None
                    self.current = {}
                    self.lock_token = ""
                    continue
                self.lock_token, _ = acquired

            if not self.current:
                try:
                    self.current = get_record(self.current_key)
                except Exception:
                    self.release_current_lock(owner=owner)
                    self.current_key = None
                    self.current = {}
                    self.lock_token = ""
                    continue

            return self.current

    def release_current_lock(self, *, owner: str) -> None:
        owner = (owner or "").strip()
        if not owner:
            return
        if not self.current_key or not self.lock_token:
            return
        release_lock(self.current_key, owner=owner, token=self.lock_token)
        self.lock_token = ""

    def refresh_current_lock(self, *, owner: str) -> bool:
        if not self.current_key or not self.lock_token:
            return False
        new_until = refresh_lock(self.current_key, owner=owner, token=self.lock_token, ttl_seconds=LOCK_TTL_SECONDS)
        return bool(new_until)

    def ensure_current_lock_valid(self, *, owner: str) -> bool:
        if not self.current_key or not self.lock_token:
            return False
        return validate_lock(self.current_key, owner=owner, token=self.lock_token)

    def abandon_current(self) -> None:
        self.current_key = None
        self.current = {}
        self.lock_token = ""

    def _drop_current(self) -> None:
        if not self.current_key:
            return
        try:
            with _WRITE_LOCK:
                delete_record(self.current_key)
        finally:
            self.current_key = None
            self.current = {}
            self.lock_token = ""

    def skip_current(self, *, username: str) -> None:
        record = self.current_record(owner=username)
        if not record:
            return
        write_descarte(username=username, record=record)
        self._drop_current()

    def submit_current(
        self,
        *,
        username: str,
        status: str,
        reviewer_note: str,
        internal_note: str,
        ko_mym_reason: str = "",
        multitematica: bool = False,
    ) -> None:
        record = self.current_record(owner=username)
        if not record:
            return
        write_resultado(
            username=username,
            record=record,
            status=status,
            ko_mym_reason=ko_mym_reason,
            reviewer_note=reviewer_note,
            internal_note=internal_note,
            multitematica=multitematica,
        )
        self._drop_current()
