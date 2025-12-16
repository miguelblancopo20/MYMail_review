from __future__ import annotations

import secrets
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from flask import session

from mymail.entrada import EntradaKey, delete_record, get_record, list_keys
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
        _STATES.pop(sid, None)


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
    excel_missing: bool = False

    def ensure_loaded(self) -> None:
        if self.queue or self.current_key or self.excel_missing:
            return
        try:
            keys = list_keys()
        except Exception:
            self.excel_missing = True
            return
        if not keys:
            self.excel_missing = False
            self.queue = []
            self.current_key = None
            self.current = {}
            return
        import random

        random.shuffle(keys)
        self.queue = keys
        self.current_key = None
        self.current = {}

    def pending_count(self) -> int:
        return len(self.queue) + (1 if self.current_key else 0)

    def _next_key(self) -> Optional[EntradaKey]:
        return self.queue.pop() if self.queue else None

    def current_record(self) -> Dict[str, str]:
        if not self.current_key:
            self.current_key = self._next_key()
            self.current = {}
        if not self.current_key:
            return {}
        if not self.current:
            try:
                self.current = get_record(self.current_key)
            except Exception:
                self.current = {}
        return self.current

    def _drop_current(self) -> None:
        if not self.current_key:
            return
        try:
            with _WRITE_LOCK:
                delete_record(self.current_key)
        finally:
            self.current_key = None
            self.current = {}

    def skip_current(self, *, username: str) -> None:
        record = self.current_record()
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
    ) -> None:
        record = self.current_record()
        if not record:
            return
        write_resultado(
            username=username,
            record=record,
            status=status,
            ko_mym_reason=ko_mym_reason,
            reviewer_note=reviewer_note,
            internal_note=internal_note,
        )
        self._drop_current()
