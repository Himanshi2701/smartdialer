"""
OPTIONAL real-provider adapter for Plivo (https://www.plivo.com/docs/voice/api/calls).

Status: written against Plivo's public Voice API docs, NOT exercised by the test
suite (it needs an account, a rented number and a public webhook URL). Nothing
in the simulator or tests imports it. It exists to show that the dialer core does
not change when a real provider is plugged in - only this translation layer does.

Usage sketch:
    export PLIVO_AUTH_ID=... PLIVO_AUTH_TOKEN=... PLIVO_FROM=+91XXXXXXXXXX
    export PLIVO_WEBHOOK_BASE=https://<your-ngrok-host>
    provider = PlivoProvider.from_env()
    # your HTTP server calls provider.handle_webhook(kind, form_params) for
    #   /plivo/ring, /plivo/answer, /plivo/hangup   and returns its XML for /answer

How Plivo concepts map onto the TelecomProvider interface:
    place_call      POST /v1/Account/{auth_id}/Call/  -> request_uuid
                    our call id is put in the callback URLs (?call_id=...) so every
                    webhook can be matched to our call even before we know Plivo's ids.
    RINGING         ring_url callback
    ANSWERED        answer_url callback. Plivo WAITS for our XML here, so for predictive
                    calls the agent bind happens synchronously in the webhook and the XML
                    <Dial>s the agent (or plays a compliant message if nobody is free).
                    That is better than the mock's BRIDGE command: no extra round trip.
    COMPLETED etc.  hangup_url callback, CallStatus in {completed, busy, failed, timeout,
                    no-answer}; BillDuration > 0 means a conversation happened.
    hangup          DELETE /Call/{call_uuid}/  (or DELETE /Request/{request_uuid}/ if not yet answered)
    lookup          Plivo has no client idempotency key. Gap: if we crash before storing
                    request_uuid we reconcile only through the webhooks (they carry our
                    call_id) plus the ring timeout. Documented in docs/ADR.md (open risks).
"""
from __future__ import annotations

import base64
import json
import os
import threading
import urllib.parse
import urllib.request
from typing import Callable, Optional

from .base import (ANSWERED, COMPLETED, FAILED, NO_ANSWER, RINGING, PlaceCallRequest, PlaceCallResult,
                   ProviderCallStatus, ProviderEvent, ProviderRejected, ProviderTimeout, TelecomProvider)

API = "https://api.plivo.com/v1/Account/{auth_id}/"


class PlivoProvider(TelecomProvider):
    name = "plivo"

    def __init__(self, auth_id: str, auth_token: str, from_number: str, webhook_base: str,
                 agent_endpoint: Callable[[str], str] = lambda agent_id: f"sip:{agent_id}@example.invalid",
                 timeout_s: float = 5.0, clock=None):
        self.auth_id, self.auth_token = auth_id, auth_token
        self.from_number = from_number
        self.webhook_base = webhook_base.rstrip("/")
        self.agent_endpoint = agent_endpoint
        self.timeout_s = timeout_s
        self.clock = clock
        self._events: list[ProviderEvent] = []
        self._lock = threading.Lock()
        self._by_key: dict[str, str] = {}              # our call id -> request_uuid / call_uuid
        self.on_answer: Optional[Callable[[str], Optional[str]]] = None   # call_id -> agent_id or None

    @classmethod
    def from_env(cls) -> "PlivoProvider":
        return cls(os.environ["PLIVO_AUTH_ID"], os.environ["PLIVO_AUTH_TOKEN"], os.environ["PLIVO_FROM"],
                   os.environ["PLIVO_WEBHOOK_BASE"])

    # ------------------------------------------------------------ REST
    def _req(self, method: str, path: str, body: Optional[dict] = None) -> dict:
        url = API.format(auth_id=self.auth_id) + path
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        tok = base64.b64encode(f"{self.auth_id}:{self.auth_token}".encode()).decode()
        req.add_header("Authorization", f"Basic {tok}")
        req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_s) as r:
                raw = r.read()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            if e.code >= 500 or e.code == 429:
                raise ProviderRejected(f"plivo {e.code}")   # Plivo did not create the call
            raise ProviderRejected(f"plivo {e.code}: {e.read()[:200]!r}")
        except (TimeoutError, urllib.error.URLError) as e:
            raise ProviderTimeout(f"plivo: {e}")               # unknown outcome -> reconcile

    def _cb(self, kind: str, call_id: str) -> str:
        return f"{self.webhook_base}/plivo/{kind}?" + urllib.parse.urlencode({"call_id": call_id})

    def place_call(self, req: PlaceCallRequest) -> PlaceCallResult:
        if req.idempotency_key in self._by_key:
            return PlaceCallResult(self._by_key[req.idempotency_key])
        body = {"from": self.from_number, "to": req.to_number.replace("-", "").lstrip("+"),
                "answer_url": self._cb("answer", req.call_id), "answer_method": "POST",
                "ring_url": self._cb("ring", req.call_id), "hangup_url": self._cb("hangup", req.call_id),
                "ring_timeout": 30}
        if req.agent_id:
            body["answer_url"] += "&" + urllib.parse.urlencode({"agent": req.agent_id})
        res = self._req("POST", "Call/", body)
        rid = res.get("request_uuid")
        if not rid:
            raise ProviderRejected(f"plivo: unexpected response {res}")
        self._by_key[req.idempotency_key] = rid
        return PlaceCallResult(rid)

    def bridge(self, provider_call_id, agent_id, idempotency_key=None):
        # With Plivo the bridge is the XML returned from the answer webhook (see answer_xml).
        return None

    def hangup(self, provider_call_id, idempotency_key=None):
        pid = provider_call_id or self._by_key.get(idempotency_key or "")
        if not pid:
            return
        try:
            self._req("DELETE", f"Call/{pid}/")
        except ProviderRejected:
            try:
                self._req("DELETE", f"Request/{pid}/")      # not answered yet -> cancel the request
            except ProviderRejected:
                pass                                         # already gone: hangup is idempotent

    def lookup(self, idempotency_key):
        pid = self._by_key.get(idempotency_key)
        if not pid:
            return None
        try:
            r = self._req("GET", f"Call/{pid}/")
        except ProviderRejected:
            return None
        st = {"completed": COMPLETED, "busy": NO_ANSWER, "no-answer": NO_ANSWER, "timeout": NO_ANSWER,
              "failed": FAILED, "in-progress": ANSWERED, "ringing": RINGING}.get(r.get("call_state", "").lower(),
                                                                                 "INITIATED")
        bill = float(r.get("bill_duration") or 0)
        return ProviderCallStatus(pid, idempotency_key, st, dict(answered=bill > 0, bridged=bill > 0, talk_s=bill))

    def poll_events(self, now: float) -> list[ProviderEvent]:
        with self._lock:
            out, self._events = self._events, []
            return out

    # ------------------------------------------------------------ webhooks
    def handle_webhook(self, kind: str, query: dict, form: dict) -> str:
        """Translate a Plivo callback into a normalized ProviderEvent. Returns XML for /answer."""
        call_id = query.get("call_id")
        pcid = form.get("RequestUUID") or form.get("CallUUID")
        status = (form.get("CallStatus") or "").lower()
        ts = self.clock.now() if self.clock else 0.0
        if kind == "ring":
            typ, payload = RINGING, {}
        elif kind == "answer":
            typ, payload = ANSWERED, {}
        else:
            bill = float(form.get("BillDuration") or 0)
            typ = {"completed": COMPLETED, "busy": NO_ANSWER, "no-answer": NO_ANSWER, "timeout": NO_ANSWER,
                   "failed": FAILED}.get(status, COMPLETED)
            payload = dict(answered=bill > 0, bridged=bill > 0, talk_s=bill, cause=form.get("HangupCauseName"))
        # Plivo does not give an event id; (call, kind, status) is a natural dedupe key.
        eid = f"{pcid}:{kind}:{status}"
        with self._lock:
            self._events.append(ProviderEvent(self.name, eid, pcid, call_id, typ, ts, payload))
        if kind == "answer":
            agent = query.get("agent") or (self.on_answer(call_id) if self.on_answer else None)
            return self.answer_xml(agent)
        return ""

    def answer_xml(self, agent_id: Optional[str]) -> str:
        if agent_id:
            return (f"<Response><Dial><User>{self.agent_endpoint(agent_id)}</User></Dial></Response>")
        return ("<Response><Speak>Sorry, all our representatives are busy. We will call you back.</Speak>"
                "<Hangup/></Response>")
