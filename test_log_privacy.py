"""Logs never carry usernames, robot names or hardware ids (issue #4).

Run with::

    python -m pytest test_log_privacy.py -v
"""

from __future__ import annotations

import logging
import time

import pytest

from app import PRODUCER_LEASE_SECONDS, SignalingServer, meta_for_log

USERNAME = "alice-secret-user"
ROBOT_NAME = "alices-kitchen-robot"
HARDWARE_ID = "cafe1234deadbeef"
META = {"name": ROBOT_NAME, "transport": "wifi", "hardware_id": HARDWARE_ID}


def _assert_clean(caplog: pytest.LogCaptureFixture) -> None:
    text = "\n".join(r.getMessage() for r in caplog.records)
    for secret in (USERNAME, "bob-other-user", ROBOT_NAME, HARDWARE_ID):
        assert secret not in text, f"{secret!r} leaked into the logs:\n{text}"


def test_meta_for_log_keeps_only_kind_and_keys() -> None:
    assert meta_for_log(META) == "kind=reachy_mini keys=[hardware_id,name,transport]"
    assert meta_for_log({"kind": "microduck"}) == "kind=microduck keys=[kind]"
    assert meta_for_log(["not", "a", "dict"]) == "<list>"


@pytest.mark.asyncio
async def test_signalling_flow_logs_no_identity(caplog: pytest.LogCaptureFixture) -> None:
    """Register, session, refused access, withdraw, sweep: nothing identifying."""
    caplog.set_level(logging.DEBUG)
    server = SignalingServer()
    producer = server.get_or_create_peer(token="tok-p", username=USERNAME)
    consumer = server.get_or_create_peer(token="tok-c", username=USERNAME)
    intruder = server.get_or_create_peer(token="tok-i", username="bob-other-user")

    await server.handle_message(
        producer, {"type": "setPeerStatus", "roles": ["producer"], "meta": META}
    )
    consumer.meta = {"name": "Conversation App"}
    await server.handle_start_session(consumer, {"peerId": producer.peer_id})
    await server.handle_start_session(intruder, {"peerId": producer.peer_id})
    await server.handle_message(
        producer, {"type": "setPeerStatus", "roles": [], "meta": META}
    )
    await server.handle_message(
        producer, {"type": "setPeerStatus", "roles": ["producer"], "meta": META}
    )
    producer.last_seen = time.monotonic() - PRODUCER_LEASE_SECONDS - 1.0
    await server.sweep_stale_producers()

    assert any("Producer registered" in r.getMessage() for r in caplog.records)
    assert any("tried to access producer" in r.getMessage() for r in caplog.records)
    assert any("Sweeping stale producer" in r.getMessage() for r in caplog.records)
    _assert_clean(caplog)
