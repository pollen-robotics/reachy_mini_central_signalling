"""Per-app session counts in fleet usage (issue #5).

Run with::

    python -m pytest test_usage_apps.py -v
"""

from __future__ import annotations

import json

import pytest

import fleet_usage
from fleet_usage import (
    APP_MIN_USERS,
    APP_OTHER,
    SESSION_END_ENDED,
    app_label,
    build_summary,
    merge_usage_rows,
)
from test_usage import DAY0, MINI, _roll_to, _rows, _tracker


def _session(tracker, clock, sid: str, user: str, name: object, seconds: float = 60) -> None:
    tracker.session_started(sid, MINI, user, {"name": name})
    clock.advance(seconds)
    tracker.session_ended(sid, SESSION_END_ENDED)


def _day_apps(tracker, clock) -> dict:
    _roll_to(tracker, clock, DAY0 + 86400)
    (day,) = _rows(tracker, "daily")
    return day["apps"]


@pytest.mark.parametrize(
    "raw,label",
    [
        ("Conversation App", "conversation-app"),
        ("reachy-desktop-app", "reachy-desktop-app"),
        ("pollen-robotics/hand_tracker", "pollen-robotics/hand_tracker"),
        ("  <script>x</script> ", "script-x-/script"),
        ("x" * 500, "x" * 64),
        ("!!!", APP_OTHER),
        (None, APP_OTHER),
        (42, APP_OTHER),
    ],
)
def test_app_label_is_normalised_and_bounded(raw, label):
    assert app_label(raw) == label


def test_app_published_only_from_min_users():
    tracker, clock = _tracker()
    for i in range(APP_MIN_USERS):
        _session(tracker, clock, f"pub-{i}", f"user-{i}", "Conversation App", seconds=120)
    for i in range(5):  # one account, many sessions: still private
        _session(tracker, clock, f"priv-{i}", "alice", "Alice Secret App", seconds=30)

    apps = _day_apps(tracker, clock)

    assert set(apps) == {"conversation-app", APP_OTHER}
    assert apps["conversation-app"]["sessions"] == APP_MIN_USERS
    assert apps["conversation-app"]["users"] == APP_MIN_USERS
    assert apps["conversation-app"]["durations"]["sum_s"] == 120.0 * APP_MIN_USERS
    assert apps[APP_OTHER]["sessions"] == 5
    assert apps[APP_OTHER]["users"] == 1
    assert "alice" not in json.dumps(apps)


def test_space_id_preferred_over_display_name():
    tracker, clock = _tracker()
    metas = [
        {"name": "My App", "space_id": "Pollen-Robotics/Hand_Tracker"},
        {"name": "Hand tracker (fork)", "space_id": "pollen-robotics/hand_tracker"},
        {"name": "Hand Tracker", "space_id": ""},  # empty id: falls back to the name
        {"name": "Hand Tracker", "space_id": {"bad": 1}},
    ]
    for i, meta in enumerate(metas):
        tracker.session_started(f"s{i}", MINI, f"u{i}", meta)
    apps = tracker._day.apps
    assert apps["pollen-robotics/hand_tracker"].sessions == 2
    assert apps["hand-tracker"].sessions == 2
    assert "my-app" not in apps


def test_name_cap_folds_extra_names_into_other(monkeypatch):
    monkeypatch.setattr(fleet_usage, "APP_MAX_NAMES_PER_DAY", 2)
    tracker, clock = _tracker()
    for i, name in enumerate(["a", "b", "c", "d"]):
        tracker.session_started(f"s{i}", MINI, f"u{i}", {"name": name})
    assert set(tracker._day.apps) == {"a", "b", APP_OTHER}
    assert tracker._day.apps[APP_OTHER].sessions == 2


def test_session_without_consumer_info_counts_as_other():
    tracker, clock = _tracker()
    tracker.session_started("s", MINI)
    clock.advance(10)
    tracker.session_ended("s", SESSION_END_ENDED)
    apps = _day_apps(tracker, clock)
    assert apps == {APP_OTHER: {"sessions": 1, "users": 0, "durations": apps[APP_OTHER]["durations"]}}
    assert apps[APP_OTHER]["durations"]["count"] == 1


def test_merge_daily_rows_combines_apps():
    durations = {"count": 1, "sum_s": 10.0, "max_s": 10.0, "hist": [1, 0, 0, 0, 0, 0]}
    a = {"day": "2026-09-30", "coverage_s": 100, "apps": {"x": {"sessions": 2, "users": 3, "durations": durations}}}
    b = {"day": "2026-09-30", "coverage_s": 100, "apps": {
        "x": {"sessions": 1, "users": 4, "durations": durations},
        APP_OTHER: {"sessions": 5, "users": 1, "durations": durations},
    }}
    merged = merge_usage_rows("daily", a, b)["apps"]
    assert merged["x"]["sessions"] == 3
    assert merged["x"]["users"] == 4
    assert merged["x"]["durations"]["sum_s"] == 20.0
    assert merged[APP_OTHER]["sessions"] == 5


def test_summary_lists_top_apps_over_recent_days():
    def day(d: str, apps: dict) -> dict:
        return {"day": d, "coverage_s": 86400, "apps": apps}

    def agg(sessions: int, users: int, sum_s: float) -> dict:
        return {"sessions": sessions, "users": users, "durations": {"count": sessions, "sum_s": sum_s, "max_s": 1, "hist": []}}

    daily = {
        "2026-09-20": day("2026-09-20", {"old-app": agg(99, 9, 3600)}),  # outside the 7 days
        "2026-09-29": day("2026-09-29", {"conv": agg(4, 3, 3600), APP_OTHER: agg(2, 1, 60)}),
        "2026-09-30": day("2026-09-30", {"conv": agg(6, 5, 7200), "desktop": agg(1, 3, 0)}),
    }
    summary = build_summary({}, daily, DAY0 + 3600, 600)

    assert summary["apps"]["min_users"] == APP_MIN_USERS
    assert summary["apps"]["rows"] == [
        {"app": "conv", "sessions": 10, "users": 5, "hours": 3.0},
        {"app": "desktop", "sessions": 1, "users": 3, "hours": 0.0},
        {"app": APP_OTHER, "sessions": 2, "users": 1, "hours": 0.0},
    ]


def test_server_feeds_consumer_name_and_account():
    from app import SignalingServer

    tracker, clock = _tracker()
    server = SignalingServer(usage=tracker)
    robot = server.get_or_create_peer(token="t-r", username="owner")
    app_peer = server.get_or_create_peer(token="t-a", username="owner")

    async def flow():
        await server.handle_set_peer_status(robot, {"roles": ["producer"], "meta": MINI})
        app_peer.meta = {"name": "Hand Tracker"}
        await server.handle_start_session(app_peer, {"peerId": robot.peer_id})

    import asyncio

    asyncio.run(flow())
    agg = tracker._day.apps["hand-tracker"]
    assert agg.sessions == 1
    assert len(agg.users) == 1
