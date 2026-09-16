"""Unit tests for the robot-kind classification behind the public counters.

``robot_kind_of`` turns the untrusted ``meta.kind`` any authenticated HF
user can send into a bounded-cardinality label; a regression here either
breaks the Reachy Mini default (daemons send no kind) or lets arbitrary
strings reach ``/health`` and the public status page.

Run with::

    python -m pytest test_robot_kind.py -v
"""

from __future__ import annotations

import pytest

from app import (
    DEFAULT_ROBOT_KIND,
    KNOWN_ROBOT_KINDS,
    OTHER_ROBOT_KIND,
    PUBLIC_ROBOT_KINDS,
    ROBOT_KIND_LABELS,
    ROBOT_KIND_MAX_RAW_LEN,
    SignalingServer,
    robot_kind_of,
)


# ----------------------------------------------------------------------
# robot_kind_of: defaults
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    "meta",
    [
        {},
        {"name": "mini1", "transport": "usb", "hardware_id": "aaaa"},  # real daemon meta
        {"kind": None},
        {"kind": ""},
        {"kind": "   "},
        {"robot_type": None},
        {"robot_type": ""},
    ],
    ids=["empty", "reachy-mini-daemon", "kind-None", "kind-empty", "kind-blank",
         "robot_type-None", "robot_type-empty"],
)
def test_missing_or_empty_kind_defaults_to_reachy_mini(meta):
    assert robot_kind_of(meta) == DEFAULT_ROBOT_KIND == "reachy_mini"


@pytest.mark.parametrize(
    "value",
    [123, 0, 1.5, True, ["microduck"], {"kind": "microduck"}, b"microduck"],
    ids=["int", "zero", "float", "bool", "list", "dict", "bytes"],
)
def test_non_string_kind_defaults(value):
    assert robot_kind_of({"kind": value}) == DEFAULT_ROBOT_KIND


@pytest.mark.parametrize(
    "meta",
    [None, "microduck", 42, ["microduck"], ("kind", "microduck"), object()],
    ids=["None", "str", "int", "list", "tuple", "object"],
)
def test_non_dict_meta_is_treated_as_empty_and_never_raises(meta):
    assert robot_kind_of(meta) == DEFAULT_ROBOT_KIND


# ----------------------------------------------------------------------
# robot_kind_of: known values and normalisation
# ----------------------------------------------------------------------


def test_microduck_is_recognised():
    assert robot_kind_of({"kind": "microduck", "release": "0.9.1"}) == "microduck"


def test_explicit_reachy_mini_is_recognised():
    assert robot_kind_of({"kind": "reachy_mini"}) == "reachy_mini"


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("MicroDuck ", "microduck"),  # case + trailing space
        ("  MICRODUCK", "microduck"),
        ("micro-duck", "microduck"),  # separators are ignored
        ("Micro-Duck", "microduck"),
        ("Micro Duck", "microduck"),
        ("micro_duck", "microduck"),
        ("Reachy Mini", "reachy_mini"),
        ("reachy-mini", "reachy_mini"),
        ("Reachy-Mini", "reachy_mini"),
        ("reachymini", "reachy_mini"),
        ("REACHY.MINI", "reachy_mini"),
        ("microduck™", "microduck"),  # non-ASCII noise dropped
    ],
)
def test_case_whitespace_and_separators_are_normalised(raw, expected):
    assert robot_kind_of({"kind": raw}) == expected


def test_length_bail_happens_before_normalisation():
    """A raw value over ``ROBOT_KIND_MAX_RAW_LEN`` is ``other`` even if
    stripping it would yield a known kind; one at the limit is still
    classified normally.
    """
    at_limit = "microduck" + " " * (ROBOT_KIND_MAX_RAW_LEN - len("microduck"))
    assert len(at_limit) == ROBOT_KIND_MAX_RAW_LEN
    assert robot_kind_of({"kind": at_limit}) == "microduck"
    assert robot_kind_of({"kind": at_limit + " "}) == OTHER_ROBOT_KIND
    assert robot_kind_of({"kind": "-" * 100 + "microduck"}) == OTHER_ROBOT_KIND


def test_robot_type_alias_is_honoured():
    assert robot_kind_of({"robot_type": "microduck"}) == "microduck"


def test_kind_wins_over_robot_type_alias():
    assert robot_kind_of({"kind": "reachy_mini", "robot_type": "microduck"}) == "reachy_mini"


@pytest.mark.parametrize(
    "unusable_kind",
    ["", "   ", None, 123, ["microduck"], {"x": 1}],
    ids=["empty", "blank", "None", "int", "list", "dict"],
)
def test_unusable_kind_falls_back_to_robot_type(unusable_kind):
    assert robot_kind_of({"kind": unusable_kind, "robot_type": "microduck"}) == "microduck"


def test_unusable_kind_and_unusable_alias_default():
    assert robot_kind_of({"kind": "  ", "robot_type": 7}) == DEFAULT_ROBOT_KIND


# ----------------------------------------------------------------------
# robot_kind_of: everything else collapses into "other"
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw",
    [
        "unicorn",
        "reachy_mini_v2",
        "<script>alert(1)</script>",
        "reachy_mini; DROP TABLE robots",
        "x" * 10_000,
        "microduck" + "x" * ROBOT_KIND_MAX_RAW_LEN,
        "---",  # compacts to "" which is not a known kind
    ],
    ids=["unknown", "near-miss", "html", "sqlish", "very-long", "known-prefix-long", "separators-only"],
)
def test_unknown_values_map_to_other(raw):
    assert robot_kind_of({"kind": raw}) == OTHER_ROBOT_KIND


def test_result_is_always_a_known_label():
    """Whatever comes in, the output is one of a fixed set of keys - the
    property the public counters and the status page rely on.
    """
    allowed = set(PUBLIC_ROBOT_KINDS)
    samples = [
        {}, None, {"kind": 1}, {"kind": "MICRODUCK"}, {"kind": "\x00\n$peers"},
        {"kind": "a" * 1000}, {"robot_type": "other"}, {"kind": "Other"},
    ]
    for meta in samples:
        assert robot_kind_of(meta) in allowed


def test_literal_other_is_not_a_known_kind():
    """``other`` is a bucket, not a declarable kind: a producer that
    sends ``kind="other"`` still lands in ``other`` via the unknown
    path, so the label table needs no special case for it.
    """
    assert "other" not in KNOWN_ROBOT_KINDS
    assert robot_kind_of({"kind": "other"}) == OTHER_ROBOT_KIND


def test_labels_cover_every_public_key_exactly():
    assert set(ROBOT_KIND_LABELS) == set(PUBLIC_ROBOT_KINDS)
    assert all(isinstance(v, str) and v for v in ROBOT_KIND_LABELS.values())


def test_classification_does_not_mutate_meta():
    meta = {"kind": "MicroDuck ", "name": "duck1"}
    before = dict(meta)
    robot_kind_of(meta)
    assert meta == before, "meta must be forwarded verbatim; classification is read-only"


# ----------------------------------------------------------------------
# SignalingServer.count_connected_producers[_by_kind]
# ----------------------------------------------------------------------


_tok = 0


def _peer(server: SignalingServer, username: str = "alice"):
    global _tok
    _tok += 1
    return server.get_or_create_peer(token=f"tok-{username}-{_tok}", username=username)


async def _register(server: SignalingServer, peer, meta: dict) -> None:
    await server.handle_set_peer_status(peer, {"roles": ["producer"], "meta": meta})


def test_by_kind_is_zero_filled_when_empty():
    server = SignalingServer()
    counts = server.count_connected_producers_by_kind()
    assert counts == {"reachy_mini": 0, "microduck": 0, "other": 0}
    assert list(counts) == list(PUBLIC_ROBOT_KINDS)
    assert server.count_connected_producers() == 0


@pytest.mark.asyncio
async def test_by_kind_counts_mixed_fleet():
    server = SignalingServer()
    await _register(server, _peer(server), {"name": "mini1", "hardware_id": "a1"})
    await _register(server, _peer(server), {"name": "mini2", "hardware_id": "a2"})
    await _register(server, _peer(server, "bob"), {"name": "duck1", "kind": "microduck", "hardware_id": "b1"})
    await _register(server, _peer(server, "carol"), {"name": "mystery", "kind": "unicorn", "hardware_id": "c1"})

    assert server.count_connected_producers_by_kind() == {
        "reachy_mini": 2,
        "microduck": 1,
        "other": 1,
    }
    assert server.count_connected_producers() == 4


@pytest.mark.asyncio
async def test_by_kind_excludes_disconnected_producers():
    server = SignalingServer()
    mini = _peer(server)
    duck = _peer(server, "bob")
    await _register(server, mini, {"name": "mini1", "hardware_id": "a1"})
    await _register(server, duck, {"name": "duck1", "kind": "microduck", "hardware_id": "b1"})

    # Transient state between SSE close and eviction, or a legacy peer
    # left as connected=False: must not be counted as "online".
    duck.connected = False

    assert server.count_connected_producers_by_kind() == {
        "reachy_mini": 1,
        "microduck": 0,
        "other": 0,
    }
    assert server.count_connected_producers() == 1


@pytest.mark.asyncio
async def test_by_kind_ignores_withdrawn_and_evicted_producers():
    server = SignalingServer()
    mini = _peer(server)
    duck = _peer(server, "bob")
    await _register(server, mini, {"name": "mini1", "hardware_id": "a1"})
    await _register(server, duck, {"name": "duck1", "kind": "microduck", "hardware_id": "b1"})

    await server.handle_set_peer_status(mini, {"roles": [], "meta": {"name": "mini1"}})
    await server.disconnect_peer(duck.peer_id)

    assert server.count_connected_producers_by_kind() == {
        "reachy_mini": 0,
        "microduck": 0,
        "other": 0,
    }


@pytest.mark.asyncio
async def test_non_dict_meta_is_rejected_and_leaves_server_state_intact():
    """A ``setPeerStatus`` whose ``meta`` is not an object is refused
    before it can replace ``peer.meta``: every downstream reader
    (``sweep_stale_producers`` included) does ``meta.get`` and would
    otherwise crash on the next tick.
    """
    from fastapi import HTTPException

    server = SignalingServer()
    p = _peer(server)
    good_meta = {"name": "r", "hardware_id": "a1"}
    await _register(server, p, good_meta)

    for bad in ("x", ["kind", "microduck"], 42, None):
        with pytest.raises(HTTPException) as exc:
            await server.handle_message(
                p, {"type": "setPeerStatus", "roles": ["producer"], "meta": bad}
            )
        assert exc.value.status_code == 400

    assert p.meta == good_meta, "rejected meta must not replace the stored one"
    assert p.peer_id in server.producers
    # The sweeper keeps working on the untouched state.
    assert await server.sweep_stale_producers() == []
    assert server.count_connected_producers_by_kind()["reachy_mini"] == 1


@pytest.mark.asyncio
async def test_by_kind_reflects_kind_change_on_reregistration():
    """A heartbeat that changes ``kind`` moves the producer between
    buckets (meta is replaced verbatim on every setPeerStatus).
    """
    server = SignalingServer()
    p = _peer(server)
    await _register(server, p, {"name": "r", "hardware_id": "a1"})
    assert server.count_connected_producers_by_kind()["reachy_mini"] == 1

    await _register(server, p, {"name": "r", "hardware_id": "a1", "kind": "microduck"})
    counts = server.count_connected_producers_by_kind()
    assert counts["reachy_mini"] == 0
    assert counts["microduck"] == 1
