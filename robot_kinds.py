"""Robot-kind classification shared by the public counters and fleet usage.

``robot_kind_of`` collapses the untrusted ``meta.kind`` into a bounded set
of labels. ``app`` re-exports every name here, so ``app.robot_kind_of``
and friends keep working.
"""

import re


# --- Robot kinds ------------------------------------------------------
#
# The public counters on ``/`` and ``/health`` break producers down by
# robot family. ``meta.kind`` is untrusted input (any authenticated HF
# user can send any meta), so before it reaches a public surface it is
# collapsed into a bounded-cardinality label: known values map to
# themselves, everything else lands in ``OTHER_ROBOT_KIND``. Raw
# ``meta`` is still forwarded verbatim on every owner-scoped path
# (SSE ``list``, ``/api/robot-status``, ``/api/debug/peers``).
#
# Reachy Mini daemons send no ``kind`` at all (their meta is
# ``{name, transport, hardware_id}``), hence the default. Micro Duck
# registers with ``kind="microduck"`` (plus ``release``, ``api_version``).
# See ``docs/META_CONTRACT.md``.
KNOWN_ROBOT_KINDS = ("reachy_mini", "microduck")
DEFAULT_ROBOT_KIND = "reachy_mini"
OTHER_ROBOT_KIND = "other"
# The fixed, ordered key set every public surface exposes: ``/health``
# ``producers_by_kind`` and the status-page cards both iterate this.
PUBLIC_ROBOT_KINDS = KNOWN_ROBOT_KINDS + (OTHER_ROBOT_KIND,)
# A raw ``kind`` longer than this is classified as ``other`` before any
# normalisation work is done on it.
ROBOT_KIND_MAX_RAW_LEN = 64

# Display labels for the status page. These constants are the ONLY
# kind-related strings that ever reach the HTML: ``Template.substitute``
# does no escaping, so nothing derived from ``meta`` may be interpolated
# into the page.
ROBOT_KIND_LABELS = {
    "reachy_mini": "Reachy Mini",
    "microduck": "Micro Duck",
    OTHER_ROBOT_KIND: "Other",
}

# Lookup table for ``robot_kind_of``: a known kind with every
# non-alphanumeric character removed -> the canonical kind. Lets
# ``micro-duck``, ``Micro Duck`` and ``microduck`` all resolve to the
# same bucket without enumerating spellings.
_KIND_BY_COMPACT = {kind.replace("_", ""): kind for kind in KNOWN_ROBOT_KINDS}
_KIND_NON_ALNUM = re.compile(r"[^a-z0-9]")


def _usable_kind(raw: object) -> bool:
    """A kind value carries information only if it is a non-blank string."""
    return isinstance(raw, str) and raw.strip() != ""


def robot_kind_of(meta: object) -> str:
    """Classify a producer's ``meta`` into a bounded robot-kind label.

    Reads ``meta["kind"]``, falling back to the ``meta["robot_type"]``
    alias when ``kind`` is not a usable value (absent, ``None``,
    non-string or blank). If neither is usable the producer is a Reachy
    Mini daemon (they never sent a kind) and the result is
    ``DEFAULT_ROBOT_KIND``.

    A usable value longer than ``ROBOT_KIND_MAX_RAW_LEN`` is ``other``
    outright. Otherwise it is lower-cased, stripped of every character
    outside ``[a-z0-9]`` and looked up against the known kinds compacted
    the same way, so ``microduck``, ``Micro-Duck``, ``micro duck``,
    ``reachy_mini``, ``Reachy Mini`` and ``reachymini`` all resolve to
    their canonical kind; anything else is ``OTHER_ROBOT_KIND``.

    The result is a bounded-cardinality label safe for public display:
    an attacker-controlled ``kind`` can at most bump the ``other``
    counter and can never inject a new key or string into ``/health``
    or the status page. This function never raises; a non-dict ``meta``
    is treated as empty.

    This is a read-only view. The raw ``meta`` dict is left untouched
    and still forwarded verbatim to owner-scoped listeners, so daemons
    and apps keep seeing exactly what the producer registered.
    """
    if not isinstance(meta, dict):
        return DEFAULT_ROBOT_KIND
    raw = meta.get("kind")
    if not _usable_kind(raw):
        raw = meta.get("robot_type")
    if not _usable_kind(raw):
        return DEFAULT_ROBOT_KIND
    if len(raw) > ROBOT_KIND_MAX_RAW_LEN:
        return OTHER_ROBOT_KIND
    compact = _KIND_NON_ALNUM.sub("", raw.lower())
    return _KIND_BY_COMPACT.get(compact, OTHER_ROBOT_KIND)
