#!/usr/bin/env python3
"""
Generate four incident-report PDFs from the committed Marineterrein-Commutes data.

IMPORTANT: this script is LOCAL-ONLY. It does NOT connect to Supabase and does
NOT rerun crash/braking detection. It treats trips.geojson as the exported,
classified dataset and adds only report-friendly context calculations from the
fields already stored in that file.

Run from the repository root:
    python generate_incident_pdfs.py

Input:
    trips.geojson

Optional source-code files (used only to document the definitions/thresholds):
    generate_trips_geojson.py
    app.js

Outputs (saved by default in ./incident_reports/):
    incident_threshold_definitions.pdf
    crash_incident_table.pdf
    sudden_braking_incident_table.pdf
    variable_accuracy_table.pdf

Dependency:
    reportlab

Install once if needed:
    python -m pip install reportlab

Notes on braking rows:
    The GeoJSON stores braking at segment level via `is_braking`. Therefore the
    default report has one row for every already-flagged braking segment. It
    does not invent a new detection/grouping rule. Use --merge-braking only if
    you explicitly want adjacent flagged segments combined into one report row.
"""

from __future__ import annotations

import argparse
import ast
import json
import math
from collections import defaultdict
from datetime import datetime, time, timedelta
from pathlib import Path
from statistics import mean
from typing import Any, Iterable

try:
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER, TA_LEFT
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import mm
    from reportlab.platypus import (
        BaseDocTemplate,
        Frame,
        PageTemplate,
        Paragraph,
        Spacer,
        Table,
        TableStyle,
        PageBreak,
        HRFlowable,
    )
except ImportError:
    raise SystemExit(
        "reportlab is required. Install it with: python -m pip install reportlab"
    )


# ---------------------------------------------------------------------------
# Paths / constants
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parent
GEOJSON_PATH = ROOT / "trips.geojson"
GENERATOR_PATH = ROOT / "generate_trips_geojson.py"
APP_PATH = ROOT / "app.js"

CRASH_CONTEXT_WINDOW_S = 5.0
BRAKING_CONTEXT_WINDOW_S = 5.0
BRAKING_MERGE_GAP_S = 2.0

# These are fallbacks. If the current generator contains the constants, the
# script parses them from the source so the definitions PDF reflects the repo.
FALLBACK_CONSTANTS = {
    "CRASH_IMPACT_THRESHOLD_G": 6.0,
    "CRASH_CLUSTER_GAP_S": 1.0,
    "CRASH_STALL_THRESHOLD_S": 1.0,
    "CRASH_STOP_SEARCH_WINDOW_S": 3.0,
    "CRASH_SPEED_LOOKBACK_MAX_S": 10.0,
    "CRASH_MAX_RECOVERY_S": 120.0,
    "CRASH_SETTLE_WINDOW_S": 3.0,
    "CRASH_SETTLE_DURATION_S": 1.0,
    "CRASH_SETTLE_MAX_RANGE_G": 3.0,
    "CRASH_GPS_STILL_MAX_SPEED_KMH": 5.5,
    "CRASH_COORD_STALL_RADIUS_M": 5.0,
    "CRASH_BASELINE_WINDOW_S": 3.0,
    "CRASH_BASELINE_DELTA_MIN_G": 0.0,
    "CRASH_BURST_GAP_S": 3.0,
    "CRASH_SPEED_CAP_KMH": 40.0,
    "BRAKING_DECEL_THRESHOLD_GPS_KMH_S": 2.0,
    "BRAKING_INTENSITY_CAP_KMH_S": 50.0,
    "SPEED_JUMP_THRESHOLD_KMH": 20.0,
    "MIN_SEGMENT_TIME_S": 0.5,
    "MAX_SPEED_KMH": 40.0,
    "SPEED_SMOOTH_WIN": 5,
    "COORD_PRECISION": 6,
}


def clean_num(v: Any) -> float | None:
    try:
        if v is None or v == "":
            return None
        x = float(v)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError):
        return None


def boolish(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() in {"true", "1", "yes", "y"}
    return bool(v)


def fmt(v: Any, decimals: int = 1, unit: str = "") -> str:
    x = clean_num(v)
    if x is None:
        return "—"
    s = f"{x:.{decimals}f}"
    return f"{s}{unit}" if unit else s


def fmt_int(v: Any) -> str:
    x = clean_num(v)
    return "—" if x is None else str(int(round(x)))


def parse_dt(v: Any) -> datetime | None:
    if not v:
        return None
    s = str(v).strip()
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def parse_clock(v: Any) -> time | None:
    if not v:
        return None
    s = str(v).strip()
    for pattern in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(s, pattern).time()
        except ValueError:
            pass
    return None


def clock_distance_seconds(dt: datetime, t: time) -> float:
    """Absolute difference in seconds between dt's clock and a time-of-day."""
    a = dt.hour * 3600 + dt.minute * 60 + dt.second + dt.microsecond / 1e6
    b = t.hour * 3600 + t.minute * 60 + t.second + t.microsecond / 1e6
    d = abs(a - b)
    return min(d, 86400 - d)


def safe_text(v: Any) -> str:
    if v is None or v == "":
        return "—"
    return str(v)


def source_line(path: Path, phrase: str) -> str:
    if not path.exists():
        return ""
    try:
        for i, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if phrase in line:
                return f"{path.name}, line {i}"
    except Exception:
        pass
    return path.name


# ---------------------------------------------------------------------------
# Source-code parsing: keep the report synchronized with the repo's current
# executable thresholds where possible.
# ---------------------------------------------------------------------------


def load_source_constants(path: Path) -> dict[str, Any]:
    values: dict[str, Any] = dict(FALLBACK_CONSTANTS)
    if not path.exists():
        return values
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except Exception:
        return values

    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
            continue
        name = node.targets[0].id
        if name not in values:
            continue
        try:
            values[name] = ast.literal_eval(node.value)
            continue
        except Exception:
            pass
        # A few constants in the current generator are defined as a bare
        # reference to another constant, not a literal — e.g.
        # CRASH_SETTLE_WINDOW_S = CRASH_STOP_SEARCH_WINDOW_S and
        # CRASH_BURST_GAP_S = CRASH_SETTLE_WINDOW_S. ast.literal_eval can't
        # evaluate a Name node, so without this branch those two constants
        # would silently fall back to FALLBACK_CONSTANTS and never track a
        # future edit to CRASH_STOP_SEARCH_WINDOW_S, even though they
        # currently happen to match it. ast.walk visits top-level module
        # statements in source order, so by the time we reach a reference
        # like this the name it points to has already been resolved above.
        if isinstance(node.value, ast.Name) and node.value.id in values:
            values[name] = values[node.value.id]
    return values


def source_has(path: Path, text: str) -> bool:
    try:
        return path.exists() and text in path.read_text(encoding="utf-8")
    except Exception:
        return False


# ---------------------------------------------------------------------------
# GeoJSON loading / indexing
# ---------------------------------------------------------------------------


def load_geojson(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise SystemExit(f"Could not find {path}. Run this script from the repo root.")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise SystemExit(f"Could not read {path}: {exc}")
    features = data.get("features", [])
    if not isinstance(features, list):
        raise SystemExit("trips.geojson does not contain a valid FeatureCollection.")
    return features


def feature_ts(feature: dict[str, Any]) -> datetime | None:
    p = feature.get("properties", {})
    return parse_dt(p.get("timestamp"))


def feature_speed(feature: dict[str, Any]) -> float | None:
    p = feature.get("properties", {})
    return clean_num(p.get("Speed", p.get("speed")))


def feature_braking(feature: dict[str, Any]) -> float | None:
    p = feature.get("properties", {})
    return clean_num(p.get("braking_intensity"))


def is_segment(feature: dict[str, Any]) -> bool:
    return feature.get("geometry", {}).get("type") == "LineString"


def is_crash(feature: dict[str, Any]) -> bool:
    p = feature.get("properties", {})
    return (
        feature.get("geometry", {}).get("type") == "Point"
        and p.get("event_type") == "crash"
    )


def is_braking(feature: dict[str, Any]) -> bool:
    return is_segment(feature) and boolish(feature.get("properties", {}).get("is_braking"))


def build_trip_index(features: list[dict[str, Any]]):
    segments: dict[str, list[dict[str, Any]]] = defaultdict(list)
    crashes: list[dict[str, Any]] = []

    for f in features:
        p = f.get("properties", {})
        tid = p.get("trip_id")
        if tid and is_segment(f):
            segments[str(tid)].append(f)
        if is_crash(f):
            crashes.append(f)

    for tid in segments:
        segments[tid].sort(key=lambda f: feature_ts(f) or datetime.min)

    return segments, crashes


# ---------------------------------------------------------------------------
# Context calculations — these use existing GeoJSON fields; they do not detect
# new incidents.
# ---------------------------------------------------------------------------


def nearby_segments(
    segs: list[dict[str, Any]],
    center_dt: datetime,
    before_s: float = 5.0,
    after_s: float = 5.0,
) -> list[dict[str, Any]]:
    out = []
    for f in segs:
        ts = feature_ts(f)
        if ts is None:
            continue
        delta = (ts - center_dt).total_seconds()
        if -before_s <= delta <= after_s:
            out.append(f)
    return out


def speeds_in_window(
    segs: Iterable[dict[str, Any]], center_dt: datetime, before_s: float, after_s: float
) -> list[tuple[float, float]]:
    out = []
    for f in segs:
        ts = feature_ts(f)
        sp = feature_speed(f)
        if ts is None or sp is None:
            continue
        delta = (ts - center_dt).total_seconds()
        if -before_s <= delta <= after_s:
            out.append((delta, sp))
    return sorted(out)


def preimpact_metrics(segs: list[dict[str, Any]], center_dt: datetime, window_s: float = 5.0):
    """Return avg/peak of stored braking intensity before a crash.

    This is deliberately called *pre-impact* deceleration: the GeoJSON does
    not store a crash-event deceleration sensor metric. It stores braking
    intensity on GNSS segments.
    """
    vals = []
    for f in segs:
        ts = feature_ts(f)
        d = feature_braking(f)
        if ts is None or d is None:
            continue
        delta = (ts - center_dt).total_seconds()
        if -window_s <= delta < 0 and d > 0:
            vals.append(d)
    return (mean(vals) if vals else None, max(vals) if vals else None)


def derive_crash_datetime(crash: dict[str, Any], segs: list[dict[str, Any]]) -> datetime | None:
    p = crash.get("properties", {})
    existing = parse_dt(p.get("timestamp"))
    if existing:
        return existing
    t = parse_clock(p.get("time_str"))
    if t is None:
        return None
    candidates = []
    for f in segs:
        dt = feature_ts(f)
        if dt is None:
            continue
        candidates.append((clock_distance_seconds(dt, t), dt))
    if not candidates:
        return None
    return min(candidates, key=lambda x: x[0])[1]


def classify_crash(p: dict[str, Any]) -> str:
    # The generator now explicitly classifies genuinely stationary-before-impact
    # events as Tips. Keep the speed fallback for older GeoJSON that predates
    # the explicit crash_type field.
    if p.get("crash_type"):
        return str(p["crash_type"])
    if boolish(p.get("speed_at_impact_unreliable")):
        return "High-Speed Fall"
    speed = clean_num(p.get("preimpact_speed_kmh"))
    if speed is None:
        speed = clean_num(p.get("speed_at_impact_kmh"))
    if speed is None:
        return "Unclassified"
    if speed <= 1:
        return "Tip"
    if speed <= 10:
        return "Low-Speed Fall"
    return "High-Speed Fall"


def outcome(p: dict[str, Any]) -> str:
    # NOTE: detect_crash_events_api() in generate_trips_geojson.py only ever
    # emits a crash event when came_to_stop is True (it drops the candidate
    # otherwise — see "if not came_to_stop or recovery_time_s >
    # CRASH_MAX_RECOVERY_S: continue" in that script), and unresolved=True
    # is one of the ways came_to_stop ends up False. So on data produced by
    # the current generator, every crash that reaches trips.geojson already
    # has came_to_stop=True/unresolved=False, and this will read "Resolved"
    # for every row. That's not a bug in this report — it's a real property
    # of the upstream filter — but don't mistake an all-Resolved column for
    # broken logic here.
    if p.get("crash_outcome"):
        return str(p["crash_outcome"])
    if boolish(p.get("unresolved")):
        return "Unresolved"
    if boolish(p.get("came_to_stop")):
        return "Resolved"
    return "Unclassified"


def crash_row(crash: dict[str, Any], segs: list[dict[str, Any]]) -> dict[str, Any]:
    p = crash.get("properties", {})
    geom = crash.get("geometry", {})
    coords = geom.get("coordinates", [None, None])
    dt = derive_crash_datetime(crash, segs)

    if dt:
        context = nearby_segments(segs, dt, CRASH_CONTEXT_WINDOW_S, 1.0)
        speed_pairs = speeds_in_window(segs, dt, CRASH_CONTEXT_WINDOW_S, 1.0)
        surrounding_speed = mean([s for _, s in speed_pairs]) if speed_pairs else None
        pre_speed = [s for d, s in speed_pairs if d < 0]
        speed_before = pre_speed[-1] if pre_speed else None
        avg_dec, peak_dec = preimpact_metrics(segs, dt, CRASH_CONTEXT_WINDOW_S)
    else:
        context = []
        surrounding_speed = None
        speed_before = None
        avg_dec = peak_dec = None

    speed_impact = clean_num(p.get("speed_at_impact_kmh"))
    classification = classify_crash(p)
    crash_outcome = outcome(p)

    # If a pre-impact speed can be derived from the stored segment stream, keep
    # it as an extra context field. It is not used to alter the source event.
    if speed_before is None and speed_impact is not None:
        speed_before = speed_impact

    return {
        "timestamp": dt.isoformat(sep=" ", timespec="seconds") if dt else safe_text(p.get("time_str")),
        "latitude": coords[1] if len(coords) > 1 else None,
        "longitude": coords[0] if len(coords) > 0 else None,
        "heading": p.get("heading"),
        "event_type": "Crash / fall",
        "severity": p.get("severity"),
        "outcome": crash_outcome,
        "peak_g": p.get("peak_g"),
        "classification": classification,
        "crash_type": p.get("crash_type"),
        "recovery_time_s": p.get("recovery_time_s"),
        "avg_preimpact_decel_kmh_s": avg_dec,
        "peak_preimpact_decel_kmh_s": peak_dec,
        "surrounding_speed_kmh": surrounding_speed,
        "speed_at_impact_kmh": speed_impact,
        "speed_before_impact_kmh": speed_before,
        "suddenness_s": p.get("suddenness_s"),
        "trip_id": p.get("trip_id"),
        "location_approximate": p.get("location_approximate"),
        "context_segments": len(context),
    }


def merge_braking_segments(segs: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    flagged = [f for f in segs if is_braking(f) and feature_ts(f)]
    flagged.sort(key=lambda f: feature_ts(f))
    groups: list[list[dict[str, Any]]] = []
    for f in flagged:
        if not groups:
            groups.append([f])
            continue
        prev = groups[-1][-1]
        gap = (feature_ts(f) - feature_ts(prev)).total_seconds()
        # Gap is measured between segment starts, matching the data's temporal
        # representation. This is a reporting-only grouping option.
        if gap <= BRAKING_MERGE_GAP_S:
            groups[-1].append(f)
        else:
            groups.append([f])
    return groups


def braking_row(group: list[dict[str, Any]]) -> dict[str, Any]:
    group = sorted(group, key=lambda f: feature_ts(f) or datetime.min)
    first = group[0]
    last = group[-1]
    p = first.get("properties", {})
    start_dt = feature_ts(first)
    end_dt = feature_ts(last)
    trip_id = p.get("trip_id")

    # For an unmerged segment, this is simply the segment's own values.
    intensities = [x for x in (feature_braking(f) for f in group) if x is not None]
    speeds = [x for x in (feature_speed(f) for f in group) if x is not None]

    # Surrounding speed is derived from the already-stored Speed values in a
    # ±5 s window around the braking segment/group.
    segs = CURRENT_SEGMENTS.get(str(trip_id), [])
    center = start_dt or end_dt
    surrounding = []
    if center:
        for f in segs:
            dt = feature_ts(f)
            sp = feature_speed(f)
            if dt is None or sp is None:
                continue
            d = (dt - center).total_seconds()
            if -BRAKING_CONTEXT_WINDOW_S <= d <= BRAKING_CONTEXT_WINDOW_S:
                surrounding.append(sp)

    if center:
        coords = first.get("geometry", {}).get("coordinates", [])
        lat = coords[0][1] if coords and len(coords[0]) > 1 else None
        lon = coords[0][0] if coords and len(coords[0]) > 0 else None
    else:
        lat = lon = None

    total_duration = None
    if start_dt and end_dt:
        # Add the final segment's stored duration where available.
        last_duration = clean_num(last.get("properties", {}).get("time_diff_s")) or 0
        total_duration = max(0.0, (end_dt - start_dt).total_seconds() + last_duration)

    return {
        "timestamp": start_dt.isoformat(sep=" ", timespec="seconds") if start_dt else None,
        "latitude": lat,
        "longitude": lon,
        "heading": first.get("properties", {}).get("heading"),
        "event_type": "Sudden braking",
        "avg_deceleration_kmh_s": mean(intensities) if intensities else None,
        "peak_deceleration_kmh_s": max(intensities) if intensities else None,
        "surrounding_speed_kmh": mean(surrounding) if surrounding else (mean(speeds) if speeds else None),
        "speed_at_braking_segment_kmh": mean(speeds) if speeds else None,
        "duration_flagged_interval_s": total_duration,
        "road_quality": first.get("properties", {}).get("road_quality"),
        "gps_distance_m": sum(clean_num(f.get("properties", {}).get("gps_distance_m")) or 0 for f in group),
        "time_interval_s": sum(clean_num(f.get("properties", {}).get("time_diff_s")) or 0 for f in group),
        "trip_id": trip_id,
        "segments_represented": len(group),
    }


# Global read-only index used by braking_row to avoid threading huge arguments.
CURRENT_SEGMENTS: dict[str, list[dict[str, Any]]] = {}


# ---------------------------------------------------------------------------
# PDF helpers
# ---------------------------------------------------------------------------

# Plain grayscale palette, no accent color: black text, gray rules/fills.
INK = colors.black               # body text
MUTED_INK = colors.black                 # all text is black now — no gray secondary text
HAIRLINE = colors.HexColor("#BBBBBB")    # thin rule / border color
ZEBRA = colors.HexColor("#EFEFEF")       # plain gray row tint
HEADER_FILL = colors.HexColor("#DDDDDD")  # plain gray table-header fill


def tracked_html(text: str) -> str:
    """Letter-space a short label for use inside a Paragraph (the kicker
    line). Paragraph collapses runs of literal whitespace the way
    HTML does, which erases the (slightly wider) gap at real word
    boundaries and makes multi-word kickers like 'CRASH AND FALL INCIDENTS'
    render as one letter-spaced blob with no visible word breaks. Using
    &nbsp; at word boundaries keeps them distinct from the plain-space
    letter tracking, which does collapse — and that's fine, since collapsing
    to a single space between tracked letters is exactly what we want."""
    words = text.split(" ")
    return "&nbsp;&nbsp;".join(" ".join(w) for w in words)


def make_styles():
    base = getSampleStyleSheet()
    return {
        "kicker": ParagraphStyle(
            "Kicker",
            parent=base["Normal"],
            fontName="Helvetica-Bold",
            fontSize=7.5,
            leading=9,
            spaceAfter=2,
            alignment=TA_LEFT,
            textColor=MUTED_INK,
        ),
        "title": ParagraphStyle(
            "ReportTitle",
            parent=base["Title"],
            fontName="Helvetica-Bold",
            fontSize=18,
            leading=21,
            spaceAfter=3,
            alignment=TA_LEFT,
            textColor=INK,
        ),
        "subtitle": ParagraphStyle(
            "Subtitle",
            parent=base["Normal"],
            fontName="Helvetica-Oblique",
            fontSize=8,
            leading=10.5,
            textColor=MUTED_INK,
            spaceAfter=9,
        ),
        "body": ParagraphStyle(
            "Body",
            parent=base["BodyText"],
            fontName="Helvetica",
            fontSize=8.5,
            leading=11.5,
            spaceAfter=5,
            textColor=INK,
        ),
        "small": ParagraphStyle(
            "Small",
            parent=base["BodyText"],
            fontName="Helvetica",
            fontSize=6.5,
            leading=8.5,
            textColor=MUTED_INK,
        ),
        "tiny": ParagraphStyle(
            "Tiny",
            parent=base["BodyText"],
            fontName="Helvetica",
            fontSize=5.5,
            leading=6.8,
            textColor=INK,
        ),
        "h2": ParagraphStyle(
            "H2",
            parent=base["Heading2"],
            fontName="Helvetica-Bold",
            fontSize=11,
            leading=13,
            spaceBefore=10,
            spaceAfter=2,
            alignment=TA_LEFT,
            textColor=INK,
        ),
        "meta": ParagraphStyle(
            "Meta",
            parent=base["Normal"],
            fontName="Helvetica",
            fontSize=7,
            leading=9,
            textColor=MUTED_INK,
        ),
        "list_item": ParagraphStyle(
            "ListItem",
            parent=base["BodyText"],
            fontName="Helvetica",
            fontSize=8.5,
            leading=11.5,
            spaceAfter=3,
            leftIndent=14,
            firstLineIndent=-14,
            textColor=INK,
        ),
        "subtitle_plain": ParagraphStyle(
            "SubtitlePlain",
            parent=base["Normal"],
            fontName="Helvetica",
            fontSize=8,
            leading=10.5,
            textColor=MUTED_INK,
            spaceAfter=9,
        ),
    }


def heading(story, text: str, styles) -> None:
    """A section heading followed by a thin accent rule — replaces bare
    Paragraph(text, styles['h2']) calls so every numbered section gets the
    same short divider instead of relying on whitespace alone to separate
    sections."""
    story.append(Paragraph(text, styles["h2"]))
    story.append(HRFlowable(width="100%", thickness=0.5, color=HAIRLINE, spaceAfter=4, spaceBefore=0))


def para(text: Any, style: ParagraphStyle) -> Paragraph:
    # Basic HTML escaping without importing html everywhere.
    s = safe_text(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return Paragraph(s, style)


def build_doc(path: Path, pagesize, title: str):
    # No footer: just margins and a single content frame, no onPage drawing.
    width, height = pagesize
    left = right = 10 * mm
    top = 12 * mm
    bottom = 12 * mm
    doc = BaseDocTemplate(
        str(path), pagesize=pagesize,
        leftMargin=left, rightMargin=right,
        topMargin=top, bottomMargin=bottom,
        title=title,
    )
    frame = Frame(left, bottom, width - left - right, height - top - bottom, id="normal")
    doc.addPageTemplates([PageTemplate(id="main", frames=frame)])
    return doc


def make_table(
    data,
    col_widths=None,
    font_size=6,
    repeat_rows=1,
    alignments=None,
    header_background=None,
):
    header_background = header_background or HEADER_FILL
    wrapped = []

    for r, row in enumerate(data):
        new = []
        for c, cell in enumerate(row):
            is_header = r == 0
            style = ParagraphStyle(
                f"Cell{r}_{c}",
                fontName="Helvetica-Bold" if is_header else "Helvetica",
                fontSize=(font_size + 0.2 if is_header else font_size),
                leading=(font_size + 1.2),
                alignment=(
                    alignments[c]
                    if alignments and c < len(alignments)
                    else TA_LEFT
                ),
                textColor=INK,
            )
            new.append(para(cell, style))
        wrapped.append(new)

    table = Table(
        wrapped,
        colWidths=col_widths,
        repeatRows=repeat_rows,
        hAlign="LEFT",
        splitByRow=1,
    )

    commands = [
        ("BACKGROUND", (0, 0), (-1, 0), header_background),
        ("LINEBELOW", (0, 0), (-1, 0), 0.75, colors.black),
        ("LINEBELOW", (0, 1), (-1, -1), 0.25, HAIRLINE),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 3),
        ("RIGHTPADDING", (0, 0), (-1, -1), 3),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]

    # Very subtle alternating rows, closer to a conventional research report
    # than a dashboard-style table.
    for i in range(2, len(data), 2):
        commands.append(
            ("BACKGROUND", (0, i), (-1, i), ZEBRA)
        )

    table.setStyle(TableStyle(commands))
    return table


def report_header(story, title, subtitle, styles, kicker="INCIDENT AND SAFETY REPORT",
                   subtitle_style=None, stacked_meta=False):
    if kicker:
        story.append(Paragraph(tracked_html(kicker), styles["kicker"]))
    story.append(Paragraph(title, styles["title"]))
    story.append(Paragraph(subtitle, subtitle_style or styles["subtitle"]))
    if stacked_meta:
        story.append(Paragraph('<b>Source</b>: trips.geojson', styles["meta"]))
        story.append(Paragraph(
            '<b>Scope</b>: Existing classified incidents and stored trip measurements',
            styles["meta"],
        ))
    else:
        story.append(Paragraph(
            '<b>SOURCE</b>&nbsp;&nbsp;trips.geojson'
            '&nbsp;&nbsp;&nbsp;·&nbsp;&nbsp;&nbsp;'
            '<b>SCOPE</b>&nbsp;&nbsp;Existing classified incidents and stored trip measurements',
            styles["meta"],
        ))
    story.append(Spacer(1, 3 * mm))
    story.append(HRFlowable(width="100%", thickness=0.75, color=colors.black, spaceAfter=5, spaceBefore=0))


def rows_to_matrix(rows: list[dict[str, Any]], columns: list[str], decimals: dict[str, int] | None = None):
    decimals = decimals or {}
    matrix = [columns]
    for row in rows:
        out = []
        for col in columns:
            v = row.get(col)
            if col == "timestamp" and v:
                try:
                    parsed = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
                    v = parsed.strftime("%d/%m/%Y %H:%M:%S")
                except (TypeError, ValueError):
                    pass
            if isinstance(v, float) and col in decimals:
                out.append(f"{v:.{decimals[col]}f}")
            elif isinstance(v, float):
                out.append(f"{v:.3f}".rstrip("0").rstrip("."))
            elif isinstance(v, bool):
                out.append("Yes" if v else "No")
            else:
                out.append(safe_text(v))
        matrix.append(out)
    return matrix


def column_widths(total_mm: float, weights: list[float]) -> list[float]:
    total = sum(weights)
    return [total_mm * mm * w / total for w in weights]


# ---------------------------------------------------------------------------
# Four reports
# ---------------------------------------------------------------------------


def write_threshold_pdf(out: Path, constants: dict[str, Any], features, crashes, braking_count):
    styles = make_styles()
    doc = build_doc(out, A4, "Incident threshold and definition report")
    story = []

    report_header(
        story,
        "Incident definitions and thresholds",
        f"Generated from the committed trips.geojson ({len(features):,} features; {len(crashes):,} crash points; {braking_count:,} flagged braking segments). ",
        styles,
        kicker=None,
        subtitle_style=styles["subtitle_plain"],
        stacked_meta=True,
    )

    heading(story, "1. What this report is based on", styles)
    story.append(Paragraph(
        "This report does not rerun incident detection. Crash points are taken from GeoJSON features with "
        "<b>event_type = crash</b>. Sudden-braking rows are taken from segment features with "
        "<b>is_braking = true</b>. The report only derives contextual fields such as surrounding speed "
        "from timestamps and stored segment values.", styles["body"]))

    heading(story, "2. Crash / fall definition", styles)
    crash_defs = [
        ["Rule / field", "Current definition", "Why it matters"],
        ["Impact threshold", f"|acc_y| ≥ {constants['CRASH_IMPACT_THRESHOLD_G']} g", "Raw accelerometer impact spike enters crash-candidate clustering."],
        ["Impact clustering", f"Consecutive threshold samples grouped when gap ≤ {constants['CRASH_CLUSTER_GAP_S']} s", "Prevents one physical impact from becoming many candidate spikes."],
        ["Burst grouping", f"Separate throws grouped when gap ≤ {constants['CRASH_BURST_GAP_S']} s", "Allows rapid multiple impacts to be evaluated as one burst."],
        ["Accelerometer settling", f"Post-impact window up to {constants['CRASH_SETTLE_WINDOW_S']} s; stable for about {constants['CRASH_SETTLE_DURATION_S']} s; range ≤ {constants['CRASH_SETTLE_MAX_RANGE_G']} g", "Requires the accelerometer signal to settle after the impact."],
        ["GPS stop", f"GPS speed ≤ {constants['CRASH_GPS_STILL_MAX_SPEED_KMH']} km/h for ≥ {constants['CRASH_SETTLE_DURATION_S']} s and coordinate spread ≤ {constants['CRASH_COORD_STALL_RADIUS_M']} m", "Separates a fall/stop from a jolt while continuing to ride."],
        ["Motion before impact", "Moving events use wheel rotation before the burst; genuinely stationary-before events are allowed when pre-impact GPS also confirms the bike was stationary.", "Allows stationary falls/tips while retaining a motion check for moving falls."],
        ["Stationary-before Tip", f"If the bike is stationary before impact and remains stationary after the impact (with accelerometer settling and GPS stop also confirmed), classify as Tip.", "Separates a tip/fall of a stationary bike from moving falls."],
        ["Wheel stall / recovery", f"Moving events require a wheel stall gap ≥ {constants['CRASH_STALL_THRESHOLD_S']} s and recovery ≤ {constants['CRASH_MAX_RECOVERY_S']} s. Tips do not require a recovery-time value.", "Confirms that a moving fall is followed by a stop; stationary tips are handled by the dedicated Tip rule."],
        ["Impact-speed estimate", f"Wheel-rotation estimate; events at/above {constants['CRASH_SPEED_CAP_KMH']} km/h are not trusted", "This is not the GNSS speed field."],
    ]
    story.append(make_table(crash_defs, column_widths(190, [28, 72, 60]), font_size=6.5))

    heading(story, "3. Crash intensity / severity", styles)
    sev = [
        ["Peak |g|", "Label"],
        ["6.0–<8.0 g", "Minor"],
        ["8.0–<11.0 g", "Hard"],
        ["≥11.0 g", "Severe"],
    ]
    story.append(make_table(sev, column_widths(75, [45, 30]), font_size=7))
    story.append(Spacer(1, 3 * mm))

    heading(story, "4. Crash classification", styles)
    classification = [
        ["Condition", "classification"],
        ["crash_type = Tip", "Tip (stationary before impact and stationary after impact)"],
        ["speed_at_impact_unreliable = true", "High-Speed Fall (speed estimate discarded, not measured)"],
        ["Legacy fallback: speed ≤1 km/h", "Tip"],
        ["Legacy fallback: >1–10 km/h", "Low-Speed Fall"],
        ["Legacy fallback: >10 km/h", "High-Speed Fall"],
        ["No trustworthy speed and no crash_type", "Unclassified"],
    ]
    story.append(make_table(classification, column_widths(100, [55, 45]), font_size=7))
    story.append(Paragraph(
        "The report now uses the generator's explicit crash_type when present. In the current generator, "
        "stationary-before events that also remain stationary after impact are exported as crash_type = Tip. "
        "For older GeoJSON without crash_type, the report keeps a speed-based fallback: ≤1 km/h is treated as Tip, "
        ">1–10 km/h as Low-Speed Fall, and >10 km/h as High-Speed Fall.", styles["small"]))

    heading(story, "5. Crash outcome", styles)
    outcome_tbl = [
        ["outcome", "Definition"],
        ["Resolved", "came_to_stop = true"],
        ["Unresolved", "unresolved = true"],
        ["Unclassified", "Neither outcome field establishes a state"],
    ]
    story.append(make_table(outcome_tbl, column_widths(100, [35, 65]), font_size=7))

    heading(story, "6. Sudden braking definition", styles)
    braking_tbl = [
        ["Rule / field", "Current definition"],
        ["Speed source", "GNSS speed, smoothed with a 5-point rolling average"],
        ["Braking intensity", "(previous stored speed − current stored speed) / elapsed seconds; positive values only"],
        ["Minimum segment time", f"{constants['MIN_SEGMENT_TIME_S']} s"],
        ["GNSS braking threshold", f"≥ {constants['BRAKING_DECEL_THRESHOLD_GPS_KMH_S']} km/h/s"],
        ["Intensity cap", f"{constants['BRAKING_INTENSITY_CAP_KMH_S']} km/h/s"],
        ["Implausible speed jump", f"> {constants['SPEED_JUMP_THRESHOLD_KMH']} km/h between consecutive stored speeds resets the braking series"],
    ]
    story.append(make_table(braking_tbl, column_widths(190, [55, 135]), font_size=6.5))
    story.append(Paragraph(
        "The report uses the existing is_braking flag. It does not apply the separate 5 km/h/s hotspot threshold from the hotspot-generation workflow, because that threshold is for hotspot inclusion rather than the raw braking flag.", styles["small"]))

    heading(story, "7. Derived report fields", styles)
    derived = [
        ["Field", "How this PDF obtains it"],
        ["Surrounding speed", f"Mean of stored Speed values from {CRASH_CONTEXT_WINDOW_S:.0f}s before to 1s after the incident timestamp (not a symmetric window — see crash_row)."],
        ["Avg / peak pre-impact deceleration", f"Mean / maximum of stored braking_intensity values in the {CRASH_CONTEXT_WINDOW_S:.0f}s before a crash. These are GNSS braking values, not accelerometer impact force."],
        ["Full crash timestamp", "Matched from crash time_str to the nearest segment timestamp on the same trip when a full timestamp is not already stored on the crash point."],
        ["Braking coordinates", "First coordinate of the flagged LineString segment; no new spatial detection is performed."],
    ]
    story.append(make_table(derived, column_widths(190, [55, 135]), font_size=6.5))

    heading(story, "8. Important limitations", styles)
    limits = [
        "The crash point location is explicitly marked location_approximate = true in the exported data.",
        "Latitude/longitude are stored to 6 decimal places, but that is numerical storage precision, not GPS accuracy. The source code comments put the practical GPS accuracy floor at roughly 1–3 m.",
        "speed_at_impact_kmh is a wheel-rotation estimate rather than the smoothed GNSS Speed field.",
        "The GeoJSON does not contain a crash-event deceleration measurement. The crash-table deceleration columns are therefore explicitly pre-impact GNSS braking metrics.",
        "Vehicle type is not part of the current committed GeoJSON segment/crash schema, so it is not fabricated in these reports.",
        "Stationary-before events are now eligible for Tip classification when pre-impact GPS also confirms the bike was stationary. Moving events retain the wheel-stall/recovery checks. "
        "The current generator only emits confirmed crash points; candidates that fail the required post-impact checks are not exported.",
    ]
    for x in limits:
        story.append(Paragraph("• " + x, styles["body"]))

    heading(story, "9. Source files", styles)
    source_files = [
        f"Executable definitions: {GENERATOR_PATH.name if GENERATOR_PATH.exists() else 'generate_trips_geojson.py'}.",
        f"Frontend classification/outcome logic: {APP_PATH.name if APP_PATH.exists() else 'app.js'}.",
        f"Data: {GEOJSON_PATH.name}.",
    ]
    for x in source_files:
        story.append(Paragraph("• " + x, styles["body"]))

    doc.build(story)


def write_crash_pdf(out: Path, rows: list[dict[str, Any]]):
    styles = make_styles()
    page = landscape(A4)
    doc = build_doc(out, page, "Crash incident table")
    story = []
    report_header(
        story,
        "Crash and fall incidents",
        f"One row per crash point already exported in trips.geojson. {len(rows):,} incident(s). Context fields are derived from stored trip segments; incident detection is not rerun.",
        styles,
        kicker=None,
        subtitle_style=styles["subtitle_plain"],
        stacked_meta=True,
    )

    columns = [
        "timestamp", "latitude", "longitude", "heading", "event_type", "severity", "outcome",
        "peak_g", "classification", "recovery_time_s",
        "avg_preimpact_decel_kmh_s", "peak_preimpact_decel_kmh_s",
        "surrounding_speed_kmh", "speed_at_impact_kmh", "speed_before_impact_kmh",
        "suddenness_s", "trip_id",
    ]
    weights = [
        27, 13, 13, 13, 18, 12, 14, 13, 25, 22, 22, 22, 20, 21, 22, 15, 30
    ]
    matrix = rows_to_matrix(rows, columns, {
        "latitude": 6, "longitude": 6,
        "peak_g": 2,
        "recovery_time_s": 2,
        "avg_preimpact_decel_kmh_s": 2,
        "peak_preimpact_decel_kmh_s": 2,
        "surrounding_speed_kmh": 1,
        "speed_at_impact_kmh": 1,
        "speed_before_impact_kmh": 1,
        "suddenness_s": 2,
    })
    story.append(make_table(matrix, column_widths(277, weights), font_size=5.2))
    story.append(Spacer(1, 3 * mm))
    story.append(Paragraph(
        "Coordinates are shown to the precision stored in the GeoJSON. ‘Standstill / recovery’ is the exported recovery_time_s: the gap from wheel stall to the next distinct wheel-rotation reading. ‘Avg/peak pre-impact decel’ is based on stored GNSS braking_intensity values in the 5 seconds before the crash.", styles["small"]))
    doc.build(story)


def write_braking_pdf(out: Path, rows: list[dict[str, Any]], merged: bool):
    styles = make_styles()
    page = landscape(A4)
    doc = build_doc(out, page, "Sudden braking incidents")
    story = []
    mode = "Adjacent flagged segments were merged into reporting groups." if merged else "One row per existing is_braking=true segment; no new incident detection/grouping was applied."
    report_header(
        story,
        "Sudden braking incidents",
        f"{len(rows):,} row(s). {mode}",
        styles,
        kicker=None,
        subtitle_style=styles["subtitle_plain"],
        stacked_meta=True,
    )
    columns = [
        "timestamp", "latitude", "longitude", "heading", "event_type", "avg_deceleration_kmh_s",
        "peak_deceleration_kmh_s", "surrounding_speed_kmh",
        "speed_at_braking_segment_kmh", "duration_flagged_interval_s",
        "road_quality", "gps_distance_m", "time_interval_s", "trip_id", "segments_represented",
    ]
    weights = [
        27, 13, 13, 13, 18, 25, 25, 22, 25, 25, 15, 18, 18, 30, 18
    ]
    matrix = rows_to_matrix(rows, columns, {
        "latitude": 6, "longitude": 6,
        "avg_deceleration_kmh_s": 2,
        "peak_deceleration_kmh_s": 2,
        "surrounding_speed_kmh": 1,
        "speed_at_braking_segment_kmh": 1,
        "duration_flagged_interval_s": 2,
        "gps_distance_m": 1,
        "time_interval_s": 3,
    })
    story.append(make_table(matrix, column_widths(277, weights), font_size=5.2))
    story.append(Spacer(1, 3 * mm))
    story.append(Paragraph(
        "The braking intensity values are the existing braking_intensity values from the GeoJSON. Surrounding speed is calculated from stored Speed values within ±5 seconds of the first flagged segment. ‘Road quality’ is the existing road_quality score, not a new classification.", styles["small"]))
    doc.build(story)


def write_accuracy_pdf(out: Path, constants: dict[str, Any]):
    styles = make_styles()
    doc = build_doc(out, A4, "Variable precision and accuracy table")
    story = []
    report_header(
        story,
        "Variable precision and accuracy",
        "Distinguishes numerical storage precision from actual measurement accuracy. Values below are based on the current repository code and comments; an empirical sensor error distribution is not inferred where the repo does not provide one.",
        styles,
        kicker=None,
        subtitle_style=styles["subtitle_plain"],
        stacked_meta=True,
    )

    rows = [
        ["Variable", "Stored / calculated precision", "Accuracy / interpretation", "Source / caveat"],
        ["latitude", f"{constants['COORD_PRECISION']} decimal places", "~11 cm decimal-place resolution; practical GPS accuracy is much coarser.", "Source comment states GPS floor is roughly 1–3 m."],
        ["longitude", f"{constants['COORD_PRECISION']} decimal places", "~11 cm decimal-place resolution at the equator; practical GPS accuracy is much coarser.", "Storage precision is not the same as positional accuracy."],
        ["GNSS Speed", "Speed capped at 40 km/h and smoothed with a 5-point rolling average; output Speed rounded to 0.1 km/h.", "No independent speed error bound is specified in the repo.", "Do not interpret 0.1 km/h as sensor accuracy."],
        ["Braking intensity", "Calculated as (previous speed − current speed) / elapsed time; rounded to 2 decimals; capped at 50 km/h/s.", "Resolution of the reported calculation, not an uncertainty estimate.", "Threshold for is_braking is 2.0 km/h/s in the current GNSS path."],
        ["Peak force / acc_y", "acc_y decoded/rounded to 0.001 g in the source processing; crash peak_g exported to 0.01 g.", "No accelerometer calibration/error tolerance is specified in the repo.", "Reported g values should not be treated as ±0.01 g accurate."],
        ["Crash speed at impact", "Rounded to 0.1 km/h.", "Wheel-rotation estimate; explicitly not GNSS speed. Events whose estimate reaches ≥40 km/h are not trusted by the generator.", "Short wheel-rotation lookbacks can be unreliable."],
        ["Recovery time", "Rounded to 0.01 s.", "Represents the gap between wheel-stall readings and the next distinct wheel-rotation reading.", "Temporal resolution depends on the underlying data1/sample timing."],
        ["Crash onset / suddenness", "Rounded to 0.01 s.", "Time from onset of the relevant accelerometer excursion to the peak sample.", "Raw accelerometer timing uses sample/timestamp mapping in the generator."],
        ["timestamp", "ISO timestamp on segments; crash point currently exports time_str (HH:MM:SS).", "Full crash date/time is reconstructed here only when a same-trip segment timestamp can be matched to the crash clock time.", "If no match exists, the PDF retains the original time_str."],
        ["Crash coordinates", "Point geometry stored as longitude/latitude.", "Explicitly approximate.", "Current generator writes location_approximate = true."],
        ["road_quality", "Integer score already calculated in the pipeline.", "A categorical/derived road-quality score, not a direct sensor accuracy measurement.", "0 means unavailable/unknown in the current frontend."],
    ]
    story.append(make_table(rows, column_widths(190, [31, 55, 60, 44]), font_size=6.3))

    heading(story, "How to read this table", styles)
    story.append(Paragraph("There are three different concepts here:", styles["body"]))
    concepts = [
        "<b>storage precision</b> — how many decimals are written to the file",
        "<b>calculation precision</b> — how values such as braking_intensity are rounded",
        "<b>measurement accuracy</b> — how close a sensor-derived value is to the true physical value",
    ]
    for i, c in enumerate(concepts, start=1):
        story.append(Paragraph(f"{i}. {c}", styles["list_item"]))
    story.append(Paragraph(
        "The repository provides explicit information for the first two and only limited statements about the third, so this report does not manufacture error margins.", styles["body"]))

    heading(story, "Repository-specific notes", styles)
    notes = [
        f"Coordinate precision is currently COORD_PRECISION = {constants['COORD_PRECISION']}; the generator comments explicitly say this is below the GPS accuracy floor.",
        f"GNSS speed uses SPEED_SMOOTH_WIN = {constants['SPEED_SMOOTH_WIN']} before braking calculations.",
        f"The generator caps displayed GNSS speed at MAX_SPEED_KMH = {constants['MAX_SPEED_KMH']} km/h.",
        "The crash marker location is nearest GNSS position to the crash onset, then carried into the exported Point feature.",
    ]
    for note in notes:
        story.append(Paragraph("• " + note, styles["body"]))

    doc.build(story)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=GEOJSON_PATH, help="Path to trips.geojson")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "incident_reports", help="Directory for the four PDFs")
    parser.add_argument(
        "--merge-braking",
        action="store_true",
        help=f"Merge consecutive flagged braking segments within {BRAKING_MERGE_GAP_S:g}s into one report row.",
    )
    args = parser.parse_args()

    features = load_geojson(args.input)
    segments, crashes = build_trip_index(features)
    global CURRENT_SEGMENTS
    CURRENT_SEGMENTS = segments

    braking_count = sum(1 for f in features if is_braking(f))
    constants = load_source_constants(GENERATOR_PATH)

    args.output_dir.mkdir(parents=True, exist_ok=True)

    crash_rows = []
    for crash in crashes:
        tid = str(crash.get("properties", {}).get("trip_id", ""))
        crash_rows.append(crash_row(crash, segments.get(tid, [])))
    crash_rows.sort(key=lambda r: str(r.get("timestamp", "")))

    if args.merge_braking:
        groups = []
        for tid, segs in segments.items():
            groups.extend(merge_braking_segments(segs))
        braking_rows = [braking_row(g) for g in groups]
        braking_rows.sort(key=lambda r: str(r.get("timestamp", "")))
    else:
        braking_rows = [braking_row([f]) for f in features if is_braking(f)]
        braking_rows.sort(key=lambda r: str(r.get("timestamp", "")))

    outputs = [
        args.output_dir / "incident_threshold_definitions.pdf",
        args.output_dir / "crash_incident_table.pdf",
        args.output_dir / "sudden_braking_incident_table.pdf",
        args.output_dir / "variable_accuracy_table.pdf",
    ]

    write_threshold_pdf(outputs[0], constants, features, crashes, braking_count)
    write_crash_pdf(outputs[1], crash_rows)
    write_braking_pdf(outputs[2], braking_rows, args.merge_braking)
    write_accuracy_pdf(outputs[3], constants)

    print("\nDone — generated:")
    for p in outputs:
        print(f"  {p}")
    print(f"\nSource data: {args.input}")
    print(f"Features: {len(features):,}")
    print(f"Crash points: {len(crashes):,}")
    print(f"Flagged braking segments: {braking_count:,}")
    print(f"Braking report rows: {len(braking_rows):,}")
    print("Supabase: NOT USED")


if __name__ == "__main__":
    main()