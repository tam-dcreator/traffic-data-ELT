"""pNEUMA CSV extractor.

Parses the pNEUMA wide-format CSV into a stream of normalised trajectory
frame records, one record per (track, time-step).

Source format
-------------
Encoding: UTF-8 with BOM (utf-8-sig).  Line endings: CRLF.  Delimiter: ``;``.

Physical line-break handling
-----------------------------
The source file contains embedded newlines inside logical vehicle records.
Physical lines are capped at ~32 KB, so long records are broken across
multiple lines.  Four join patterns are handled:

1. Previous line ends with ``;`` → append next line directly.
2. Next line starts with ``;`` → append directly.
3. Next line starts with space + non-space → insert ``;`` separator.
4. Otherwise → token was split at the boundary; join with a NUL marker
   (``\\x00``) so downstream repair can identify the split point.

Boundary repair
---------------
Fields containing a ``\\x00`` marker are *boundary-affected*.  They may
suffer from:

* A missing decimal point (e.g. ``715`` + ``800000`` → ``715.800000``).
* Missing leading digits (e.g. ``3`` + ``.979245`` → ``37.979245``).

Repair uses field-type constraints (coordinate range, timestamp step from
context, speed bounds) to select the correct reconstruction.  Only
boundary-affected fields are ever modified.  If no valid reconstruction is
found the track is rejected.
"""

from __future__ import annotations

import csv
import io
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from traffic_data_elt.utils import get_logger

log = get_logger(__name__)

# Number of fixed header columns before the repeating frame tuples.
_HEADER_COLS = 4
# Width of each repeating frame tuple.
_FRAME_WIDTH = 6

# Marker inserted at physical-line join points (Case 4 of _concat_lines).
_BOUNDARY_MARKER = "\x00"

# Matches the first physical line of a logical vehicle record.
_TRACK_START = re.compile(r"^\s*\d+\s*;\s*[A-Za-z]")


@dataclass(slots=True)
class PneumaRecord:
    """One trajectory frame for one vehicle."""

    source_file: str
    track_id: int
    vehicle_type: str
    traveled_d_m: float
    avg_speed_ms: float
    lat: float
    lon: float
    speed_ms: float
    lon_acc_ms2: float
    lat_acc_ms2: float
    timestamp_s: float


# ---------------------------------------------------------------------------
# Boundary-aware field repair
# ---------------------------------------------------------------------------

# Valid ranges for pNEUMA Athens study area.
_LAT_MIN, _LAT_MAX = 37.9, 38.1
_LON_MIN, _LON_MAX = 23.6, 23.9
_SPEED_MAX = 200.0  # m/s
_ACC_MAX = 50.0  # m/s²


def _boundary_candidates(token: str) -> list[str]:
    """Generate candidate strings for a boundary-affected *token*.

    The token contains one or more ``\\x00`` markers indicating where the
    physical-line break occurred.  We try:

    1. Remove marker (simple concatenation) — works for normal splits like
       ``22.600`` + ``00`` → ``22.60000``.
    2. Insert a ``.`` at the marker position (missing-dot case).
    3. Insert each digit 0–9 at the marker position (missing digit *at* boundary).
    4. Insert each digit 0–9 *before* the left part of the first marker
       (missing digit *before* boundary, e.g. ``3`` + ``.979245`` needs ``7``
       prepended to get ``37.979245`` — but the '7' goes between existing
       content and the fragment).

    Returns de-duplicated candidates as strings (without the marker).
    """
    # Replace all markers for base candidate (simple join).
    base = token.replace(_BOUNDARY_MARKER, "")
    candidates = [base]

    # Work with the first marker only (most common case: single split).
    idx = token.index(_BOUNDARY_MARKER)
    left = token[:idx]
    right = token[idx + 1:]
    # Remove any additional markers in right part for candidate generation.
    right_clean = right.replace(_BOUNDARY_MARKER, "")

    # Insert '.' at boundary.
    if "." not in left and "." not in right_clean:
        candidates.append(left + "." + right_clean)

    # Insert digit at boundary.
    for d in "0123456789":
        candidates.append(left + d + right_clean)

    # Insert digit before the left part (prepend to left).
    # Handles case where truncation lost a leading digit of the left portion.
    # E.g. left='3', right='.979245' → try '73.979245', '37.979245' etc.
    # We insert digit between the sign (if any) and the numeric content.
    sign = ""
    left_digits = left
    if left_digits.startswith("-"):
        sign = "-"
        left_digits = left_digits[1:]

    for d in "0123456789":
        candidates.append(sign + d + left_digits + right_clean)

    # Also try inserting '.' between left and a digit+right (dot was lost AND digit).
    # E.g. '715' + '800000' could need '715.800000' (dot insert covers this already).

    # De-duplicate while preserving order.
    seen: set[str] = set()
    unique: list[str] = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            unique.append(c)
    return unique


def _repair_lat(token: str, neighbor: float | None) -> float | None:
    """Repair a boundary-affected latitude field."""
    candidates = _boundary_candidates(token)
    best: float | None = None
    best_dist = math.inf
    for c in candidates:
        try:
            v = float(c)
        except ValueError:
            continue
        if _LAT_MIN <= v <= _LAT_MAX:
            if neighbor is not None:
                dist = abs(v - neighbor)
                if dist < best_dist:
                    best = v
                    best_dist = dist
            else:
                # No neighbor — accept first valid.
                return v
    return best


def _repair_lon(token: str, neighbor: float | None) -> float | None:
    """Repair a boundary-affected longitude field."""
    candidates = _boundary_candidates(token)
    best: float | None = None
    best_dist = math.inf
    for c in candidates:
        try:
            v = float(c)
        except ValueError:
            continue
        if _LON_MIN <= v <= _LON_MAX:
            if neighbor is not None:
                dist = abs(v - neighbor)
                if dist < best_dist:
                    best = v
                    best_dist = dist
            else:
                return v
    return best


def _repair_speed(token: str) -> float | None:
    """Repair a boundary-affected speed field."""
    candidates = _boundary_candidates(token)
    for c in candidates:
        try:
            v = float(c)
        except ValueError:
            continue
        if 0.0 <= v <= _SPEED_MAX and math.isfinite(v):
            return v
    return None


def _repair_acc(token: str) -> float | None:
    """Repair a boundary-affected acceleration field."""
    candidates = _boundary_candidates(token)
    for c in candidates:
        try:
            v = float(c)
        except ValueError:
            continue
        if -_ACC_MAX <= v <= _ACC_MAX:
            return v
    return None


def _repair_timestamp(token: str, expected: float | None) -> float | None:
    """Repair a boundary-affected timestamp field.

    Prefers the candidate closest to *expected* (inferred from step).
    """
    candidates = _boundary_candidates(token)
    if expected is None:
        # Without context, accept any non-negative reasonable value.
        for c in candidates:
            try:
                v = float(c)
            except ValueError:
                continue
            if 0.0 <= v <= 100000.0:
                return v
        return None

    best: float | None = None
    best_dist = math.inf
    for c in candidates:
        try:
            v = float(c)
        except ValueError:
            continue
        if v < 0:
            continue
        dist = abs(v - expected)
        if dist < best_dist:
            best = v
            best_dist = dist

    # Accept only if close to expected (within one step or 1 second).
    if best is not None and best_dist > max(abs(expected) * 0.001 + 1.0, 1.0):
        return None
    return best


# ---------------------------------------------------------------------------
# Raw-line concatenation with boundary markers
# ---------------------------------------------------------------------------


def _concat_lines(accumulated: str, new_line: str) -> str:
    """Append *new_line* to *accumulated* using the correct join rule.

    Case 4 inserts a NUL marker (``\\x00``) at the join point so that
    downstream field repair can identify boundary-affected tokens.
    """
    if accumulated.endswith(";"):
        return accumulated + new_line
    if new_line.startswith(";"):
        return accumulated + new_line
    if len(new_line) > 1 and new_line[0] == " " and new_line[1] != " ":
        return accumulated + ";" + new_line
    # Case 4: split token — mark the boundary.
    return accumulated + _BOUNDARY_MARKER + new_line


def _parse_logical_text(text: str) -> list[str]:
    """Parse logical-record text into fields, preserving boundary markers.

    Strips whitespace around delimiters but keeps ``\\x00`` inside fields.
    Removes only the trailing empty field from the final semicolon.
    """
    reader = csv.reader(io.StringIO(text), delimiter=";")
    row = next(reader)
    row = [v.strip() for v in row]
    if row and row[-1] == "":
        row.pop()
    return row


# ---------------------------------------------------------------------------
# Extractor class
# ---------------------------------------------------------------------------


class PneumaExtractor:
    """Reads one pNEUMA CSV file and yields :class:`PneumaRecord` objects.

    Parameters
    ----------
    path:
        Path to the CSV file.
    row_limit:
        Maximum number of *logical* vehicle rows to process.  ``0`` means
        no limit.
    """

    def __init__(self, path: str | Path, row_limit: int = 0) -> None:
        self._path = Path(path)
        self._row_limit = row_limit

    def extract(self) -> Iterator[PneumaRecord]:
        """Yield normalised frame records from the source file."""
        source_file = self._path.name
        rows_seen = 0
        records_yielded = 0
        rows_rejected = 0

        log.info("extracting from %s (row_limit=%d)", source_file, self._row_limit)

        with self._path.open(encoding="utf-8-sig") as fh:
            next(fh, None)  # skip header

            accumulated: str = ""

            for raw_line in fh:
                line = raw_line.rstrip("\r\n")
                if not line:
                    continue

                if _TRACK_START.match(raw_line):
                    if accumulated:
                        result = self._process_logical(
                            accumulated, source_file, rows_seen
                        )
                        if result is not None:
                            frames, rejected = result
                            rows_seen += 1
                            if rejected:
                                rows_rejected += 1
                                log.warning(
                                    "rejected track row %d in %s: %s",
                                    rows_seen,
                                    source_file,
                                    rejected,
                                )
                            else:
                                yield from frames
                                records_yielded += len(frames)
                            if self._row_limit and rows_seen >= self._row_limit:
                                accumulated = ""
                                break
                    accumulated = line
                else:
                    accumulated = _concat_lines(accumulated, line)

            # Flush final record.
            if accumulated and not (self._row_limit and rows_seen >= self._row_limit):
                result = self._process_logical(accumulated, source_file, rows_seen)
                if result is not None:
                    rows_seen += 1
                    frames, rejected = result
                    if rejected:
                        rows_rejected += 1
                        log.warning(
                            "rejected track row %d in %s: %s",
                            rows_seen,
                            source_file,
                            rejected,
                        )
                    else:
                        yield from frames
                        records_yielded += len(frames)

        log.info(
            "finished %s: %d logical rows, %d frame records yielded, %d rejected",
            source_file,
            rows_seen,
            records_yielded,
            rows_rejected,
        )

    # ------------------------------------------------------------------

    @staticmethod
    def _process_logical(
        text: str, source_file: str, rows_seen: int
    ) -> tuple[list[PneumaRecord], str] | None:
        if not text.strip():
            return None
        try:
            row = _parse_logical_text(text)
        except (csv.Error, StopIteration) as exc:
            return [], str(exc)
        if not row:
            return None
        try:
            frames = list(PneumaExtractor._parse_track_row(row, source_file))
            return frames, ""
        except (ValueError, IndexError) as exc:
            return [], str(exc)

    @staticmethod
    def _parse_track_row(row: list[str], source_file: str) -> Iterator[PneumaRecord]:
        """Parse one wide-format logical row into frame records.

        Boundary-affected fields (containing ``\\x00``) are repaired using
        field-type constraints and neighbour context.
        """
        if len(row) < _HEADER_COLS + _FRAME_WIDTH:
            raise ValueError(
                f"row has only {len(row)} fields; "
                f"expected at least {_HEADER_COLS + _FRAME_WIDTH}"
            )

        # --- Fixed track-level fields ---
        if not row[0] or _BOUNDARY_MARKER in row[0]:
            raise ValueError("track_id field is empty or damaged")
        track_id = int(row[0].replace(_BOUNDARY_MARKER, ""))
        vehicle_type = row[1].replace(_BOUNDARY_MARKER, "")
        traveled_d_m = float(row[2].replace(_BOUNDARY_MARKER, ""))
        avg_speed_ms = float(row[3].replace(_BOUNDARY_MARKER, ""))

        frame_cols = row[_HEADER_COLS:]

        if len(frame_cols) % _FRAME_WIDTH != 0:
            raise ValueError(
                f"track {track_id}: frame column count {len(frame_cols)} "
                f"is not divisible by {_FRAME_WIDTH}"
            )

        n_frames = len(frame_cols) // _FRAME_WIDTH

        # --- First pass: parse frames, deferring boundary repair ---
        # Store raw tokens and initially-parsed values.
        raw_tokens: list[list[str]] = []  # [frame_idx][field_idx]
        parsed: list[list[float | None]] = []

        for i in range(n_frames):
            offset = i * _FRAME_WIDTH
            frame_strs = frame_cols[offset: offset + _FRAME_WIDTH]

            if any(f == "" for f in frame_strs):
                raise ValueError(
                    f"track {track_id}: empty field in frame {i}: {frame_strs!r}"
                )

            raw_tokens.append(frame_strs)
            frame_vals: list[float | None] = []
            for token in frame_strs:
                if _BOUNDARY_MARKER in token:
                    frame_vals.append(None)  # needs repair
                else:
                    frame_vals.append(float(token))
            parsed.append(frame_vals)

        # --- Infer timestamp step from clean values ---
        clean_ts: list[tuple[int, float]] = []
        for i in range(n_frames):
            if parsed[i][5] is not None:
                clean_ts.append((i, parsed[i][5]))  # type: ignore[arg-type]

        step: float | None = None
        if len(clean_ts) >= 2:
            steps: list[float] = []
            for a in range(len(clean_ts) - 1):
                ia, va = clean_ts[a]
                ib, vb = clean_ts[a + 1]
                gap = ib - ia
                s = (vb - va) / gap
                if s > 0:
                    steps.append(s)
            if steps:
                steps.sort()
                step = steps[len(steps) // 2]

        # --- Second pass: repair boundary-affected fields ---
        for i in range(n_frames):
            for j in range(6):
                if parsed[i][j] is not None:
                    continue  # already clean
                token = raw_tokens[i][j]
                repaired: float | None = None

                if j == 0:  # lat
                    neighbor = _find_neighbor(parsed, i, 0)
                    repaired = _repair_lat(token, neighbor)
                elif j == 1:  # lon
                    neighbor = _find_neighbor(parsed, i, 1)
                    repaired = _repair_lon(token, neighbor)
                elif j == 2:  # speed
                    repaired = _repair_speed(token)
                elif j in (3, 4):  # lon_acc, lat_acc
                    repaired = _repair_acc(token)
                elif j == 5:  # timestamp
                    expected = _expected_ts(parsed, i, step)
                    repaired = _repair_timestamp(token, expected)

                if repaired is None:
                    raise ValueError(
                        f"track {track_id} frame {i}: "
                        f"cannot repair boundary field {j}: {token!r}"
                    )
                parsed[i][j] = repaired

        # --- Final validation ---
        timestamps = [parsed[i][5] for i in range(n_frames)]
        if len(timestamps) >= 2:
            ts_step = timestamps[1] - timestamps[0]  # type: ignore[operator]
            if ts_step <= 0:
                raise ValueError(
                    f"track {track_id}: non-positive timestamp step {ts_step}"
                )
            tolerance = max(abs(ts_step) * 0.01, 1e-6)
            for i in range(1, len(timestamps)):
                actual = timestamps[i] - timestamps[i - 1]  # type: ignore[operator]
                if abs(actual - ts_step) > tolerance:
                    raise ValueError(
                        f"track {track_id}: inconsistent timestamp step at frame {i}: "
                        f"got {actual}, expected ~{ts_step}"
                    )

        for i in range(n_frames):
            lat = parsed[i][0]
            lon = parsed[i][1]
            speed_ms = parsed[i][2]
            lon_acc = parsed[i][3]
            lat_acc = parsed[i][4]
            timestamp_s = parsed[i][5]

            if not (_LAT_MIN <= lat <= _LAT_MAX and _LON_MIN <= lon <= _LON_MAX):  # type: ignore[operator]
                raise ValueError(
                    f"track {track_id} frame {i}: "
                    f"coordinate out of expected range: lat={lat}, lon={lon}"
                )
            if not math.isfinite(speed_ms) or speed_ms < 0:  # type: ignore[arg-type]
                raise ValueError(
                    f"track {track_id} frame {i}: invalid speed {speed_ms}"
                )

            yield PneumaRecord(
                source_file=source_file,
                track_id=track_id,
                vehicle_type=vehicle_type,
                traveled_d_m=traveled_d_m,
                avg_speed_ms=avg_speed_ms,
                lat=lat,  # type: ignore[arg-type]
                lon=lon,  # type: ignore[arg-type]
                speed_ms=speed_ms,  # type: ignore[arg-type]
                lon_acc_ms2=lon_acc,  # type: ignore[arg-type]
                lat_acc_ms2=lat_acc,  # type: ignore[arg-type]
                timestamp_s=timestamp_s,  # type: ignore[arg-type]
            )


# ---------------------------------------------------------------------------
# Neighbour helpers
# ---------------------------------------------------------------------------


def _find_neighbor(parsed: list[list[float | None]], idx: int, field: int) -> float | None:
    """Find the nearest clean value for *field* in a neighbouring frame."""
    # Search backwards first, then forwards.
    for offset in range(1, min(idx + 1, 20)):
        v = parsed[idx - offset][field]
        if v is not None:
            return v
    for offset in range(1, min(len(parsed) - idx, 20)):
        v = parsed[idx + offset][field]
        if v is not None:
            return v
    return None


def _expected_ts(parsed: list[list[float | None]], idx: int, step: float | None) -> float | None:
    """Compute expected timestamp at *idx* from neighbours + step."""
    if step is None:
        return None
    # Try previous clean timestamp.
    for offset in range(1, min(idx + 1, 50)):
        prev = parsed[idx - offset][5]
        if prev is not None:
            return prev + step * offset
    # Try next clean timestamp.
    for offset in range(1, min(len(parsed) - idx, 50)):
        nxt = parsed[idx + offset][5]
        if nxt is not None:
            return nxt - step * offset
    return None
