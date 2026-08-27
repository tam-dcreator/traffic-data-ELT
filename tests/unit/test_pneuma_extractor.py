"""Unit tests for src/traffic_data_elt/extract/pneuma.py.

Covers:
- _concat_lines()            — all four join rules (including boundary marker)
- _parse_logical_text()      — field parsing and trailing-empty removal
- _TRACK_START               — regex correctly accepts/rejects lines
- _boundary_candidates()     — candidate generation for damaged fields
- Boundary repair via extract() — lat, lon, timestamp, and rejection
- PneumaExtractor.extract()  — end-to-end via temporary CSV files
"""

from __future__ import annotations

from pathlib import Path

import pytest

from traffic_data_elt.extract.pneuma import (
    _BOUNDARY_MARKER,
    _TRACK_START,
    PneumaExtractor,
    PneumaRecord,
    _boundary_candidates,
    _concat_lines,
    _parse_logical_text,
    _repair_lat,
    _repair_lon,
    _repair_timestamp,
)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_HEADER = (
    "track_id; type; traveled_d; avg_speed; lat; lon; speed; lon_acc; lat_acc; time\n"
)

_F1 = "37.977391; 23.737688; 4.9178; 0.0518; -0.0299; 0.000000"
_F2 = "37.977391; 23.737688; 5.0000; 0.0000;  0.0000; 0.040000"


def _write_csv(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "test.csv"
    p.write_text(_HEADER + body, encoding="utf-8")
    return p


def _two_frame_row(track_id: int = 1) -> str:
    return f"{track_id}; Car; 48.85; 9.77; {_F1}; {_F2};\n"


# ---------------------------------------------------------------------------
# _TRACK_START regex
# ---------------------------------------------------------------------------


class TestTrackStartRegex:
    def test_accepts_simple_track(self):
        assert _TRACK_START.match("1; Car; 48.85; 9.77;\n")

    def test_accepts_multi_digit_id(self):
        assert _TRACK_START.match("123; Motorcycle; 100.0; 5.0;\n")

    def test_rejects_header(self):
        assert not _TRACK_START.match("track_id; type; traveled_d;\n")

    def test_rejects_numeric_continuation(self):
        assert not _TRACK_START.match("00; 37.977454; 23.737681;\n")

    def test_rejects_semicolon_start(self):
        assert not _TRACK_START.match("; 32.8245;\n")

    def test_rejects_space_start(self):
        assert not _TRACK_START.match(" 37.977255; 23.737924;\n")

    def test_rejects_dot_start(self):
        assert not _TRACK_START.match(".737820; 37.6;\n")


# ---------------------------------------------------------------------------
# _concat_lines
# ---------------------------------------------------------------------------


class TestConcatLinesCase1:
    """Previous line ends with ';' — append directly."""

    def test_appends_directly(self):
        result = _concat_lines("1; Car; 9.77;", " 37.977391;")
        assert result == "1; Car; 9.77; 37.977391;"
        assert _BOUNDARY_MARKER not in result


class TestConcatLinesCase2:
    """Next line starts with ';' — append directly."""

    def test_newline_before_semicolon(self):
        result = _concat_lines("23.73812", "; 4.9178;")
        assert result == "23.73812; 4.9178;"
        assert _BOUNDARY_MARKER not in result


class TestConcatLinesCase3:
    """Next line starts with space — insert ';' separator."""

    def test_inserts_semicolon(self):
        result = _concat_lines("22.520000", " 37.977255;")
        assert result == "22.520000; 37.977255;"
        assert _BOUNDARY_MARKER not in result


class TestConcatLinesCase4:
    """Split token — inserts boundary marker."""

    def test_marker_inserted_on_digit_concat(self):
        result = _concat_lines("...22.600", "00; 37.977454;")
        assert _BOUNDARY_MARKER in result
        # Marker is between '0' (end of prev) and '0' (start of next)
        assert "22.600" + _BOUNDARY_MARKER + "00;" in result

    def test_marker_on_dot_start(self):
        result = _concat_lines("...3", ".979245; 23.7;")
        assert _BOUNDARY_MARKER in result
        assert "3" + _BOUNDARY_MARKER + ".979245" in result

    def test_marker_on_negative_continuation(self):
        result = _concat_lines("...; -", ".5032; 0.0;")
        assert _BOUNDARY_MARKER in result


# ---------------------------------------------------------------------------
# _parse_logical_text
# ---------------------------------------------------------------------------


class TestParseLogicalText:
    def test_strips_whitespace(self):
        text = " 1 ;  Car ; 48.85 ; 9.77 ;"
        row = _parse_logical_text(text)
        assert row[0] == "1"
        assert row[1] == "Car"

    def test_removes_trailing_empty(self):
        text = "1; Car; 10.0; 5.0; 37.98; 23.74; 1.0; 0.0; 0.0; 0.04;"
        row = _parse_logical_text(text)
        assert row[-1] != ""

    def test_preserves_internal_empty(self):
        text = "1; Car; 10.0; 5.0; ; 23.74; 1.0; 0.0; 0.0; 0.04;"
        row = _parse_logical_text(text)
        assert row[4] == ""

    def test_preserves_boundary_marker_in_field(self):
        text = f"1; Car; 10.0; 5.0; 3{_BOUNDARY_MARKER}.979; 23.74; 1.0; 0.0; 0.0; 0.04;"
        row = _parse_logical_text(text)
        assert _BOUNDARY_MARKER in row[4]


# ---------------------------------------------------------------------------
# _boundary_candidates
# ---------------------------------------------------------------------------


class TestBoundaryCandidates:
    def test_simple_join(self):
        # '22.600' + '00' boundary → '22.60000' is first candidate
        token = f"22.600{_BOUNDARY_MARKER}00"
        candidates = _boundary_candidates(token)
        assert "22.60000" in candidates

    def test_dot_insert(self):
        # '715' + '800000' boundary → '715.800000' via dot insert
        token = f"715{_BOUNDARY_MARKER}800000"
        candidates = _boundary_candidates(token)
        assert "715.800000" in candidates

    def test_digit_prepend(self):
        # '3' + '.979245' boundary → '73.979245' via prepend '7'
        token = f"3{_BOUNDARY_MARKER}.979245"
        candidates = _boundary_candidates(token)
        assert "73.979245" in candidates

    def test_digit_insert_at_marker(self):
        # '3' + '.979245' → '37.979245' via inserting '7' at marker
        token = f"3{_BOUNDARY_MARKER}.979245"
        candidates = _boundary_candidates(token)
        assert "37.979245" in candidates


# ---------------------------------------------------------------------------
# Boundary repair functions
# ---------------------------------------------------------------------------


class TestRepairLat:
    def test_missing_digit_lat(self):
        # '3' + '.979245' → should repair to 37.979245
        token = f"3{_BOUNDARY_MARKER}.979245"
        result = _repair_lat(token, neighbor=37.979247)
        assert result == pytest.approx(37.979245)

    def test_no_valid_candidate(self):
        token = f"9{_BOUNDARY_MARKER}.123456"
        result = _repair_lat(token, neighbor=37.98)
        # None of the candidates 9X.123456 or X9.123456 are in [37.9, 38.1]
        # unless 39.123456 or 97.123456 etc — none valid
        # Actually '39.123456' is NOT in [37.9, 38.1]. So should be None.
        assert result is None


class TestRepairLon:
    def test_missing_digit_lon(self):
        # '2' + '.737820' → should repair to 23.737820
        token = f"2{_BOUNDARY_MARKER}.737820"
        result = _repair_lon(token, neighbor=23.737818)
        assert result == pytest.approx(23.737820)

    def test_missing_dot_lon(self):
        # '23' + '736653' → should repair to 23.736653
        token = f"23{_BOUNDARY_MARKER}736653"
        result = _repair_lon(token, neighbor=23.736651)
        assert result == pytest.approx(23.736653)


class TestRepairTimestamp:
    def test_missing_dot_timestamp(self):
        # '715' + '800000' → 715.800000 when expected ≈ 715.8
        token = f"715{_BOUNDARY_MARKER}800000"
        result = _repair_timestamp(token, expected=715.800000)
        assert result == pytest.approx(715.800000)

    def test_missing_digit_timestamp(self):
        # '6' + '.800000' → '46.800000' when expected ≈ 46.8
        token = f"6{_BOUNDARY_MARKER}.800000"
        result = _repair_timestamp(token, expected=46.800000)
        assert result == pytest.approx(46.800000)

    def test_simple_split_timestamp(self):
        # '0.04' + '0000' → '0.040000' (normal split, no repair needed)
        token = f"0.04{_BOUNDARY_MARKER}0000"
        result = _repair_timestamp(token, expected=0.040000)
        assert result == pytest.approx(0.040000)


# ---------------------------------------------------------------------------
# PneumaExtractor.extract() — end-to-end
# ---------------------------------------------------------------------------


class TestExtractorNormalRow:
    def test_single_row(self, tmp_path):
        csv_path = _write_csv(tmp_path, _two_frame_row(1))
        records = list(PneumaExtractor(csv_path).extract())
        assert len(records) == 2
        assert records[0].track_id == 1
        assert records[0].timestamp_s == pytest.approx(0.0)
        assert records[1].timestamp_s == pytest.approx(0.04)

    def test_multiple_tracks(self, tmp_path):
        body = _two_frame_row(1) + _two_frame_row(2)
        records = list(PneumaExtractor(_write_csv(tmp_path, body)).extract())
        assert len(records) == 4
        assert [r.track_id for r in records] == [1, 1, 2, 2]

    def test_row_limit(self, tmp_path):
        body = _two_frame_row(1) + _two_frame_row(2) + _two_frame_row(3)
        records = list(PneumaExtractor(_write_csv(tmp_path, body), row_limit=2).extract())
        assert len(records) == 4
        assert all(r.track_id in (1, 2) for r in records)


class TestExtractorSplitToken:
    """Normal split tokens (dot preserved) are repaired via simple join."""

    def test_timestamp_split(self, tmp_path):
        # '0.04' + '0000' → '0.040000'
        body = (
            "1; Car; 48.85; 9.77;"
            " 37.977391; 23.737688; 4.9178; 0.0518; -0.0299; 0.04\n"
            "0000;\n"
        )
        records = list(PneumaExtractor(_write_csv(tmp_path, body)).extract())
        assert len(records) == 1
        assert records[0].timestamp_s == pytest.approx(0.04)

    def test_lon_split(self, tmp_path):
        body = (
            "1; Car; 48.85; 9.77;"
            " 37.977391; 23.7376\n"
            "88; 4.9178; 0.0518; -0.0299; 0.000000;\n"
        )
        records = list(PneumaExtractor(_write_csv(tmp_path, body)).extract())
        assert len(records) == 1
        assert records[0].lon == pytest.approx(23.737688)


class TestExtractorNewlineBeforeDelimiter:
    def test_value_not_concatenated(self, tmp_path):
        body = (
            "1; Car; 48.85; 9.77;"
            " 37.977391; 23.73812\n"
            "; 4.9178; 0.0518; -0.0299; 0.000000;\n"
        )
        records = list(PneumaExtractor(_write_csv(tmp_path, body)).extract())
        assert len(records) == 1
        assert records[0].lon == pytest.approx(23.73812)


class TestExtractorNewlineAfterDelimiter:
    def test_continuation_after_semicolon(self, tmp_path):
        body = (
            "1; Car; 48.85; 9.77;\n"
            " 37.977391; 23.737688; 4.9178; 0.0518; -0.0299; 0.000000;\n"
        )
        records = list(PneumaExtractor(_write_csv(tmp_path, body)).extract())
        assert len(records) == 1
        assert records[0].lat == pytest.approx(37.977391)


class TestExtractorMalformedRecord:
    def test_bad_frame_width_rejected(self, tmp_path):
        body = (
            "1; Car; 10.0; 5.0; 37.98; 23.74; 1.0; 0.0; 0.0; 0.04; extra;\n"
        ) + _two_frame_row(2)
        records = list(PneumaExtractor(_write_csv(tmp_path, body)).extract())
        assert all(r.track_id == 2 for r in records)

    def test_empty_field_rejected(self, tmp_path):
        body = (
            "1; Car; 48.85; 9.77;"
            " 37.977391; ; 4.9178; 0.0518; -0.0299; 0.000000;\n"
        ) + _two_frame_row(2)
        records = list(PneumaExtractor(_write_csv(tmp_path, body)).extract())
        assert all(r.track_id == 2 for r in records)


# ---------------------------------------------------------------------------
# Boundary repair through extract() — the new cases
# ---------------------------------------------------------------------------


class TestExtractorBoundaryRepairLat:
    """Missing leading digit in latitude repaired from neighbour context."""

    def test_lat_3_repaired_to_37(self, tmp_path):
        # Frame 0: clean lat=37.979247.  Frame 1: lat split as '3' + '.979245'.
        # Prev line ends '...584.160000; 3' next starts '.979245; 23.735438;...'
        frame0 = " 37.979247; 23.735441; 28.7553; 1.5050; 0.2755; 584.160000"
        body = (
            "1; Car; 100.0; 10.0;"
            f"{frame0};"
            " 3\n"
            ".979245; 23.735438; 28.9727; 1.5138; 0.2720; 584.200000;\n"
        )
        records = list(PneumaExtractor(_write_csv(tmp_path, body)).extract())
        assert len(records) == 2
        assert records[1].lat == pytest.approx(37.979245)


class TestExtractorBoundaryRepairLon:
    """Missing leading digit in longitude repaired from neighbour context."""

    def test_lon_2_repaired_to_23(self, tmp_path):
        # Frame 0: clean lon=23.737818.  Frame 1: lon split as '2' + '.737820'.
        frame0 = " 37.977391; 23.737818; 4.9178; 0.0518; -0.0299; 0.000000"
        body = (
            "1; Car; 48.85; 9.77;"
            f"{frame0};"
            " 37.977391; 2\n"
            ".737820; 4.9178; 0.0518; -0.0299; 0.040000;\n"
        )
        records = list(PneumaExtractor(_write_csv(tmp_path, body)).extract())
        assert len(records) == 2
        assert records[1].lon == pytest.approx(23.737820)


class TestExtractorBoundaryRepairTimestamp:
    """Timestamp missing dot or digit repaired from step context."""

    def test_timestamp_715_800000_missing_dot(self, tmp_path):
        # Three frames: ts = 715.760000, '715' + '800000', 715.840000
        frame = " 37.977391; 23.737688; 4.9178; 0.0518; -0.0299"
        body = (
            "1; Car; 100.0; 10.0;"
            f"{frame}; 715.760000;"
            f"{frame}; 715\n"
            "800000;"
            f"{frame}; 715.840000;\n"
        )
        records = list(PneumaExtractor(_write_csv(tmp_path, body)).extract())
        assert len(records) == 3
        assert records[1].timestamp_s == pytest.approx(715.800000)

    def test_timestamp_missing_digit(self, tmp_path):
        # ts = 46.760000, '6' + '.800000' (should be 46.800000), 46.840000
        frame = " 37.977391; 23.737688; 4.9178; 0.0518; -0.0299"
        body = (
            "1; Car; 100.0; 10.0;"
            f"{frame}; 46.760000;"
            f"{frame}; 6\n"
            ".800000;"
            f"{frame}; 46.840000;\n"
        )
        records = list(PneumaExtractor(_write_csv(tmp_path, body)).extract())
        assert len(records) == 3
        assert records[1].timestamp_s == pytest.approx(46.800000)


class TestExtractorBoundaryRepairRejection:
    """Ambiguous boundary repair that cannot be resolved is rejected."""

    def test_unrepairable_lat_rejects_track(self, tmp_path):
        # lat split as '9' + '.123456' — no candidate in [37.9, 38.1]
        frame0 = " 37.977391; 23.737688; 4.9178; 0.0518; -0.0299; 0.000000"
        body = (
            "1; Car; 48.85; 9.77;"
            f"{frame0};"
            " 9\n"
            ".123456; 23.737688; 5.0000; 0.0000; 0.0000; 0.040000;\n"
        ) + _two_frame_row(2)
        records = list(PneumaExtractor(_write_csv(tmp_path, body)).extract())
        # Track 1 rejected; track 2 yields
        assert all(r.track_id == 2 for r in records)

    def test_unrepairable_timestamp_rejects_track(self, tmp_path):
        # timestamp split as '9' + '.999999' — expected ≈ 0.04 from context
        # No candidate near 0.04 → reject
        frame = " 37.977391; 23.737688; 4.9178; 0.0518; -0.0299"
        body = (
            "1; Car; 100.0; 10.0;"
            f"{frame}; 0.000000;"
            f"{frame}; 9\n"
            ".999999;"
            f"{frame}; 0.080000;\n"
        ) + _two_frame_row(2)
        records = list(PneumaExtractor(_write_csv(tmp_path, body)).extract())
        assert all(r.track_id == 2 for r in records)
