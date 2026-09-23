"""Unit tests for Bronze range planning (pure, no I/O).

Covers the range-planning contract from the incident spec:
- exact multiple of part size
- non-exact multiple (short final part)
- one-part source
- the current 15,759,302,461-byte production source -> 30 parts
- 1-based part numbers, gapless/contiguous ranges
- HTTP Range / Content-Range header derivation
"""

from __future__ import annotations

import pytest

from traffic_data_elt.load.bronze_ingest import (
    PartRange,
    expected_part_count,
    plan_parts,
)

MIB = 1024 * 1024
PART_512 = 512 * MIB  # 536,870,912
PROD_SIZE = 15_759_302_461


class TestPlanPartsBasics:
    def test_one_part_source(self):
        parts = plan_parts(1000, PART_512)
        assert len(parts) == 1
        p = parts[0]
        assert p.part_number == 1
        assert p.start_byte == 0
        assert p.end_byte == 999
        assert p.expected_length == 1000
        assert p.http_range_header == "bytes=0-999"

    def test_exact_multiple(self):
        # exactly 3 parts, no short final part
        size = 3 * PART_512
        parts = plan_parts(size, PART_512)
        assert len(parts) == 3
        assert [p.expected_length for p in parts] == [PART_512, PART_512, PART_512]
        assert parts[0].start_byte == 0
        assert parts[0].end_byte == PART_512 - 1
        assert parts[-1].end_byte == size - 1

    def test_non_exact_multiple_short_final_part(self):
        size = 2 * PART_512 + 123
        parts = plan_parts(size, PART_512)
        assert len(parts) == 3
        assert parts[0].expected_length == PART_512
        assert parts[1].expected_length == PART_512
        assert parts[2].expected_length == 123
        assert parts[2].end_byte == size - 1

    def test_part_numbers_are_1_based_and_sequential(self):
        parts = plan_parts(5 * PART_512 + 7, PART_512)
        assert [p.part_number for p in parts] == [1, 2, 3, 4, 5, 6]

    def test_ranges_are_contiguous_and_gapless(self):
        from itertools import pairwise
        parts = plan_parts(4 * PART_512 + 999, PART_512)
        for prev, nxt in pairwise(parts):
            assert nxt.start_byte == prev.end_byte + 1
        # total coverage equals source size
        assert sum(p.expected_length for p in parts) == 4 * PART_512 + 999
        assert parts[0].start_byte == 0
        assert parts[-1].end_byte == 4 * PART_512 + 999 - 1


class TestProductionSource:
    def test_15gb_source_yields_30_parts(self):
        parts = plan_parts(PROD_SIZE, PART_512)
        assert len(parts) == 30
        assert expected_part_count(PROD_SIZE, PART_512) == 30

    def test_15gb_final_part_is_short_and_covers_end(self):
        parts = plan_parts(PROD_SIZE, PART_512)
        # first 29 parts are full-size, last is the remainder
        assert all(p.expected_length == PART_512 for p in parts[:29])
        last = parts[-1]
        assert last.part_number == 30
        assert last.end_byte == PROD_SIZE - 1
        assert last.expected_length == PROD_SIZE - 29 * PART_512
        assert 0 < last.expected_length < PART_512

    def test_15gb_total_coverage_equals_source(self):
        parts = plan_parts(PROD_SIZE, PART_512)
        assert sum(p.expected_length for p in parts) == PROD_SIZE


class TestHeaders:
    def test_http_range_header(self):
        p = PartRange(part_number=2, start_byte=PART_512, end_byte=2 * PART_512 - 1,
                      expected_length=PART_512)
        assert p.http_range_header == f"bytes={PART_512}-{2 * PART_512 - 1}"

    def test_content_range_total(self):
        p = PartRange(part_number=1, start_byte=0, end_byte=999, expected_length=1000)
        assert p.content_range_total == "0-999"


class TestValidation:
    def test_zero_source_size_raises(self):
        with pytest.raises(ValueError, match="source_size must be positive"):
            plan_parts(0, PART_512)

    def test_negative_source_size_raises(self):
        with pytest.raises(ValueError, match="source_size must be positive"):
            plan_parts(-1, PART_512)

    def test_zero_part_size_raises(self):
        with pytest.raises(ValueError, match="part_size_bytes must be positive"):
            plan_parts(1000, 0)

    def test_too_many_parts_raises(self):
        # 1-byte parts over a large source would exceed 10,000 parts
        with pytest.raises(ValueError, match="exceeding the S3 limit"):
            plan_parts(20_000, 1)
