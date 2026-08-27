# pNEUMA CSV Ingestion: Challenges and Solutions

## Context

The pNEUMA dataset consists of vehicle trajectory data collected from drone footage over central Athens. Each CSV file contains hundreds of vehicle tracks in a wide-format layout where a single logical record can span tens of thousands of semicolon-delimited fields.

The source files are UTF-8 with BOM, use CRLF line endings, and semicolons as delimiters. Each logical vehicle record follows this structure:

```
track_id ; type ; traveled_d ; avg_speed ; lat_0 ; lon_0 ; speed_0 ; lon_acc_0 ; lat_acc_0 ; time_0 ; lat_1 ; lon_1 ; ...
```

A single vehicle track with 500+ observation frames produces a logical row exceeding 30,000 characters.

## Problem: Embedded Physical Line Breaks

The source CSV files contain physical line breaks **inside** logical vehicle records. These breaks are not quoted or escaped — they appear mid-row at approximately 32 KB intervals, cutting through numeric tokens arbitrarily.

### Observed Break Patterns

| Pattern | Example (raw bytes) | Naive Result | Correct Value |
|---------|-------------------|--------------|---------------|
| After semicolon | `...0.04;\r\n 37.977...` | Fields split cleanly | N/A (handled) |
| Before semicolon | `...23.738\r\n; 32.82...` | Fields split cleanly | N/A (handled) |
| Between tokens (space) | `...22.520000\r\n 37.977...` | Two complete tokens | N/A (handled) |
| Mid-token with dot | `...22.600\r\n00; 37.97...` | `22.60000` | `22.60000` |
| Mid-token without dot | `...715\r\n800000; 37.97...` | `715800000` | `715.800000` |
| Truncated leading digit | `...584.16; 3\r\n.979245...` | `3.979245` | `37.979245` |

The last three patterns are destructive — they produce values that fail validation (impossible coordinates, broken timestamp sequences).

### Scale of Corruption

In our 87 MB sample file containing 922 vehicle tracks:

- **702** boundary joins had the decimal point preserved (harmless).
- **202** joins produced tokens missing a decimal point entirely.
- **122** joins produced tokens missing one or more leading digits.

A naive `csv.reader` approach recovered only **601 valid tracks** (65%). The remaining 321 tracks were rejected due to impossible coordinates (`lon=23736270.0`), invalid timestamps (`step=-559.96`), or parse failures.

## Solution: Boundary-Marker Repair

### Step 1 — Logical Record Detection

Each logical vehicle record starts with a line matching:

```
^\s*\d+\s*;\s*[A-Za-z]
```

(integer track_id, semicolon, vehicle-type word). This regex reliably separates track-start lines from continuation lines.

### Step 2 — Physical Line Accumulation with Markers

Physical lines belonging to the same track are joined using four rules:

1. **Previous line ends with `;`** → append directly (separator already present).
2. **Next line starts with `;`** → append directly (separator already present).
3. **Next line starts with space + non-space** → insert `;` separator (between-token break).
4. **Otherwise** → insert a NUL marker (`\x00`) at the join point (split-token case).

The NUL marker flags exactly which character position in the assembled text corresponds to a physical-line boundary. This metadata propagates through CSV parsing into individual field strings.

### Step 3 — Field-Aware Contextual Repair

After semicolons are parsed, any field containing a `\x00` marker is identified as *boundary-affected*. For each such field, a set of candidate reconstructions is generated:

- **Simple join** — remove marker (works when the dot was preserved, e.g. `22.600` + `00`).
- **Dot insertion** — insert `.` at the marker position (for missing-dot cases like `715` + `800000`).
- **Digit insertion** — insert each digit 0–9 at the marker (for partially truncated tokens).
- **Digit prepend** — insert each digit 0–9 before the left fragment (for lost leading digits).

The correct candidate is selected using field-type constraints:

| Field | Validation |
|-------|-----------|
| Latitude | Must be in [37.9, 38.1]; prefer closest to neighbouring frame |
| Longitude | Must be in [23.6, 23.9]; prefer closest to neighbouring frame |
| Speed | Must be in [0, 200] m/s |
| Acceleration | Must be in [-50, 50] m/s² |
| Timestamp | Must be closest to expected value inferred from step |

Timestamp step is inferred from clean (non-boundary) consecutive values — not hardcoded. This handles files with different sampling intervals.

### Step 4 — Strict Validation (Unchanged)

After repair, all frames pass the same validation that was always in place:

- Coordinate bounds check (Athens study area).
- Non-negative finite speed.
- Monotonically increasing timestamps with consistent step.
- No empty fields within frames.
- Frame count divisible by 6.

Tracks that cannot be repaired are rejected with a clear diagnostic message.

## Results

| Metric | Naive csv.reader | Raw-line concat | + Dot repair | + Boundary-aware repair |
|--------|-----------------|-----------------|--------------|------------------------|
| Tracks detected | 801 | 922 | 922 | 922 |
| Tracks valid | 423 | 601 | 658 | **922** |
| Frame records | 423,945 | 668,226 | 759,323 | **1,446,887** |
| Rejections | 378 | 321 | 264 | **0** |

The final boundary-aware approach recovers **100% of tracks** from the sample file with zero rejections, producing 3.4x more valid frame records than the initial implementation.

## Design Principles

1. **Only boundary-affected fields are modified.** Normal source values are never touched.
2. **Repair uses context, not heuristics.** Neighbouring frames provide ground truth for coordinate and timestamp validation.
3. **No hardcoded constants.** Timestamp step is inferred per-track. Coordinate ranges come from the Athens bounding box (configurable via constants).
4. **Validation is never weakened.** The same strict checks apply after repair — invalid reconstructions are rejected.
5. **Streaming-oriented.** The file is processed line-by-line; logical records are assembled and parsed one at a time without loading the full 87 MB into memory.

## File Reference

- Extractor: `src/traffic_data_elt/extract/pneuma.py`
- Unit tests: `tests/unit/test_pneuma_extractor.py`
- Sample data: `data/sample/pnemas.csv`
