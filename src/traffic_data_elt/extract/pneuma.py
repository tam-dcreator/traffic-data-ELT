"""pNEUMA CSV extractor.

Parses the pNEUMA wide-format CSV into a stream of normalised trajectory
frame records, one record per (track, time-step).

pNEUMA CSV format
-----------------
Delimiter : semicolon  (;)
Header row: present (first non-blank line)

Columns::

    track_id ; type ; traveled_d ; avg_speed ;
    lat_0 ; lon_0 ; speed_0 ; lon_acc_0 ; lat_acc_0 ; time_0 ;
    lat_1 ; lon_1 ; speed_1 ; lon_acc_1 ; lat_acc_1 ; time_1 ;
    ...

The repeating 6-tuple (lat, lon, speed, lon_acc, lat_acc, time) can run to
thousands of frames per track.  Each row in the output represents one frame
of one track.

Output columns
--------------
source_file     : basename of the originating CSV file
track_id        : integer vehicle identifier within the file
vehicle_type    : string label (Car, Motorcycle, Taxi, Bus, …)
traveled_d_m    : total distance travelled (metres, float)
avg_speed_ms    : average speed (m/s, float)
lat             : latitude at this frame (decimal degrees, float)
lon             : longitude at this frame (decimal degrees, float)
speed_ms        : instantaneous speed (m/s, float)
lon_acc_ms2     : longitudinal acceleration (m/s², float)
lat_acc_ms2     : lateral acceleration (m/s², float)
timestamp_s     : time offset from recording start (seconds, float)
"""

from __future__ import annotations

import csv
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from traffic_data_elt.utils import get_logger

log = get_logger(__name__)

# Number of fixed header columns before the repeating frame tuples.
_HEADER_COLS = 4
# Width of each repeating frame tuple.
_FRAME_WIDTH = 6


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


class PneumaExtractor:
    """Reads one pNEUMA CSV file and yields :class:`PneumaRecord` objects.

    Parameters
    ----------
    path:
        Path to the CSV file.
    row_limit:
        Maximum number of *source rows* (tracks) to process.  ``0`` means
        no limit; useful for smoke-testing with a small slice.
    """

    def __init__(self, path: str | Path, row_limit: int = 0) -> None:
        self._path = Path(path)
        self._row_limit = row_limit

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract(self) -> Iterator[PneumaRecord]:
        """Yield normalised frame records from the source file."""
        source_file = self._path.name
        rows_seen = 0
        records_yielded = 0
        rows_rejected = 0

        log.info("extracting from %s (row_limit=%d)", source_file, self._row_limit)

        with self._path.open(newline="", encoding="utf-8") as fh:
            reader = csv.reader(fh, delimiter=";")
            header_skipped = False

            for raw_row in reader:
                # Strip whitespace from every field (pNEUMA files often have
                # trailing spaces after the final semicolon).
                row = [c.strip() for c in raw_row if c.strip() != ""]

                # Skip the column-name header row.
                if not header_skipped:
                    header_skipped = True
                    continue

                # Skip blank lines.
                if not row:
                    continue

                if self._row_limit and rows_seen >= self._row_limit:
                    break

                rows_seen += 1

                try:
                    yield from self._parse_track_row(row, source_file)
                    records_yielded += len(row[_HEADER_COLS:]) // _FRAME_WIDTH
                except (ValueError, IndexError) as exc:
                    rows_rejected += 1
                    log.warning(
                        "rejected track row %d in %s: %s",
                        rows_seen,
                        source_file,
                        exc,
                    )

        log.info(
            "finished %s: %d source rows, %d frame records yielded, %d rejected",
            source_file,
            rows_seen,
            records_yielded,
            rows_rejected,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_track_row(row: list[str], source_file: str) -> Iterator[PneumaRecord]:
        """Parse one wide-format source row into frame records."""
        if len(row) < _HEADER_COLS + _FRAME_WIDTH:
            raise ValueError(
                f"row has only {len(row)} fields; "
                f"expected at least {_HEADER_COLS + _FRAME_WIDTH}"
            )

        track_id = int(row[0])
        vehicle_type = row[1]
        traveled_d_m = float(row[2])
        avg_speed_ms = float(row[3])

        frame_cols = row[_HEADER_COLS:]
        # The number of frame columns must be a multiple of _FRAME_WIDTH.
        # Truncate any trailing partial tuple rather than rejecting the row.
        n_frames = len(frame_cols) // _FRAME_WIDTH

        for i in range(n_frames):
            offset = i * _FRAME_WIDTH
            lat = float(frame_cols[offset])
            lon = float(frame_cols[offset + 1])
            speed_ms = float(frame_cols[offset + 2])
            lon_acc = float(frame_cols[offset + 3])
            lat_acc = float(frame_cols[offset + 4])
            timestamp_s = float(frame_cols[offset + 5])

            # Basic coordinate sanity check — Athens bounding box ± generous margin.
            if not (37.9 <= lat <= 38.1 and 23.6 <= lon <= 23.9):
                raise ValueError(
                    f"coordinate out of expected range: lat={lat}, lon={lon}"
                )

            if not math.isfinite(speed_ms) or speed_ms < 0:
                raise ValueError(f"invalid speed: {speed_ms}")

            yield PneumaRecord(
                source_file=source_file,
                track_id=track_id,
                vehicle_type=vehicle_type,
                traveled_d_m=traveled_d_m,
                avg_speed_ms=avg_speed_ms,
                lat=lat,
                lon=lon,
                speed_ms=speed_ms,
                lon_acc_ms2=lon_acc,
                lat_acc_ms2=lat_acc,
                timestamp_s=timestamp_s,
            )
