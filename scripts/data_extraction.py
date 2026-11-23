"""Compatibility entry point for the SkyPoints ETL pipeline."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.etl import main


if __name__ == "__main__":
    raise SystemExit(main())
