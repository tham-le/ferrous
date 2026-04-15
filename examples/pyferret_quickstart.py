#!/usr/bin/env python3
"""
PyFerret demo driven from Python instead of a journal file.

Runs `ferrous get` to produce a local NetCDF, then uses the PyFerret
Python API to open and analyze it. This is the pattern from
FERROUS.md's "Zero learning curve for existing FERRET users" section.

Usage:
    python3 pyferret_quickstart.py

Requires `pyferret` to be installed (conda-forge:
`conda install -c conda-forge pyferret`).
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


DATASET_ID = (
    "CMIP6.ScenarioMIP.CNRM-CERFACS.CNRM-CM6-1.ssp245.r1i1p1f2."
    "Amon.tas.gr.v20190219|esgf.ceda.ac.uk"
)


def find_ferrous() -> str:
    """Prefer a local release build, fall back to $PATH."""
    local = Path("target/release/ferrous")
    if local.is_file() and os.access(local, os.X_OK):
        return str(local.resolve())
    return "ferrous"


def fetch_slice(out_path: Path) -> None:
    ferrous = find_ferrous()
    print(f"==> {ferrous} get ...")
    subprocess.run(
        [
            ferrous,
            "get",
            "--dataset-id",
            DATASET_ID,
            "--variable",
            "tas",
            "--time-iso",
            "2020:2025",
            "--lat-deg",
            "30:46",
            "--lon-deg",
            "0:30",
            "--out",
            str(out_path),
        ],
        check=True,
    )


def analyze(nc_path: Path) -> None:
    try:
        import pyferret
    except ImportError:
        print(
            "pyferret is not installed. Install from conda-forge:\n"
            "    conda install -c conda-forge pyferret",
            file=sys.stderr,
        )
        sys.exit(1)

    pyferret.start(quiet=True, unmapped=True)  # unmapped = no display required
    try:
        # Exactly the same USE command a FERRET user would type.
        pyferret.run(f'USE "{nc_path}"')
        pyferret.run("LET tas_c = tas - 273.15")
        pyferret.run('SET VAR/UNITS="degrees_C" tas_c')

        # Grab the time-averaged mean temperature per grid cell.
        result = pyferret.getdata("tas_c[L=@AVE]")
        data = result["data"]
        print(f"tas_c time-mean shape: {data.shape}")
        # Use numpy for the summary — pyferret returns a numpy array.
        import numpy as np

        valid = data[np.isfinite(data) & (np.abs(data) < 1e10)]
        print(
            f"tas_c over the region, 2020-2025 mean: "
            f"min {valid.min():.2f} °C, max {valid.max():.2f} °C, "
            f"mean {valid.mean():.2f} °C"
        )
    finally:
        pyferret.stop()


if __name__ == "__main__":
    out = Path("/tmp/ferrous_tas_med.nc")
    if not out.exists():
        fetch_slice(out)
    else:
        print(f"==> re-using existing {out}")
    analyze(out)
