#!/usr/bin/env python3
"""
Use Ferrous directly from Python via the PyO3 extension module.

Setup (one-time):
    pip install maturin
    maturin develop --release --features python   # inside the repo

After that:
    python examples/python_bindings.py

The `ferrous.get(...)` function drives the same fetch path as the CLI
but skips the subprocess shell-out — errors raise Python exceptions,
return value is the path of the written file.
"""

from __future__ import annotations

import numpy as np
import xarray as xr

import ferrous


def main() -> None:
    print(f"ferrous extension module version: {ferrous.__version__}")

    # 1. Fetch the slice. No shelling-out, no subprocess.
    path = ferrous.get(
        dataset_id="CMIP6.ScenarioMIP.CNRM-CERFACS.CNRM-CM6-1.ssp245"
                   ".r1i1p1f2.Amon.tas.gr.v20190219|esgf.ceda.ac.uk",
        variable="tas",
        out="/tmp/ferrous_python.nc",
        time_iso="2020:2025",
        lat_deg="30:46",
        lon_deg="0:30",
    )
    print(f"Wrote {path}")

    # 2. xarray has everything it needs: CF units, calendar, attributes.
    ds = xr.open_dataset(path)
    tas_c = ds["tas"] - 273.15
    tas_c.attrs["units"] = "degrees_C"

    valid = tas_c.where(np.isfinite(tas_c))
    print(
        f"\nMediterranean tas 2020-2025:"
        f"\n  shape = {tuple(tas_c.shape)}"
        f"\n  min   = {float(valid.min()):.2f} °C"
        f"\n  max   = {float(valid.max()):.2f} °C"
        f"\n  mean  = {float(valid.mean()):.2f} °C"
    )

    # 3. Annual means — same one-liner that works in any xarray pipeline.
    annual = tas_c.groupby("time.year").mean().mean(dim=["lat", "lon"])
    print("\nAnnual-mean regional temperature:")
    for year, value in zip(annual["year"].values, annual.values):
        print(f"  {int(year)}: {float(value):.2f} °C")


if __name__ == "__main__":
    main()
