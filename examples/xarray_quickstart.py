#!/usr/bin/env python3
"""
Open a Ferrous-produced NetCDF slice with xarray and print a summary.

Usage:
    python3 xarray_quickstart.py [path/to/slice.nc]

If no path is supplied, defaults to /tmp/ferrous_tas_med.nc, which is
where run_xarray.sh writes its output.

The point of this script is that it is as short as climate-data
analysis usually gets in Python: no manual OPeNDAP plumbing, no
downloading a 4 GB file to extract 77 KB — Ferrous did that part.
"""

from __future__ import annotations

import sys

import numpy as np
import xarray as xr


def summarize(path: str) -> None:
    ds = xr.open_dataset(path)
    print(ds)
    print()

    tas = ds["tas"]
    # CMIP6 fill-value is ~1e20; mask anything absurd.
    valid = tas.where(np.abs(tas) < 1e10)
    kelvin_to_celsius = 273.15
    print(f"tas over the fetched region:")
    print(f"  shape  : {tuple(tas.shape)}")
    print(f"  min    : {float(valid.min()) - kelvin_to_celsius:6.2f} °C")
    print(f"  max    : {float(valid.max()) - kelvin_to_celsius:6.2f} °C")
    print(f"  mean   : {float(valid.mean()) - kelvin_to_celsius:6.2f} °C")
    print(
        f"  valid  : {int(valid.count())} / {int(tas.size)} "
        f"({100 * int(valid.count()) / int(tas.size):.1f}% non-fill)"
    )

    # Annual-mean timeseries over the full slice — one line with xarray.
    if "time" in tas.dims and tas.sizes["time"] >= 12:
        # Group 12 steps per year, average over everything else.
        n_years = tas.sizes["time"] // 12
        per_year = (
            tas.isel(time=slice(0, 12 * n_years))
            .coarsen(time=12)
            .mean()
            .mean(dim=[d for d in tas.dims if d != "time"])
        )
        print()
        print(f"Annual-mean tas over the region ({n_years} yr):")
        for year_offset, value in enumerate(per_year.values):
            print(f"  year +{year_offset:2d}: {float(value) - kelvin_to_celsius:6.2f} °C")


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/ferrous_tas_med.nc"
    summarize(path)
