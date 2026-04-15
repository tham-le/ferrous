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

import xarray as xr


def summarize(path: str) -> None:
    # xarray auto-masks _FillValue and parses time-units/calendar from the
    # attributes Ferrous copied out of DAS. The whole pipeline is three
    # calls from a bare .nc path to a Celsius climatology.
    ds = xr.open_dataset(path)
    print(ds)
    print()

    tas_c = ds["tas"] - 273.15  # K -> C (units preserved via .attrs)
    tas_c.attrs["units"] = "degrees_C"

    print("tas over the fetched region:")
    print(f"  shape : {tuple(tas_c.shape)}")
    print(f"  min   : {float(tas_c.min()):6.2f} °C")
    print(f"  max   : {float(tas_c.max()):6.2f} °C")
    print(f"  mean  : {float(tas_c.mean()):6.2f} °C")

    # Annual-mean timeseries over the full slice — one groupby on the
    # now-real datetime64 time axis.
    if "time" in tas_c.dims:
        annual = tas_c.groupby("time.year").mean().mean(dim=["lat", "lon"])
        print()
        print("Annual-mean regional tas:")
        for year, value in zip(annual["year"].values, annual.values):
            print(f"  {int(year)}: {float(value):6.2f} °C")


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/ferrous_tas_med.nc"
    summarize(path)
