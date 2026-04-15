#!/usr/bin/env bash
# Fetch a CMIP6 slice with Ferrous, then hand the result to xarray.
# Designed to be run from the repo root.

set -euo pipefail

OUT="${OUT:-/tmp/ferrous_tas_med.nc}"
DATASET_ID="CMIP6.ScenarioMIP.CNRM-CERFACS.CNRM-CM6-1.ssp245.r1i1p1f2.Amon.tas.gr.v20190219|esgf.ceda.ac.uk"

# Pick up the binary from target/release or $PATH.
if [[ -x "target/release/ferrous" ]]; then
    FERROUS="target/release/ferrous"
else
    FERROUS="ferrous"
fi

echo "==> fetching slice via $FERROUS"
"$FERROUS" get \
    --dataset-id "$DATASET_ID" \
    --variable tas \
    --time-iso 2020:2025 \
    --lat-deg 30:46 \
    --lon-deg 0:30 \
    --out "$OUT"

echo
echo "==> handing $OUT to xarray"
PYTHON="${PYTHON:-python3}"
"$PYTHON" "$(dirname "$0")/xarray_quickstart.py" "$OUT"
