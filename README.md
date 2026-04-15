# Ferrous

> Fast, ergonomic CMIP6 climate data access — powered by Rust.
> Measured **5561× reduction** in server traffic through OPeNDAP slicing,
> with a **27× speedup** on repeat requests via local cache.

## Status

Working end-to-end: `search` / `get` / `inspect` CLI subcommands,
`--out file.nc` produces a real NetCDF-3 classic file with all the CF
metadata (`units`, `calendar`, `_FillValue`, …) that xarray / PyFerret /
MATLAB / R expect. A `ferrous.get(...)` Python binding lives behind an
optional Cargo feature, buildable via `maturin develop --features python`.
Everything tested end-to-end against the live CEDA ESGF node.

## Quick start

```bash
cargo build --release
./target/release/ferrous --help
```

## Measured traffic reduction (live, CEDA node, CNRM-CM6-1)

| Variable | Region | Full file | Sliced | Reduction |
|---|---|---|---|---|
| `tos` (sea surface T) | y=100..140, x=50..80 × 12 months | 210.9 MB | 61 KB | **3444×** |
| `tas` (surface air T) | 30..46°N, 0..30°E × 12 months | 74.7 MB | 13 KB | **5561×** |

On a repeat `ferrous get` of the same slice, **0 bytes** leave the local
disk and wall time drops from ~1 s to ~0.04 s.

## Search

```bash
# CMIP6 facet search
ferrous search --variable tos --experiment ssp245 --source CNRM-CM6-1 --limit 5

# JSON output for scripting
ferrous search --variable tos --experiment ssp245 --json

# Override the search endpoint (CEDA is the reliable default; others drift)
ferrous --endpoint https://esgf-data.dkrz.de/esg-search/search search --variable tos
```

## Get (sliced fetch)

```bash
# The FERROUS.md headline example: natural CLI, all human units.
ferrous get --variable tas --experiment ssp245 --source CNRM-CM6-1 \
  --time-iso 2020:2050 --lat-deg 43:46 --lon-deg 5:7 \
  --out tas_med.dods

# Atmospheric (1D rectilinear) — degree resolution via 1D axis lookup.
ferrous get --variable tas --experiment ssp245 --source CNRM-CM6-1 \
  --lat-deg 30:46 --lon-deg 0:30 --time-iso 2020:2025 \
  --out tas_med.dods

# Ocean (2D curvilinear tri-polar) — degree resolution via 2D bbox.
ferrous get \
  --dataset-id "CMIP6.ScenarioMIP.CNRM-CERFACS.CNRM-CM6-1.ssp245.r1i1p1f2.Omon.tos.gn.v20190219|esgf.ceda.ac.uk" \
  --variable tos \
  --lat-deg 30:46 --lon-deg 0:30 --time 0:11 \
  --out tos_med.dods

# Array-index selection works for any grid (escape hatch).
ferrous get --dataset-id "…" --variable tos \
  --time 0:11 --lat 100:140 --lon 50:80 \
  --out tos_raw.dods

# --dry-run prints the OPeNDAP URL without fetching.
ferrous get --dataset-id "…" --variable tos --time 0:0 --dry-run --out /dev/null

# Bypass the local cache for one invocation.
ferrous --no-cache get --variable tas --lat-deg 0:10 --lon-deg 0:10 --out tas.dods
```

**Resolution modes:**

| Flag | Input | How |
|---|---|---|
| `--time` | array indices | direct |
| `--time-iso START:STOP` | ISO dates (`YYYY` or `YYYY-MM-DD`) | fetches DAS, parses CF units + calendar, converts via chrono |
| `--lat` / `--lon` | array indices | direct |
| `--lat-deg` / `--lon-deg` | degrees | fetches coord variables, resolves 1D or 2D |

Supported calendars: `gregorian` / `standard` / `proleptic_gregorian`, and
`noleap` / `365_day`. Others (`360_day`, `all_leap`, `julian`) error with
a clear message — fall back to `--time` indices there.

## Inspect

```bash
# Decode a fetched .dods file: per-variable shape + min/max/mean
ferrous inspect tos_slice.dods

# Show the raw DDS too
ferrous inspect tos_slice.dods --dds
```

### Output format

`ferrous get` auto-detects the format from the `--out` extension:

| Extension | Format | Who reads it |
|---|---|---|
| `.nc` | NetCDF-3 classic | xarray, PyFerret, MATLAB, R, Panoply, … |
| anything else | DAP2 binary | pydap, `ferrous inspect` |

Override with `--format {nc,dods}` if you want a different extension.

The NetCDF-3 writer is ~280 lines of pure Rust — no libnetcdf /
HDF5 C dependency, works on every platform where `cargo build` works.
NetCDF-4 (HDF5-backed) is a roadmap item.

## Examples

Ready-to-run demos live in [`examples/`](examples/):

```bash
# xarray one-shot (Python script + bash wrapper)
./examples/run_xarray.sh

# Jupyter notebook with inline plots
jupyter notebook examples/xarray_quickstart.ipynb

# Python bindings — no subprocess, no shell-out
maturin develop --release --features python   # one-time build
python examples/python_bindings.py

# PyFerret (journal file or Python-driven)
pyferret -script examples/pyferret_quickstart.jnl
python examples/pyferret_quickstart.py
```

All examples pull the same 77 KB Mediterranean slice and produce a
figure or summary. See [`examples/README.md`](examples/README.md) for
details and prerequisites.

## Roadmap

Done:

- [x] ESGF Solr search (Dataset + File records)
- [x] OPeNDAP constraint expression builder
- [x] Polite-mode rate-limited HTTP client
- [x] `search` / `get` / `inspect` subcommands
- [x] Content-addressed local cache — repeat requests = 0 bytes
- [x] DAP2 binary decoder + Grid container support
- [x] Degree-based `--lat-deg` / `--lon-deg` (1D + 2D curvilinear)
- [x] ISO-date `--time-iso 2020:2050` via CF time-units
- [x] All five CF calendars — gregorian / noleap / all_leap / 360_day / julian
- [x] NetCDF-3 classic output with global + per-variable attributes
- [x] `--all-files` mode — multi-file time-axis concatenation
- [x] Streaming progress bar on the main data fetch
- [x] PyO3 Python bindings — `import ferrous; ferrous.get(...)`
- [x] `examples/` — xarray, Jupyter, PyFerret, Python-bindings quickstarts

Not yet:

- [ ] NetCDF-4 output (HDF5-backed) for larger files / compression
- [ ] Argovis (Argo float) support — separate data source, deserves its own module

## License

MIT — see [`LICENSE`](LICENSE).
