# Ferrous

> Fast, ergonomic CMIP6 climate data access — powered by Rust.
> Measured **5561× reduction** in server traffic through OPeNDAP slicing,
> with a **27× speedup** on repeat requests via local cache.

## Status

`search` finds CMIP6 datasets on ESGF, `get` fetches only the slice you
ask for via OPeNDAP (with degree- or index-based selection), `inspect`
decodes the result locally. All tested end-to-end against the live CEDA
node.

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

Ferrous writes the OPeNDAP server's DAP2 binary response directly to
disk. That format is what every OPeNDAP server speaks and is readable
by pydap, xarray's OPeNDAP backend, and Ferrous's own `inspect`
subcommand. THREDDS-backed ESGF nodes like CEDA do not advertise
`.nc` / `.nc4` suffixes, so a local DAP2 → NetCDF4 re-pack is the
planned next step.

## Roadmap

Done:

- [x] ESGF Solr search (Dataset + File records)
- [x] OPeNDAP constraint expression builder
- [x] Polite-mode rate-limited HTTP client
- [x] `search` / `get` / `inspect` subcommands
- [x] Content-addressed local cache — repeat requests = 0 bytes
- [x] DAP2 binary decoder + Grid container support
- [x] Degree-based `--lat-deg` / `--lon-deg` — 1D rectilinear
- [x] 2D curvilinear coordinate resolution — CMIP6 ocean tri-polar grids
- [x] ISO date `--time-iso 2020:2050` via CF time-units + calendar parsing

Next:

- [ ] DAP2 → NetCDF4 re-pack (so output opens in xarray directly)
- [ ] Progress bar on long fetches
- [ ] Multi-file time-chunked dataset assembly
- [ ] `360_day` / `all_leap` / `julian` calendar support
- [ ] PyO3 Python bindings
- [ ] Argovis (Argo float) support

## License

MIT — see [`LICENSE`](LICENSE).
