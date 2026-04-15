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
# Degree-based selection (1D rectilinear grids — most CMIP6 atmospheric data)
ferrous get --variable tas --experiment ssp245 --source CNRM-CM6-1 \
  --lat-deg 30:46 --lon-deg 0:30 --time 0:11 \
  --out tas_med.dods

# Or array-index selection (works for every grid, including 2D ocean curvilinear)
ferrous get \
  --dataset-id "CMIP6.ScenarioMIP.CNRM-CERFACS.CNRM-CM6-1.ssp245.r1i1p1f2.Omon.tos.gn.v20190219|esgf.ceda.ac.uk" \
  --variable tos \
  --time 0:11 --lat 100:140 --lon 50:80 \
  --out tos_slice.dods

# Print the constructed OPeNDAP URL without downloading
ferrous get --dataset-id "…" --variable tos --time 0:0 --dry-run --out /dev/null

# Bypass the local cache for one invocation
ferrous --no-cache get --variable tas --lat-deg 0:10 --lon-deg 0:10 --out tas.dods
```

`--lat-deg` / `--lon-deg` fetch the file's 1D coordinate axes via
OPeNDAP, resolve to inclusive index ranges, and feed the result back
into the constraint. 2D curvilinear grids (CMIP6 ocean tri-polar)
error out clearly — use `--lat` / `--lon` index ranges there.

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

- [x] ESGF Solr search (Dataset + File)
- [x] OPeNDAP constraint expression builder
- [x] Polite-mode rate-limited HTTP client
- [x] `search` subcommand
- [x] `get` subcommand — index slicing
- [x] Local cache (content-addressed, never re-fetch)
- [x] DAP2 binary decoder + Grid container support
- [x] `inspect` subcommand — local DAP2 introspection
- [x] Degree-based `--lat-deg` / `--lon-deg` (1D rectilinear)
- [ ] Date-based `--time-iso 2020-2050` resolution
- [ ] 2D curvilinear coordinate resolution (CMIP6 ocean grids)
- [ ] DAP2 → NetCDF4 re-pack
- [ ] Progress bar on long fetches
- [ ] Multi-file time-chunked dataset assembly
- [ ] PyO3 Python bindings
- [ ] Argovis (Argo float) support

## License

MIT — see [`LICENSE`](LICENSE).
