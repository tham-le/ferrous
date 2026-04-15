# Ferrous

> Fast, ergonomic CMIP6 climate data access — powered by Rust.
> Measured 3444× reduction in server traffic through OPeNDAP slicing.

## Status

Early development, but the core pitch is working end-to-end: `search`
discovers CMIP6 datasets on ESGF, `get` fetches only the slice you ask
for via OPeNDAP instead of downloading the full NetCDF file.

## Quick start

```bash
cargo build --release
./target/release/ferrous --help
```

## Measured traffic reduction

Live against the CEDA ESGF node, 12 months × 41 lat × 31 lon slice of a
CMIP6 sea-surface-temperature dataset:

```
full file:  210 893 610 bytes (~210.9 MB)
slice:           61 226 bytes (~0.06 MB)
                ─────────────
reduction:  3444× (0.03% of the full transfer)
```

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
# By explicit dataset id (from `ferrous search`)
ferrous get \
  --dataset-id "CMIP6.ScenarioMIP.CNRM-CERFACS.CNRM-CM6-1.ssp245.r1i1p1f2.Omon.tos.gn.v20190219|esgf.ceda.ac.uk" \
  --variable tos \
  --time 0:11 --lat 100:140 --lon 50:80 \
  --out tos_slice.dods

# Or by facets (first match wins — pass --dataset-id to pin)
ferrous get --variable tos --experiment ssp245 --source CNRM-CM6-1 \
  --time 0:11 --lat 100:140 --lon 50:80 \
  --out tos_slice.dods

# --dry-run prints the constructed OPeNDAP URL without downloading
ferrous get --dataset-id "…" --variable tos --time 0:0 --dry-run --out /dev/null
```

Slice arguments are **array indices**, inclusive bounds, matching the
DAP2 spec. Degree/date resolution (`--lat 43:46 --lon 5:7
--years 2020-2050`) is planned but not wired yet.

### Output format

Today Ferrous writes the OPeNDAP server's DAP2 binary response directly
to disk. That format is what every OPeNDAP server speaks and is readable
by pydap, xarray's OPeNDAP backend, nctoolbox, and FERRET's USE command
(with a DAP URL). THREDDS-backed ESGF nodes like CEDA do not advertise
the `.nc` / `.nc4` suffix, so a local DAP2 → NetCDF4 re-pack is the
planned next step.

## Roadmap

- [x] ESGF Solr search (Dataset + File)
- [x] OPeNDAP constraint expression builder
- [x] Polite-mode rate-limited HTTP client
- [x] `search` subcommand
- [x] `get` subcommand — index slicing
- [ ] Degree/date coordinate resolution (`--lat 43:46` in degrees)
- [ ] DAP2 → NetCDF4 re-pack
- [ ] Local cache (content-addressed, never re-fetch)
- [ ] Progress bar on long fetches
- [ ] Multi-file time-chunked dataset assembly
- [ ] PyO3 Python bindings
- [ ] Argovis (Argo float) support

## License

MIT — see [`LICENSE`](LICENSE).
