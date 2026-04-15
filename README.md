# Ferrous

> Fast, ergonomic CMIP6 climate data access — powered by Rust.
> Reduces server traffic by up to 95% through OPeNDAP slicing and local caching.

## Status

Early development. `search` is wired up end-to-end against live ESGF nodes;
`get` (sliced OPeNDAP download + NetCDF output) is next.

## Quick start

```bash
cargo build --release
./target/release/ferrous --help
```

## What works today

```bash
# Search CMIP6 datasets matching the given facets
ferrous search --variable tos --experiment ssp245 --source CNRM-CM6-1 --limit 5

# JSON output for scripting
ferrous search --variable tos --experiment ssp245 --json

# Override the search endpoint (IPSL currently returns 500s; CEDA is reliable)
ferrous --endpoint https://esgf.ceda.ac.uk/esg-search/search search --variable tos
```

## Planned

```bash
# Fetch only the slice you need — ~200KB instead of ~4GB
ferrous get --variable tos --lat 43:46 --lon 5:7 --years 2020-2050 --out sst.nc

# Second request — served from local cache, zero server traffic
ferrous get --variable tos --lat 43:46 --lon 5:7 --years 2020-2050
```

## License

MIT — see [`LICENSE`](LICENSE).
