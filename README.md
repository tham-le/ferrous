# Ferrous

> Fast, ergonomic CMIP6 climate data access — powered by Rust.
> Reduces server traffic by up to 95% through OPeNDAP slicing and local caching.

## Status

Early development. See [`FERROUS.md`](FERROUS.md) for the full project spec and
[`GITHUB_RESEARCH.md`](GITHUB_RESEARCH.md) for reference tools studied during design.

## Quick start

```bash
cargo build --release
./target/release/ferrous --help
```

## Planned CLI

```bash
# Search available datasets
ferrous search --variable tos --model CNRM-CM6 --scenario ssp245

# Fetch only the slice you need — 200KB instead of 4GB
ferrous get --variable tos --lat 43:46 --lon 5:7 --years 2020-2050 --out sst.nc
```

## License

MIT — see [`LICENSE`](LICENSE).
