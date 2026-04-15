# Ferrous usage examples

Three short, runnable demos that take Ferrous output into the tools
researchers actually use: **xarray**, **Jupyter**, and **PyFerret**.

Every example pulls the same slice so you can compare:

| Field | Value |
|---|---|
| Variable | `tas` (near-surface air temperature) |
| Model | `CNRM-CM6-1` |
| Scenario | `ssp245` |
| Region | Mediterranean — 30°–46°N, 0°–30°E |
| Time | 2020–2025 (72 monthly steps) |
| Full file | ~74 MB |
| Sliced | ~77 KB (**~1000× reduction**) |

## Prerequisites

```bash
# Build the Ferrous CLI
cargo build --release

# Put it on your PATH (or use target/release/ferrous below)
export PATH="$PWD/target/release:$PATH"
```

---

## 1. xarray (plain Python) — `xarray_quickstart.py`

Shortest path from Ferrous output to a plotted figure.

```bash
pip install xarray netCDF4 matplotlib
./examples/run_xarray.sh
```

The script shells out to `ferrous get` for the slice, then hands the
resulting `.nc` file to xarray. **Zero boilerplate** — the whole
analysis is eight lines of Python.

---

## 2. Jupyter notebook — `xarray_quickstart.ipynb`

The same workflow as above, but organised as notebook cells with
explanatory markdown. Good for interactive exploration, teaching, or
sharing results.

```bash
pip install jupyter xarray netCDF4 matplotlib
jupyter notebook examples/xarray_quickstart.ipynb
```

---

## 3. PyFerret — `pyferret_quickstart.jnl` + `pyferret_quickstart.py`

FERRET is still the reference tool at many ocean/atmosphere research
labs. Ferrous output feeds straight into the `USE` command; existing
journal files need no changes beyond pointing at the Ferrous-produced
file instead of a hand-downloaded 4 GB one.

```bash
# Journal-file style (vanilla FERRET)
pyferret < examples/pyferret_quickstart.jnl

# Or Python-driven (pyferret module)
python examples/pyferret_quickstart.py
```

---

## Output format

Ferrous can write either NetCDF-3 (`.nc`, xarray/FERRET-friendly) or
DAP2 binary (`.dods`, the OPeNDAP wire format). The examples all use
`.nc`; override with `--format dods` if you need the raw DAP2 for
debugging or pydap.

## See also

- The [`ferrous inspect`](../src/commands.rs) subcommand decodes a
  local `.dods` or `.nc` file without leaving the CLI — handy for
  sanity-checking before importing into a notebook.
- The top-level [`README.md`](../README.md) covers the CLI options in
  full.
