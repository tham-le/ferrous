//! PyO3 bindings for the `ferrous` library, gated behind the `python`
//! Cargo feature.
//!
//! Build via maturin:
//!
//! ```bash
//! pip install maturin
//! maturin develop --features python  # debug build, installs into current venv
//! # or: maturin build --release --features python   # produce a wheel
//! ```
//!
//! Usage from Python:
//!
//! ```python
//! import ferrous
//!
//! nc_path = ferrous.get(
//!     dataset_id="CMIP6.ScenarioMIP.CNRM-CERFACS.CNRM-CM6-1.ssp245"
//!                ".r1i1p1f2.Amon.tas.gr.v20190219|esgf.ceda.ac.uk",
//!     variable="tas",
//!     out="/tmp/tas_med.nc",
//!     time_iso="2020:2025",
//!     lat_deg="30:46",
//!     lon_deg="0:30",
//! )
//!
//! import xarray as xr
//! xr.open_dataset(nc_path)
//! ```

use std::path::PathBuf;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::cli::{Cli, Command, GetArgs, OutputFormat};

/// Build a synthetic [`Cli`] + [`GetArgs`] from Python keyword arguments and
/// dispatch through [`crate::commands::run_get`] on a fresh Tokio runtime.
///
/// Returns the output path on success; on failure, raises
/// `RuntimeError` with the underlying Ferrous error message.
#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (
    dataset_id,
    variable,
    out,
    *,
    time = None,
    time_iso = None,
    lat = None,
    lon = None,
    lat_deg = None,
    lon_deg = None,
    lat_coord = None,
    lon_coord = None,
    file_index = 1,
    all_files = false,
    format = None,
    no_cache = false,
    cache_dir = None,
    no_rate_limit = false,
    endpoint = None,
))]
fn get(
    py: Python<'_>,
    dataset_id: String,
    variable: String,
    out: PathBuf,
    time: Option<String>,
    time_iso: Option<String>,
    lat: Option<String>,
    lon: Option<String>,
    lat_deg: Option<String>,
    lon_deg: Option<String>,
    lat_coord: Option<String>,
    lon_coord: Option<String>,
    file_index: usize,
    all_files: bool,
    format: Option<&str>,
    no_cache: bool,
    cache_dir: Option<PathBuf>,
    no_rate_limit: bool,
    endpoint: Option<String>,
) -> PyResult<PathBuf> {
    let format = match format {
        None => None,
        Some("nc") => Some(OutputFormat::Nc),
        Some("dods") => Some(OutputFormat::Dods),
        Some(other) => {
            return Err(PyRuntimeError::new_err(format!(
                "format must be 'nc' or 'dods', got '{other}'"
            )));
        }
    };

    let args = GetArgs {
        dataset_id: Some(dataset_id),
        variable,
        experiment: None,
        source: None,
        frequency: None,
        time,
        time_iso,
        lat,
        lon,
        lat_deg,
        lon_deg,
        lat_coord,
        lon_coord,
        extra: Vec::new(),
        file_index,
        all_files,
        out: out.clone(),
        format,
        dry_run: false,
    };
    let cli = Cli {
        no_rate_limit,
        endpoint,
        no_cache,
        cache_dir,
        command: Command::Get(args.clone()),
    };

    // Release the GIL while we do network I/O. The Tokio runtime drives
    // reqwest + the progress bar; nothing inside run_get touches Python
    // objects, so it's safe to drop the GIL for the whole block.
    py.allow_threads(|| -> PyResult<()> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| PyRuntimeError::new_err(format!("tokio runtime: {e}")))?;
        rt.block_on(async {
            crate::commands::run_get(&cli, &args)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("ferrous: {e}")))
        })
    })?;

    Ok(out)
}

/// Top-level PyO3 module registration.
#[pymodule]
fn ferrous(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", crate::VERSION)?;
    m.add_function(wrap_pyfunction!(get, m)?)?;
    Ok(())
}
