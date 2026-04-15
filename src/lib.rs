//! Ferrous — fast, ergonomic CMIP6 climate data access.
//!
//! This crate is the core library behind the `ferrous` CLI. It exposes primitives
//! for querying ESGF search nodes, building OPeNDAP constraint expressions, and
//! fetching sliced NetCDF data without downloading entire files.
//!
//! Modules are added incrementally as the project grows; see `FERROUS.md` for the
//! roadmap.

pub mod cache;
pub mod cf_time;
pub mod cli;
pub mod commands;
pub mod coords;
pub mod dap2;
pub mod error;
pub mod esgf;
pub mod http;
pub mod opendap;

pub use error::{Error, Result};

/// Crate version as declared in `Cargo.toml`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
