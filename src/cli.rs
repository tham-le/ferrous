//! CLI argument definitions for the `ferrous` binary.
//!
//! Parsing lives in the library so the binary stays a thin runner and the
//! argument schema is unit-testable without spawning a process. The actual
//! subcommand implementations are in [`crate::commands`].

use clap::{Parser, Subcommand};

/// Top-level CLI.
///
/// `ferrous` is a CMIP6 data fetcher; subcommands cover search and (in
/// upcoming commits) sliced downloads.
#[derive(Debug, Parser)]
#[command(name = "ferrous", version, about, long_about = None)]
pub struct Cli {
    /// Disable polite rate limiting. Use responsibly — ESGF nodes run on
    /// academic infrastructure.
    #[arg(long, global = true)]
    pub no_rate_limit: bool,

    /// Override the ESGF search endpoint URL.
    #[arg(long, global = true)]
    pub endpoint: Option<String>,

    /// Bypass the local response cache for this invocation.
    #[arg(long, global = true)]
    pub no_cache: bool,

    /// Override the local cache directory (default: $FERROUS_CACHE_DIR or
    /// ~/.ferrous/cache).
    #[arg(long, global = true)]
    pub cache_dir: Option<std::path::PathBuf>,

    #[command(subcommand)]
    pub command: Command,
}

/// Subcommands accepted by `ferrous`.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Search ESGF for datasets matching the given CMIP6 facets.
    Search(SearchArgs),
    /// Fetch a sliced NetCDF region via OPeNDAP.
    Get(GetArgs),
    /// Decode a local .dods file and print its DDS + per-variable stats.
    Inspect(InspectArgs),
}

/// Arguments to `ferrous search`.
#[derive(Debug, clap::Args)]
pub struct SearchArgs {
    /// CMIP6 variable id (e.g. `tos`, `tas`, `pr`).
    #[arg(long)]
    pub variable: Option<String>,

    /// CMIP6 experiment id (e.g. `historical`, `ssp245`).
    #[arg(long)]
    pub experiment: Option<String>,

    /// CMIP6 source (model) id (e.g. `CNRM-CM6-1`).
    #[arg(long)]
    pub source: Option<String>,

    /// Output frequency (e.g. `mon`, `day`, `yr`).
    #[arg(long)]
    pub frequency: Option<String>,

    /// Maximum number of datasets to return.
    #[arg(long, default_value_t = 10)]
    pub limit: usize,

    /// Emit results as JSON instead of a human-readable table.
    #[arg(long)]
    pub json: bool,
}

/// Arguments to `ferrous get`.
///
/// Slice arguments use OPeNDAP-style inclusive index bounds. Either
/// `--dataset-id` or the CMIP6 facet triple (variable + experiment + source)
/// may be used; when facets are given Ferrous runs a Dataset search, picks
/// the first match, and enumerates its files.
#[derive(Debug, clap::Args)]
pub struct GetArgs {
    /// Explicit dataset id (e.g.
    /// `CMIP6.ScenarioMIP.CNRM-CERFACS.CNRM-CM6-1.ssp245.r1i1p1f2.Omon.tos.gn.v20190219|esgf.ceda.ac.uk`).
    /// Mutually exclusive with the facet flags.
    #[arg(long, conflicts_with_all = ["experiment", "source", "frequency"])]
    pub dataset_id: Option<String>,

    /// CMIP6 variable id to project (e.g. `tos`). Required.
    #[arg(long)]
    pub variable: String,

    /// CMIP6 experiment id used to resolve a dataset when `--dataset-id` is
    /// absent.
    #[arg(long)]
    pub experiment: Option<String>,

    /// CMIP6 source (model) id used to resolve a dataset when `--dataset-id`
    /// is absent.
    #[arg(long)]
    pub source: Option<String>,

    /// Output frequency filter used during dataset resolution.
    #[arg(long)]
    pub frequency: Option<String>,

    /// Time index slice (`START:STOP` or `START:STRIDE:STOP`).
    #[arg(long)]
    pub time: Option<String>,

    /// Latitude index slice.
    #[arg(long)]
    pub lat: Option<String>,

    /// Longitude index slice.
    #[arg(long)]
    pub lon: Option<String>,

    /// Additional index slices for datasets with extra dimensions (depth,
    /// pressure level, ...). One `--slice` per extra dimension, in declared
    /// order after time/lat/lon.
    #[arg(long = "slice")]
    pub extra: Vec<String>,

    /// When a dataset resolves to multiple files, pick this one (1-indexed).
    /// Default: 1.
    #[arg(long, default_value_t = 1)]
    pub file_index: usize,

    /// Output path for the fetched bytes.
    #[arg(long, short = 'o')]
    pub out: std::path::PathBuf,

    /// Print the constraint URL and resolved file but do not download.
    #[arg(long)]
    pub dry_run: bool,
}

/// Arguments to `ferrous inspect`.
#[derive(Debug, clap::Args)]
pub struct InspectArgs {
    /// Path to a `.dods` file produced by `ferrous get`.
    pub path: std::path::PathBuf,

    /// Treat absolute values >= this as fill / missing data when computing
    /// summary stats. Default matches CMIP6's `1e20` _FillValue convention.
    #[arg(long, default_value_t = 1.0e10)]
    pub fill_threshold: f64,

    /// Print the raw DDS header in addition to per-variable stats.
    #[arg(long)]
    pub dds: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn clap_schema_is_valid() {
        // Panics at test time if any derive is malformed.
        Cli::command().debug_assert();
    }

    #[test]
    fn search_accepts_cmip6_facets() {
        let cli = Cli::try_parse_from([
            "ferrous",
            "search",
            "--variable",
            "tos",
            "--experiment",
            "ssp245",
            "--source",
            "CNRM-CM6-1",
            "--limit",
            "5",
        ])
        .unwrap();
        let Command::Search(args) = cli.command else {
            panic!("expected Search");
        };
        assert_eq!(args.variable.as_deref(), Some("tos"));
        assert_eq!(args.experiment.as_deref(), Some("ssp245"));
        assert_eq!(args.source.as_deref(), Some("CNRM-CM6-1"));
        assert_eq!(args.limit, 5);
        assert!(!args.json);
    }

    #[test]
    fn get_accepts_dataset_id_variable_and_slices() {
        let cli = Cli::try_parse_from([
            "ferrous",
            "get",
            "--dataset-id",
            "some.dataset.id|node",
            "--variable",
            "tos",
            "--time",
            "0:120",
            "--lat",
            "20:30",
            "--lon",
            "40:50",
            "--out",
            "tos.nc",
        ])
        .unwrap();
        let Command::Get(args) = cli.command else {
            panic!("expected Get");
        };
        assert_eq!(args.dataset_id.as_deref(), Some("some.dataset.id|node"));
        assert_eq!(args.variable, "tos");
        assert_eq!(args.time.as_deref(), Some("0:120"));
        assert_eq!(args.lat.as_deref(), Some("20:30"));
        assert_eq!(args.lon.as_deref(), Some("40:50"));
        assert_eq!(args.out, std::path::PathBuf::from("tos.nc"));
        assert!(!args.dry_run);
        assert_eq!(args.file_index, 1);
    }

    #[test]
    fn get_rejects_conflict_between_dataset_id_and_facets() {
        // --dataset-id conflicts_with experiment/source/frequency per the
        // clap schema.
        let result = Cli::try_parse_from([
            "ferrous",
            "get",
            "--dataset-id",
            "x|n",
            "--variable",
            "tos",
            "--experiment",
            "ssp245",
            "--out",
            "tos.nc",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn inspect_takes_a_path_argument() {
        let cli = Cli::try_parse_from(["ferrous", "inspect", "slice.dods", "--dds"]).unwrap();
        let Command::Inspect(args) = cli.command else {
            panic!("expected Inspect");
        };
        assert_eq!(args.path, std::path::PathBuf::from("slice.dods"));
        assert!(args.dds);
    }

    #[test]
    fn inspect_requires_a_path() {
        assert!(Cli::try_parse_from(["ferrous", "inspect"]).is_err());
    }

    #[test]
    fn get_requires_variable_and_out() {
        assert!(Cli::try_parse_from(["ferrous", "get"]).is_err());
        assert!(
            Cli::try_parse_from(["ferrous", "get", "--variable", "tos"]).is_err(),
            "missing --out must fail"
        );
    }

    #[test]
    fn no_rate_limit_is_global() {
        let cli = Cli::try_parse_from(["ferrous", "--no-rate-limit", "search"]).unwrap();
        assert!(cli.no_rate_limit);
    }

    #[test]
    fn endpoint_override_is_global() {
        let cli = Cli::try_parse_from([
            "ferrous",
            "--endpoint",
            "https://example.org/search",
            "search",
        ])
        .unwrap();
        assert_eq!(cli.endpoint.as_deref(), Some("https://example.org/search"));
    }

    #[test]
    fn unknown_subcommand_is_rejected() {
        assert!(Cli::try_parse_from(["ferrous", "bogus"]).is_err());
    }

    #[test]
    fn limit_defaults_to_ten() {
        let cli = Cli::try_parse_from(["ferrous", "search"]).unwrap();
        let Command::Search(args) = cli.command else {
            panic!("expected Search");
        };
        assert_eq!(args.limit, 10);
    }
}
