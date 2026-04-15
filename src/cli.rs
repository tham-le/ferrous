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

    #[command(subcommand)]
    pub command: Command,
}

/// Subcommands accepted by `ferrous`.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Search ESGF for datasets matching the given CMIP6 facets.
    Search(SearchArgs),
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
        let Command::Search(args) = cli.command;
        assert_eq!(args.variable.as_deref(), Some("tos"));
        assert_eq!(args.experiment.as_deref(), Some("ssp245"));
        assert_eq!(args.source.as_deref(), Some("CNRM-CM6-1"));
        assert_eq!(args.limit, 5);
        assert!(!args.json);
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
        let Command::Search(args) = cli.command;
        assert_eq!(args.limit, 10);
    }
}
