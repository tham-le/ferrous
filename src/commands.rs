//! Subcommand implementations.
//!
//! Each function here takes already-parsed CLI arguments and returns
//! `Result<()>`, so the binary stays a thin dispatcher.

use crate::cli::{Cli, SearchArgs};
use crate::esgf::{Dataset, SearchClient, SearchQuery, DEFAULT_SEARCH_ENDPOINT};
use crate::http::{Client, ClientBuilder, RateLimiter};
use crate::{Error, Result};

/// Build an HTTP client from global CLI flags.
pub fn build_http(cli: &Cli) -> Result<Client> {
    let mut builder = ClientBuilder::default();
    if cli.no_rate_limit {
        builder = builder.rate_limiter(RateLimiter::unlimited());
    }
    builder.build()
}

/// Run `ferrous search`.
pub async fn run_search(cli: &Cli, args: &SearchArgs) -> Result<()> {
    let http = build_http(cli)?;
    let endpoint = cli
        .endpoint
        .clone()
        .unwrap_or_else(|| DEFAULT_SEARCH_ENDPOINT.to_string());
    let client = SearchClient::new(http, &endpoint);

    let mut query = SearchQuery::cmip6().limit(args.limit);
    if let Some(v) = &args.variable {
        query = query.variable(v);
    }
    if let Some(v) = &args.experiment {
        query = query.experiment(v);
    }
    if let Some(v) = &args.source {
        query = query.source(v);
    }
    if let Some(v) = &args.frequency {
        query = query.frequency(v);
    }

    let results = match client.search(&query).await {
        Ok(r) => r,
        Err(Error::NoResults) => {
            println!("No datasets match the query.");
            return Ok(());
        }
        Err(e) => return Err(e),
    };

    if args.json {
        // Minimal JSON shape — id, title, and first OPeNDAP URL are what a
        // downstream script cares about. Full structured JSON export is a
        // follow-up.
        let rows: Vec<_> = results
            .datasets
            .iter()
            .map(|d| {
                serde_json::json!({
                    "id": d.id,
                    "title": d.title,
                    "variable_id": d.variable_id,
                    "source_id": d.source_id,
                    "experiment_id": d.experiment_id,
                    "frequency": d.frequency,
                    "opendap_url": d.opendap_url(),
                })
            })
            .collect();
        let out = serde_json::json!({
            "total": results.total,
            "datasets": rows,
        });
        println!("{}", serde_json::to_string_pretty(&out).unwrap());
    } else {
        println!(
            "Found {} dataset(s) (showing {}):",
            results.total,
            results.datasets.len()
        );
        for (i, ds) in results.datasets.iter().enumerate() {
            print_dataset(i + 1, ds);
        }
    }
    Ok(())
}

fn print_dataset(n: usize, ds: &Dataset) {
    println!("\n[{n}] {}", ds.id);
    if !ds.title.is_empty() && ds.title != ds.id {
        println!("    title:     {}", ds.title);
    }
    if !ds.variable_id.is_empty() {
        println!("    variable:  {}", ds.variable_id.join(", "));
    }
    if !ds.source_id.is_empty() {
        println!("    model:     {}", ds.source_id.join(", "));
    }
    if !ds.experiment_id.is_empty() {
        println!("    scenario:  {}", ds.experiment_id.join(", "));
    }
    if !ds.frequency.is_empty() {
        println!("    frequency: {}", ds.frequency.join(", "));
    }
    if let Some(url) = ds.opendap_url() {
        println!("    opendap:   {url}");
    }
}
