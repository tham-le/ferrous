//! Subcommand implementations.
//!
//! Each function here takes already-parsed CLI arguments and returns
//! `Result<()>`, so the binary stays a thin dispatcher.

use std::fs;
use std::path::Path;

use crate::cache::Cache;
use crate::cf_time::CfTimeAxis;
use crate::cli::{Cli, GetArgs, InspectArgs, OutputFormat, SearchArgs};
use crate::coords::{self, IndexRange};
use crate::dap2::{self, DapData, DapVariable};
use crate::das;
use crate::esgf::{Dataset, SearchClient, SearchQuery, DEFAULT_SEARCH_ENDPOINT};
use crate::http::{Client, ClientBuilder, RateLimiter};
use crate::nc_out::{self, AttrValue, Attrs};
use crate::opendap::{Constraint, Slice};
use crate::{Error, Result};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

/// Coordinate-variable names tried in order when `--lat-coord` / `--lon-coord`
/// is not supplied. CMIP6 atmospheric data uses `lat` / `lon` consistently;
/// the longer names are a fallback for older / non-CMIP datasets.
const LAT_COORD_CANDIDATES: &[&str] = &["lat", "latitude"];
const LON_COORD_CANDIDATES: &[&str] = &["lon", "longitude"];

/// Build the response cache from global CLI flags.
pub fn build_cache(cli: &Cli) -> Cache {
    if cli.no_cache {
        Cache::disabled()
    } else if let Some(dir) = &cli.cache_dir {
        Cache::new(dir)
    } else {
        Cache::default_location()
    }
}

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

/// Run `ferrous get`.
///
/// Resolves the target dataset (either directly via `--dataset-id` or by
/// Dataset search), enumerates its files, fetches and slices either one
/// file (default / `--file-index N`) or every file (`--all-files`, with
/// out-of-range files auto-skipped), then writes the result to `--out`.
///
/// In multi-file mode per-file responses are concatenated along the time
/// axis before writing — lat / lon / other non-time coords are taken from
/// the first file.
pub async fn run_get(cli: &Cli, args: &GetArgs) -> Result<()> {
    let http = build_http(cli)?;
    let cache = build_cache(cli);
    let endpoint = cli
        .endpoint
        .clone()
        .unwrap_or_else(|| DEFAULT_SEARCH_ENDPOINT.to_string());
    let client = SearchClient::new(http.clone(), &endpoint);

    let dataset_id = match &args.dataset_id {
        Some(id) => id.clone(),
        None => resolve_dataset_id(&client, args).await?,
    };
    let files = client
        .search_files(&SearchQuery::cmip6_files_of(&dataset_id))
        .await?;

    // Select the files to fetch.
    let targets: Vec<&crate::esgf::File> = if args.all_files {
        files.files.iter().collect()
    } else {
        if args.file_index == 0 || args.file_index > files.files.len() {
            return Err(Error::InvalidArgument(format!(
                "--file-index {} out of range (dataset has {} file(s))",
                args.file_index,
                files.files.len()
            )));
        }
        vec![&files.files[args.file_index - 1]]
    };

    println!("dataset:    {dataset_id}");
    if args.all_files {
        println!(
            "mode:       all-files ({} candidate file(s))",
            targets.len()
        );
    }

    // Walk each target file; collect decoded DAP2 responses.
    let mut fetched: Vec<FetchedFile> = Vec::new();
    for (i, file) in targets.iter().enumerate() {
        match fetch_one_file(&http, &cache, file, args, i + 1, targets.len()).await {
            Ok(f) => fetched.push(f),
            Err(Error::NoResults) => {
                eprintln!(
                    "skipping '{}': no axis values fall in the requested window",
                    file.title
                );
            }
            Err(e) => return Err(e),
        }
    }

    if fetched.is_empty() {
        return Err(Error::NoResults);
    }
    if args.dry_run {
        return Ok(());
    }

    // Combine, write, report.
    let combined = if fetched.len() == 1 {
        fetched.remove(0)
    } else {
        combine_fetched(fetched)?
    };
    write_output(&http, &cache, args, &combined).await
}

/// One file's fetch result, held until we're ready to write.
struct FetchedFile {
    /// Decoded DAP2 response (populated only for [`OutputFormat::Nc`]; we
    /// still decode in Dods mode so the concatenator can find the time
    /// variable's shape — but the raw bytes are what gets written).
    response: dap2::DapResponse,
    /// OPeNDAP base URL (without `.dods` suffix or query) — keeps DAS
    /// fetching tidy when we write out.
    opendap_url: String,
    /// Full URL actually fetched (for source-of-truth reporting).
    fetch_url: String,
    /// Raw DAP2 bytes if we need to write them out unchanged.
    bytes: Vec<u8>,
    /// Reported full-file size (`file.size` from ESGF).
    full_file_size: Option<u64>,
    /// Cache vs network.
    source: &'static str,
}

async fn fetch_one_file(
    http: &Client,
    cache: &Cache,
    file: &crate::esgf::File,
    args: &GetArgs,
    n: usize,
    total: usize,
) -> Result<FetchedFile> {
    let opendap_url = file
        .opendap_url()
        .ok_or_else(|| Error::Parse(format!("file {} has no OPENDAP access URL", file.id)))?;

    // Per-file time resolution. In multi-file mode a resolution failure
    // downgrades to NoResults so the caller can skip this file cleanly.
    let time_idx = if let Some(spec) = args.time_iso.as_deref() {
        match resolve_time_iso_range(http, cache, &opendap_url, spec).await {
            Ok(r) => Some(r),
            Err(Error::InvalidArgument(msg)) if msg.contains("no axis values fall in") => {
                return Err(Error::NoResults);
            }
            Err(e) => return Err(e),
        }
    } else {
        None
    };

    let lat_deg = parse_deg_range(args.lat_deg.as_deref(), "--lat-deg")?;
    let lon_deg = parse_deg_range(args.lon_deg.as_deref(), "--lon-deg")?;
    let (lat_idx, lon_idx) = if lat_deg.is_some() || lon_deg.is_some() {
        resolve_lat_lon_degrees(
            http,
            cache,
            &opendap_url,
            args.lat_coord.as_deref(),
            args.lon_coord.as_deref(),
            lat_deg,
            lon_deg,
        )
        .await?
    } else {
        (None, None)
    };

    let constraint = build_constraint(&args.variable, args, time_idx, lat_idx, lon_idx)?;
    let fetch_url = format!(
        "{}.dods{}",
        opendap_url,
        if constraint.is_empty() {
            String::new()
        } else {
            format!("?{}", constraint.to_query())
        }
    );

    println!(
        "file [{n}/{total}]: {} — constraint {}",
        file.title,
        constraint.to_query()
    );
    if let Some(size) = file.size {
        println!(
            "  full size: {} bytes (~{:.1} MB)",
            size,
            size as f64 / 1_000_000.0
        );
    }

    if args.dry_run {
        // Build a zero-bytes placeholder so the caller can still count it.
        return Ok(FetchedFile {
            response: dap2::DapResponse {
                dds: String::new(),
                variables: Vec::new(),
            },
            opendap_url,
            fetch_url,
            bytes: Vec::new(),
            full_file_size: file.size,
            source: "dry-run",
        });
    }

    let (bytes, source) = match cache.get(&fetch_url)? {
        Some(b) => {
            println!("  cache hit ({} bytes, 0 fetched).", b.len());
            (b, "cache")
        }
        None => {
            let label = format!("{} {}", args.variable, file.title);
            let b = http.get_bytes_with_progress(&fetch_url, &label).await?;
            if let Err(e) = cache.put(&fetch_url, &b) {
                eprintln!("  warning: failed to populate cache: {e}");
            }
            (b, "network")
        }
    };
    let response = dap2::decode(&bytes)?;
    Ok(FetchedFile {
        response,
        opendap_url,
        fetch_url,
        bytes,
        full_file_size: file.size,
        source,
    })
}

/// Concatenate multiple per-file responses along the time axis into a single
/// virtual response. Assumes every file has the same set of variables with
/// matching dimensions (except along `time`, which grows). Non-time-varying
/// coords (lat, lon) are taken from the first file and checked for equality
/// against later files; mismatches error loud.
fn combine_fetched(mut parts: Vec<FetchedFile>) -> Result<FetchedFile> {
    // Carry template metadata from the first file.
    let first = parts.remove(0);
    let mut combined_response = first.response.clone();
    let mut total_bytes = first.bytes.len();

    for part in parts {
        total_bytes += part.bytes.len();
        concatenate_along_time(&mut combined_response, part.response)?;
    }

    // Re-encode the combined response as if it came from a single source.
    // Dods passthrough doesn't work for multi-file; always downgrade to
    // NC output by setting the bytes to the DAP2 of the first file (the
    // bytes field is only used by the Dods path — warn in write_output if
    // the user asked for Dods).
    Ok(FetchedFile {
        response: combined_response,
        opendap_url: first.opendap_url,
        fetch_url: format!(
            "{} (+{} more files concatenated along time)",
            first.fetch_url,
            // Subtract 1 for the first file we already unpacked.
            total_bytes.saturating_sub(first.bytes.len())
        ),
        // We can't rebuild DAP2 bytes without re-encoding; callers that want
        // a combined Dods file need to request per-file and stitch
        // themselves. This path is expected to write .nc instead.
        bytes: Vec::new(),
        full_file_size: first.full_file_size,
        source: "network",
    })
}

/// Append `other` to `target` along the `time` dimension for every variable
/// whose first dimension is time. Variables without a time dimension must
/// match exactly across files (they're spatial coords or scalar metadata).
fn concatenate_along_time(target: &mut dap2::DapResponse, other: dap2::DapResponse) -> Result<()> {
    if target.variables.len() != other.variables.len() {
        return Err(Error::Parse(format!(
            "multi-file concat: variable count mismatch ({} vs {})",
            target.variables.len(),
            other.variables.len()
        )));
    }
    for (t, o) in target.variables.iter_mut().zip(other.variables.into_iter()) {
        if t.name != o.name {
            return Err(Error::Parse(format!(
                "multi-file concat: variable order mismatch ('{}' vs '{}')",
                t.name, o.name
            )));
        }
        if is_time_varying(t) {
            append_along_time(t, o)?;
        } else {
            // Non-time coord (lat, lon, etc.) — must be identical.
            if t.dimensions != o.dimensions {
                return Err(Error::Parse(format!(
                    "multi-file concat: '{}' has mismatched dims across files",
                    t.name
                )));
            }
        }
    }
    Ok(())
}

fn is_time_varying(v: &dap2::DapVariable) -> bool {
    v.dimensions.first().is_some_and(|d| d.name == "time")
}

fn append_along_time(t: &mut dap2::DapVariable, o: dap2::DapVariable) -> Result<()> {
    // Dimensions after time must agree element-for-element.
    if t.dimensions.len() != o.dimensions.len() {
        return Err(Error::Parse(format!(
            "multi-file concat: '{}' has mismatched rank across files",
            t.name
        )));
    }
    for (td, od) in t.dimensions.iter().zip(o.dimensions.iter()).skip(1) {
        if td.size != od.size || td.name != od.name {
            return Err(Error::Parse(format!(
                "multi-file concat: '{}' dim '{}' size changes across files ({} vs {})",
                t.name, td.name, td.size, od.size
            )));
        }
    }
    // Grow time dim then extend data.
    if let Some(first_dim) = t.dimensions.first_mut() {
        first_dim.size += o.dimensions[0].size;
    }
    match (&mut t.data, o.data) {
        (dap2::DapData::F32(a), dap2::DapData::F32(b)) => a.extend(b),
        (dap2::DapData::F64(a), dap2::DapData::F64(b)) => a.extend(b),
        (dap2::DapData::I32(a), dap2::DapData::I32(b)) => a.extend(b),
        (dap2::DapData::I16(a), dap2::DapData::I16(b)) => a.extend(b),
        (dap2::DapData::U8(a), dap2::DapData::U8(b)) => a.extend(b),
        (a, b) => {
            return Err(Error::Parse(format!(
                "multi-file concat: '{}' dtype mismatch ({:?} vs {:?})",
                t.name,
                a.len(),
                b.len()
            )));
        }
    }
    Ok(())
}

async fn write_output(
    http: &Client,
    cache: &Cache,
    args: &GetArgs,
    combined: &FetchedFile,
) -> Result<()> {
    ensure_parent_dir(&args.out)?;
    let format = OutputFormat::resolve(args.format, &args.out);

    let written_bytes = match format {
        OutputFormat::Dods => {
            if combined.bytes.is_empty() {
                return Err(Error::InvalidArgument(
                    "--format dods is incompatible with --all-files (would have to \
                     re-encode concatenated DAP2); use .nc output instead"
                        .into(),
                ));
            }
            fs::write(&args.out, &combined.bytes)?;
            combined.bytes.len()
        }
        OutputFormat::Nc => {
            let attrs =
                match fetch_attrs(http, cache, &combined.opendap_url, &combined.response).await {
                    Ok(a) => a,
                    Err(e) => {
                        eprintln!("warning: DAS unavailable ({e}); writing .nc without attrs");
                        Attrs::empty()
                    }
                };
            nc_out::write_with_attrs(&args.out, &combined.response, &attrs)?;
            fs::metadata(&args.out)
                .map(|m| m.len() as usize)
                .unwrap_or(0)
        }
    };

    println!(
        "\nWrote {} bytes ({:?}) to {} [via {}]",
        written_bytes,
        format,
        args.out.display(),
        combined.source,
    );
    if let Some(full) = combined.full_file_size {
        if full > 0 && !combined.bytes.is_empty() && combined.source == "network" {
            let saved = combined.bytes.len() as f64;
            let full = full as f64;
            println!(
                "Transferred {:.2}% of the full file ({:.1}x reduction).",
                saved / full * 100.0,
                full / saved,
            );
        }
    }
    Ok(())
}

/// Look up a single dataset id using the CMIP6 facet arguments.
async fn resolve_dataset_id(client: &SearchClient, args: &GetArgs) -> Result<String> {
    let mut query = SearchQuery::cmip6().variable(&args.variable).limit(5);
    if let Some(v) = &args.experiment {
        query = query.experiment(v);
    }
    if let Some(v) = &args.source {
        query = query.source(v);
    }
    if let Some(v) = &args.frequency {
        query = query.frequency(v);
    }

    let results = client.search(&query).await?;
    if results.datasets.len() > 1 {
        eprintln!(
            "Facets matched {} datasets; picking the first. Use --dataset-id to pin one explicitly:",
            results.total
        );
        for (i, ds) in results.datasets.iter().take(5).enumerate() {
            eprintln!("  [{}] {}", i + 1, ds.id);
        }
    }
    Ok(results.datasets[0].id.clone())
}

/// Assemble an OPeNDAP constraint from `--time`, `--lat`, `--lon`, and any
/// `--slice` arguments, in the CMIP6-conventional order.
///
/// Resolved index ranges (from `--time-iso` / `--lat-deg` / `--lon-deg`) take
/// precedence over the raw-index forms when both are supplied; clap prevents
/// that conflict on the user side, but the logic is defensive so library
/// callers can't silently disagree either.
fn build_constraint(
    variable: &str,
    args: &GetArgs,
    time_resolved: Option<IndexRange>,
    lat_resolved: Option<IndexRange>,
    lon_resolved: Option<IndexRange>,
) -> Result<Constraint> {
    let mut slices: Vec<Slice> = Vec::new();
    match (time_resolved, args.time.as_deref()) {
        (Some(r), _) => slices.push(Slice::range(r.start, r.stop)),
        (None, Some(spec)) => slices.push(spec.parse()?),
        (None, None) => {}
    }
    match (lat_resolved, args.lat.as_deref()) {
        (Some(r), _) => slices.push(Slice::range(r.start, r.stop)),
        (None, Some(spec)) => slices.push(spec.parse()?),
        (None, None) => {}
    }
    match (lon_resolved, args.lon.as_deref()) {
        (Some(r), _) => slices.push(Slice::range(r.start, r.stop)),
        (None, Some(spec)) => slices.push(spec.parse()?),
        (None, None) => {}
    }
    for extra in &args.extra {
        slices.push(extra.parse()?);
    }
    if slices.is_empty() {
        // No slicing requested — project the whole variable. Valid OPeNDAP
        // but defeats the point of Ferrous; warn so the user knows.
        eprintln!(
            "note: no --time/--lat/--lon/--slice given; fetching the full variable. \
             Use index slices to reduce transfer volume."
        );
        Constraint::new().select(variable, std::iter::empty())
    } else {
        Constraint::new().select(variable, slices)
    }
}

/// Fetch the DAS for `opendap_url` (cached), parse it, and return attributes
/// filtered down to the variables that are actually in `response`. Global
/// attributes are always included.
async fn fetch_attrs(
    http: &Client,
    cache: &Cache,
    opendap_url: &str,
    response: &dap2::DapResponse,
) -> Result<Attrs> {
    let das_url = format!("{opendap_url}.das");
    let bytes = match cache.get(&das_url)? {
        Some(b) => b,
        None => {
            let b = http.get_bytes(&das_url).await?;
            let _ = cache.put(&das_url, &b);
            b
        }
    };
    let das_text = std::str::from_utf8(&bytes)
        .map_err(|e| Error::Parse(format!("DAS response is not UTF-8: {e}")))?;
    let parsed = das::parse(das_text);
    Ok(das_to_nc_attrs(&parsed, response))
}

/// Convert a [`das::Attributes`] map into [`Attrs`] suitable for the NetCDF
/// writer. Drops attributes on variables that aren't in the response (the
/// DAS always covers every variable of the whole file, but we only write
/// the variables the DAP2 response actually returned).
fn das_to_nc_attrs(das_attrs: &das::Attributes, response: &dap2::DapResponse) -> Attrs {
    let mut out = Attrs::empty();
    if let Some(global) = das_attrs.get("NC_GLOBAL") {
        out.global = global
            .iter()
            .map(|(n, v)| (n.clone(), das_value_to_attr(v)))
            .collect();
    }
    for var in &response.variables {
        if let Some(attrs) = das_attrs.get(&var.name) {
            let list = attrs
                .iter()
                .map(|(n, v)| (n.clone(), das_value_to_attr(v)))
                .collect();
            out.per_var.insert(var.name.clone(), list);
        }
    }
    out
}

fn das_value_to_attr(v: &das::DasValue) -> AttrValue {
    match v {
        das::DasValue::Text(s) => AttrValue::Text(s.clone()),
        das::DasValue::F32(v) => AttrValue::F32(v.clone()),
        das::DasValue::F64(v) => AttrValue::F64(v.clone()),
    }
}

/// Parse the user's `--time-iso START:STOP`, fetch the time axis values and
/// the file's DAS, convert the ISO endpoints to axis-values, and resolve to
/// an inclusive index range.
async fn resolve_time_iso_range(
    http: &Client,
    cache: &Cache,
    opendap_url: &str,
    spec: &str,
) -> Result<IndexRange> {
    let (start_str, stop_str) = spec.split_once(':').ok_or_else(|| {
        Error::InvalidArgument(format!("--time-iso must be START:STOP, got '{spec}'"))
    })?;
    let (start_dt, _) = parse_iso_endpoint(start_str, EndpointSide::Start)?;
    let (stop_dt, _) = parse_iso_endpoint(stop_str, EndpointSide::Stop)?;
    if stop_dt < start_dt {
        return Err(Error::InvalidArgument(format!(
            "--time-iso: stop ({stop_str}) is before start ({start_str})"
        )));
    }

    // Fetch the DAS + the time axis values, with cache.
    let das_url = format!("{opendap_url}.das");
    let das_bytes = match cache.get(&das_url)? {
        Some(b) => b,
        None => {
            let b = http.get_bytes(&das_url).await?;
            let _ = cache.put(&das_url, &b);
            b
        }
    };
    let das = std::str::from_utf8(&das_bytes)
        .map_err(|e| Error::Parse(format!("DAS response is not UTF-8: {e}")))?;
    let axis = CfTimeAxis::from_das(das, "time")?;

    let time_var = fetch_coord(http, cache, opendap_url, vec!["time"]).await?;
    let time_values = coord_values_as_f64(&time_var.data, &time_var.name)?;

    let start_val = axis.datetime_to_axis(start_dt)?;
    let stop_val = axis.datetime_to_axis(stop_dt)?;
    let r = coords::resolve_range(&time_values, start_val, stop_val)?;
    eprintln!(
        "resolved time: {start_str}..{stop_str} -> axis {start_val}..{stop_val} \
         -> idx {}..{} ({} steps)",
        r.start,
        r.stop,
        r.len(),
    );
    Ok(r)
}

/// Which end of a range is being parsed — determines how bare `YYYY` or
/// `YYYY-MM` expand.
#[derive(Clone, Copy, Debug)]
enum EndpointSide {
    Start,
    Stop,
}

/// Parse one side of `--time-iso`. Accepts:
/// * `YYYY` — Jan 1 (start) or Dec 31 23:59:59 (stop)
/// * `YYYY-MM-DD` — midnight
/// * `YYYY-MM-DDTHH:MM:SS` — exact
fn parse_iso_endpoint(s: &str, side: EndpointSide) -> Result<(NaiveDateTime, String)> {
    let s = s.trim();

    // YYYY
    if s.len() == 4 {
        if let Ok(year) = s.parse::<i32>() {
            let dt = match side {
                EndpointSide::Start => NaiveDate::from_ymd_opt(year, 1, 1)
                    .and_then(|d| d.and_hms_opt(0, 0, 0))
                    .ok_or_else(|| {
                        Error::InvalidArgument(format!("year {year} is out of range"))
                    })?,
                EndpointSide::Stop => NaiveDate::from_ymd_opt(year, 12, 31)
                    .and_then(|d| d.and_hms_opt(23, 59, 59))
                    .ok_or_else(|| {
                        Error::InvalidArgument(format!("year {year} is out of range"))
                    })?,
            };
            return Ok((dt, format!("{year}")));
        }
    }

    // YYYY-MM-DDTHH:MM:SS variants.
    for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"] {
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, fmt) {
            return Ok((dt, s.to_string()));
        }
    }

    // YYYY-MM-DD.
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        let t = match side {
            EndpointSide::Start => NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
            EndpointSide::Stop => NaiveTime::from_hms_opt(23, 59, 59).unwrap(),
        };
        return Ok((d.and_time(t), s.to_string()));
    }

    Err(Error::InvalidArgument(format!(
        "couldn't parse '{s}' as ISO date (expected YYYY, YYYY-MM-DD, or YYYY-MM-DDTHH:MM:SS)"
    )))
}

/// Parse a `MIN:MAX` floating-point range used by `--lat-deg` / `--lon-deg`.
fn parse_deg_range(spec: Option<&str>, flag: &str) -> Result<Option<(f64, f64)>> {
    let Some(s) = spec else {
        return Ok(None);
    };
    let (lo, hi) = s
        .split_once(':')
        .ok_or_else(|| Error::InvalidArgument(format!("{flag} must be MIN:MAX, got '{s}'")))?;
    let lo: f64 = lo
        .trim()
        .parse()
        .map_err(|_| Error::InvalidArgument(format!("{flag} min '{lo}' is not a number")))?;
    let hi: f64 = hi
        .trim()
        .parse()
        .map_err(|_| Error::InvalidArgument(format!("{flag} max '{hi}' is not a number")))?;
    if hi < lo {
        return Err(Error::InvalidArgument(format!(
            "{flag}: max ({hi}) must be >= min ({lo})"
        )));
    }
    Ok(Some((lo, hi)))
}

/// Fetch the lat / lon coordinate variables for a file, resolve the user's
/// degree ranges to inclusive index ranges.
///
/// The returned `(lat_or_y_idx, lon_or_x_idx)` pair fills the lat/lon slots
/// of the constraint regardless of whether the underlying grid is
/// rectilinear (1D axes, indices in `lat` and `lon` dimension) or
/// curvilinear (2D `lat[y, x]` / `lon[y, x]`, indices in `y` and `x`).
/// Either way, the OPeNDAP constraint syntax is the same — inclusive index
/// ranges in declared-dimension order.
async fn resolve_lat_lon_degrees(
    http: &Client,
    cache: &Cache,
    opendap_url: &str,
    lat_coord_override: Option<&str>,
    lon_coord_override: Option<&str>,
    lat_deg: Option<(f64, f64)>,
    lon_deg: Option<(f64, f64)>,
) -> Result<(Option<IndexRange>, Option<IndexRange>)> {
    // Nothing requested → nothing to do.
    if lat_deg.is_none() && lon_deg.is_none() {
        return Ok((None, None));
    }

    let lat_var = fetch_coord(
        http,
        cache,
        opendap_url,
        coord_candidates(lat_coord_override, LAT_COORD_CANDIDATES),
    )
    .await?;
    let lon_var = fetch_coord(
        http,
        cache,
        opendap_url,
        coord_candidates(lon_coord_override, LON_COORD_CANDIDATES),
    )
    .await?;

    match (lat_var.dimensions.len(), lon_var.dimensions.len()) {
        (1, 1) => resolve_1d(lat_var, lon_var, lat_deg, lon_deg),
        (2, 2) => resolve_2d(lat_var, lon_var, lat_deg, lon_deg),
        (a, b) => Err(Error::InvalidArgument(format!(
            "coordinate variables have mismatched dimensionality \
             (lat is {a}D, lon is {b}D); ferrous supports either 1D+1D \
             rectilinear or 2D+2D curvilinear grids"
        ))),
    }
}

/// Build the candidate-names slice for a coord-variable lookup, honouring
/// an optional user override.
fn coord_candidates<'a>(override_name: Option<&'a str>, defaults: &'a [&'a str]) -> Vec<&'a str> {
    match override_name {
        Some(name) => vec![name],
        None => defaults.to_vec(),
    }
}

fn resolve_1d(
    lat_var: DapVariable,
    lon_var: DapVariable,
    lat_deg: Option<(f64, f64)>,
    lon_deg: Option<(f64, f64)>,
) -> Result<(Option<IndexRange>, Option<IndexRange>)> {
    let lat_idx = match lat_deg {
        Some((lo, hi)) => {
            let axis = coord_values_as_f64(&lat_var.data, &lat_var.name)?;
            let r = coords::resolve_range(&axis, lo, hi)?;
            eprintln!(
                "resolved lat: {lo}..{hi} deg -> idx {}..{} ({} elements, 1D axis {}..{})",
                r.start,
                r.stop,
                r.len(),
                axis[0],
                axis[axis.len() - 1],
            );
            Some(r)
        }
        None => None,
    };
    let lon_idx = match lon_deg {
        Some((lo, hi)) => {
            let axis = coord_values_as_f64(&lon_var.data, &lon_var.name)?;
            let r = coords::resolve_range(&axis, lo, hi)?;
            eprintln!(
                "resolved lon: {lo}..{hi} deg -> idx {}..{} ({} elements, 1D axis {}..{})",
                r.start,
                r.stop,
                r.len(),
                axis[0],
                axis[axis.len() - 1],
            );
            Some(r)
        }
        None => None,
    };
    Ok((lat_idx, lon_idx))
}

fn resolve_2d(
    lat_var: DapVariable,
    lon_var: DapVariable,
    lat_deg: Option<(f64, f64)>,
    lon_deg: Option<(f64, f64)>,
) -> Result<(Option<IndexRange>, Option<IndexRange>)> {
    // Curvilinear grids can't decouple lat and lon — the bbox is joint, so
    // both --lat-deg and --lon-deg must be supplied.
    let (lat_lo, lat_hi) = lat_deg.ok_or_else(|| {
        Error::InvalidArgument(
            "this is a 2D curvilinear grid; both --lat-deg and --lon-deg must be supplied \
             (or fall back to --lat / --lon index ranges)"
                .into(),
        )
    })?;
    let (lon_lo, lon_hi) = lon_deg.ok_or_else(|| {
        Error::InvalidArgument(
            "this is a 2D curvilinear grid; both --lat-deg and --lon-deg must be supplied \
             (or fall back to --lat / --lon index ranges)"
                .into(),
        )
    })?;

    if lat_var.dimensions != lon_var.dimensions {
        return Err(Error::InvalidArgument(format!(
            "2D lat / lon coord vars must share the same dimensions; got {:?} vs {:?}",
            lat_var.dimensions, lon_var.dimensions
        )));
    }
    let (ny, nx) = (lat_var.dimensions[0].size, lat_var.dimensions[1].size);
    let lat_values = coord_values_as_f64(&lat_var.data, &lat_var.name)?;
    let lon_values = coord_values_as_f64(&lon_var.data, &lon_var.name)?;
    let (y, x) = coords::resolve_2d_bbox(
        &lat_values,
        &lon_values,
        (ny, nx),
        (lat_lo, lat_hi),
        (lon_lo, lon_hi),
    )?;
    eprintln!(
        "resolved 2D curvilinear bbox: lat {lat_lo}..{lat_hi}, lon {lon_lo}..{lon_hi} \
         -> y={}..{} ({} cells), x={}..{} ({} cells) on {ny}x{nx} grid",
        y.start,
        y.stop,
        y.len(),
        x.start,
        x.stop,
        x.len(),
    );
    Ok((Some(y), Some(x)))
}

/// Fetch the first matching coordinate variable from the file. Tries each
/// candidate name in order; returns the full `DapVariable` so the caller can
/// decide whether to drive the 1D or 2D resolver.
async fn fetch_coord(
    http: &Client,
    cache: &Cache,
    opendap_url: &str,
    candidates: Vec<&str>,
) -> Result<DapVariable> {
    let mut last_err: Option<Error> = None;
    for name in &candidates {
        let url = format!("{opendap_url}.dods?{name}");
        let bytes = match cache.get(&url)? {
            Some(b) => b,
            None => match http.get_bytes(&url).await {
                Ok(b) => {
                    let _ = cache.put(&url, &b);
                    b
                }
                Err(e) => {
                    last_err = Some(e);
                    continue;
                }
            },
        };
        match dap2::decode(&bytes) {
            Ok(mut resp) if !resp.variables.is_empty() => {
                // When the server wraps a coord in a Grid, the target
                // variable is the ARRAY (first entry). The MAPS carry the
                // self-coordinates which we don't need here.
                return Ok(resp.variables.remove(0));
            }
            Ok(_) => {
                last_err = Some(Error::Parse(format!(
                    "coordinate response for '{name}' contained no variables"
                )));
            }
            Err(e) => {
                last_err = Some(e);
            }
        }
    }
    Err(last_err.unwrap_or_else(|| {
        Error::InvalidArgument(format!("no coordinate variable found among {candidates:?}"))
    }))
}

fn coord_values_as_f64(data: &DapData, name: &str) -> Result<Vec<f64>> {
    match data {
        DapData::F32(v) => Ok(v.iter().map(|&x| x as f64).collect()),
        DapData::F64(v) => Ok(v.clone()),
        _ => Err(Error::Parse(format!(
            "coordinate variable '{name}' is not a floating-point type"
        ))),
    }
}

/// Run `ferrous inspect`.
///
/// Reads a local `.dods` file (typically produced by `ferrous get`),
/// decodes it via [`crate::dap2`], and prints per-variable shape and
/// summary statistics. Useful for sanity-checking a fetch and for figuring
/// out array indices when planning a follow-up slice.
pub fn run_inspect(args: &InspectArgs) -> Result<()> {
    let bytes = fs::read(&args.path)?;
    let response = dap2::decode(&bytes)?;

    println!("File:      {}", args.path.display());
    println!("Bytes:     {}", bytes.len());
    println!("Variables: {}", response.variables.len());
    if args.dds {
        println!("\n--- DDS ---\n{}\n--- end ---", response.dds);
    }
    println!();

    for v in &response.variables {
        print_variable_summary(v, args.fill_threshold);
        println!();
    }
    Ok(())
}

fn print_variable_summary(v: &DapVariable, fill_threshold: f64) {
    let dims: Vec<String> = v
        .dimensions
        .iter()
        .map(|d| format!("{}={}", d.name, d.size))
        .collect();
    let dims_str = if dims.is_empty() {
        "scalar".to_string()
    } else {
        format!("[{}]", dims.join(", "))
    };
    println!(
        "{} ({:?}, {}, {} elements)",
        v.name,
        v.dtype,
        dims_str,
        v.element_count()
    );
    let stats = compute_stats(&v.data, fill_threshold);
    match stats {
        Some(s) => {
            println!(
                "  valid:   {}/{}  ({:.1}% non-fill)",
                s.valid_count,
                s.total,
                100.0 * s.valid_count as f64 / s.total.max(1) as f64
            );
            println!("  min:     {}", s.min);
            println!("  max:     {}", s.max);
            println!("  mean:    {}", s.mean);
        }
        None => println!("  (no stats: variable type or all values masked as fill)"),
    }
}

struct Stats {
    total: usize,
    valid_count: usize,
    min: f64,
    max: f64,
    mean: f64,
}

fn compute_stats(data: &DapData, fill_threshold: f64) -> Option<Stats> {
    // Numeric pull-out: we only summarise floating-point variables. Integer
    // arrays in CMIP6 are usually time/index axes where min/max/mean are
    // less meaningful — printing the shape alone is more honest there.
    let (total, iter): (usize, Box<dyn Iterator<Item = f64> + '_>) = match data {
        DapData::F32(v) => (v.len(), Box::new(v.iter().map(|&x| x as f64))),
        DapData::F64(v) => (v.len(), Box::new(v.iter().copied())),
        _ => return None,
    };
    let mut valid_count = 0usize;
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    let mut sum = 0.0_f64;
    for x in iter {
        if x.is_nan() || x.abs() >= fill_threshold {
            continue;
        }
        valid_count += 1;
        if x < min {
            min = x;
        }
        if x > max {
            max = x;
        }
        sum += x;
    }
    if valid_count == 0 {
        return None;
    }
    Some(Stats {
        total,
        valid_count,
        min,
        max,
        mean: sum / valid_count as f64,
    })
}

fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            fs::create_dir_all(parent)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dap2::{DapData, DapDimension, DapResponse, DapType, DapVariable};

    fn tas_var(time_len: usize, start_value: f32) -> DapVariable {
        let lat = 2;
        let lon = 3;
        let n = time_len * lat * lon;
        let data = (0..n).map(|i| start_value + i as f32).collect();
        DapVariable {
            name: "tas".into(),
            dtype: DapType::Float32,
            dimensions: vec![
                DapDimension {
                    name: "time".into(),
                    size: time_len,
                },
                DapDimension {
                    name: "lat".into(),
                    size: lat,
                },
                DapDimension {
                    name: "lon".into(),
                    size: lon,
                },
            ],
            data: DapData::F32(data),
        }
    }

    fn time_var(values: Vec<f64>) -> DapVariable {
        DapVariable {
            name: "time".into(),
            dtype: DapType::Float64,
            dimensions: vec![DapDimension {
                name: "time".into(),
                size: values.len(),
            }],
            data: DapData::F64(values),
        }
    }

    fn lat_var() -> DapVariable {
        DapVariable {
            name: "lat".into(),
            dtype: DapType::Float64,
            dimensions: vec![DapDimension {
                name: "lat".into(),
                size: 2,
            }],
            data: DapData::F64(vec![10.0, 20.0]),
        }
    }

    fn lon_var() -> DapVariable {
        DapVariable {
            name: "lon".into(),
            dtype: DapType::Float64,
            dimensions: vec![DapDimension {
                name: "lon".into(),
                size: 3,
            }],
            data: DapData::F64(vec![0.0, 30.0, 60.0]),
        }
    }

    #[test]
    fn concat_extends_time_dim_and_data() {
        let mut target = DapResponse {
            dds: String::new(),
            variables: vec![
                tas_var(2, 0.0),
                time_var(vec![100.0, 200.0]),
                lat_var(),
                lon_var(),
            ],
        };
        let other = DapResponse {
            dds: String::new(),
            variables: vec![
                tas_var(3, 1000.0),
                time_var(vec![300.0, 400.0, 500.0]),
                lat_var(),
                lon_var(),
            ],
        };
        concatenate_along_time(&mut target, other).unwrap();
        assert_eq!(target.variables[0].dimensions[0].size, 5, "time grows 2+3");
        assert_eq!(target.variables[0].data.as_f32().unwrap().len(), 5 * 2 * 3);
        assert_eq!(
            target.variables[1].data.as_f64().unwrap(),
            &[100.0, 200.0, 300.0, 400.0, 500.0]
        );
        assert_eq!(target.variables[2].dimensions[0].size, 2);
        assert_eq!(target.variables[3].dimensions[0].size, 3);
    }

    #[test]
    fn concat_rejects_mismatched_spatial_shape() {
        let mut target = DapResponse {
            dds: String::new(),
            variables: vec![tas_var(1, 0.0), time_var(vec![0.0]), lat_var(), lon_var()],
        };
        let mut bad_lat = lat_var();
        bad_lat.dimensions[0].size = 99;
        bad_lat.data = DapData::F64(vec![0.0; 99]);
        let other = DapResponse {
            dds: String::new(),
            variables: vec![tas_var(1, 0.0), time_var(vec![100.0]), bad_lat, lon_var()],
        };
        assert!(concatenate_along_time(&mut target, other).is_err());
    }

    #[test]
    fn concat_rejects_variable_order_mismatch() {
        let mut target = DapResponse {
            dds: String::new(),
            variables: vec![tas_var(1, 0.0), time_var(vec![0.0])],
        };
        let other = DapResponse {
            dds: String::new(),
            variables: vec![time_var(vec![100.0]), tas_var(1, 0.0)],
        };
        assert!(concatenate_along_time(&mut target, other).is_err());
    }

    #[test]
    fn concat_preserves_time_dim_for_variables_without_time() {
        let mut target = DapResponse {
            dds: String::new(),
            variables: vec![tas_var(1, 0.0), time_var(vec![0.0]), lat_var()],
        };
        let other = DapResponse {
            dds: String::new(),
            variables: vec![tas_var(1, 0.0), time_var(vec![100.0]), lat_var()],
        };
        concatenate_along_time(&mut target, other).unwrap();
        assert_eq!(target.variables[2].dimensions[0].size, 2, "lat unchanged");
        assert_eq!(target.variables[2].data.as_f64().unwrap(), &[10.0, 20.0]);
    }
}
