//! Subcommand implementations.
//!
//! Each function here takes already-parsed CLI arguments and returns
//! `Result<()>`, so the binary stays a thin dispatcher.

use std::fs;
use std::path::Path;

use crate::cache::Cache;
use crate::cli::{Cli, GetArgs, InspectArgs, SearchArgs};
use crate::coords::{self, IndexRange};
use crate::dap2::{self, DapData, DapVariable};
use crate::esgf::{Dataset, SearchClient, SearchQuery, DEFAULT_SEARCH_ENDPOINT};
use crate::http::{Client, ClientBuilder, RateLimiter};
use crate::opendap::{Constraint, Slice};
use crate::{Error, Result};

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
/// Resolves a target File (either directly via `--dataset-id` or by Dataset
/// search), builds an OPeNDAP constraint from the provided index slices,
/// fetches `<opendap_url>.dods?<constraint>`, and writes the raw DAP2 bytes
/// to the `--out` path.
///
/// The DAP2 binary format is not NetCDF, but it's the format every OPeNDAP
/// server supports uniformly, and tools like `pydap` can parse it. Adding
/// NetCDF4 output (via `.dap.nc4` suffix on Hyrax nodes or a local re-pack)
/// is a follow-up.
pub async fn run_get(cli: &Cli, args: &GetArgs) -> Result<()> {
    let http = build_http(cli)?;
    let endpoint = cli
        .endpoint
        .clone()
        .unwrap_or_else(|| DEFAULT_SEARCH_ENDPOINT.to_string());
    let client = SearchClient::new(http.clone(), &endpoint);

    // 1. Resolve the target dataset id.
    let dataset_id = match &args.dataset_id {
        Some(id) => id.clone(),
        None => resolve_dataset_id(&client, args).await?,
    };

    // 2. Enumerate the files in that dataset and pick one.
    let files = client
        .search_files(&SearchQuery::cmip6_files_of(&dataset_id))
        .await?;
    if args.file_index == 0 || args.file_index > files.files.len() {
        return Err(Error::InvalidArgument(format!(
            "--file-index {} out of range (dataset has {} file(s))",
            args.file_index,
            files.files.len()
        )));
    }
    let file = &files.files[args.file_index - 1];
    let opendap_url = file
        .opendap_url()
        .ok_or_else(|| Error::Parse(format!("file {} has no OPENDAP access URL", file.id)))?;

    // 3. Resolve degree → index ranges if --lat-deg / --lon-deg supplied.
    //    These override the index forms (--lat / --lon).
    let lat_deg = parse_deg_range(args.lat_deg.as_deref(), "--lat-deg")?;
    let lon_deg = parse_deg_range(args.lon_deg.as_deref(), "--lon-deg")?;
    let (lat_idx, lon_idx) = if lat_deg.is_some() || lon_deg.is_some() {
        let cache = build_cache(cli);
        resolve_lat_lon_degrees(
            &http,
            &cache,
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

    // 4. Build the constraint from --time + (resolved or raw) --lat/--lon + --slice.
    let constraint = build_constraint(&args.variable, args, lat_idx, lon_idx)?;
    let fetch_url = format!(
        "{}.dods{}",
        opendap_url,
        if constraint.is_empty() {
            String::new()
        } else {
            format!("?{}", constraint.to_query())
        }
    );

    println!("dataset:    {dataset_id}");
    println!(
        "file:       {} ({} of {})",
        file.title,
        args.file_index,
        files.files.len()
    );
    if let Some(size) = file.size {
        println!(
            "full size:  {} bytes (~{:.1} MB)",
            size,
            size as f64 / 1_000_000.0
        );
    }
    println!("constraint: {}", constraint.to_query());
    println!("URL:        {fetch_url}");

    if args.dry_run {
        return Ok(());
    }

    // 5. Cache lookup → fetch if missing → write to --out.
    let cache = build_cache(cli);
    println!();
    let (bytes, source) = match cache.get(&fetch_url)? {
        Some(b) => {
            println!("Cache hit ({} bytes, 0 fetched).", b.len());
            (b, "cache")
        }
        None => {
            println!("Fetching...");
            let b = http.get_bytes(&fetch_url).await?;
            if let Err(e) = cache.put(&fetch_url, &b) {
                // Caching is best-effort; a failure to write the cache must
                // not fail the user-visible fetch.
                eprintln!("warning: failed to populate cache: {e}");
            }
            (b, "network")
        }
    };
    ensure_parent_dir(&args.out)?;
    fs::write(&args.out, &bytes)?;

    let saved = bytes.len() as f64;
    let full = file.size.unwrap_or(0) as f64;
    println!(
        "Wrote {} bytes (~{:.2} MB) to {} [via {source}]",
        bytes.len(),
        saved / 1_000_000.0,
        args.out.display()
    );
    if source == "network" && full > 0.0 && saved > 0.0 {
        let ratio = saved / full * 100.0;
        println!(
            "Transferred {:.2}% of the full file ({:.1}x reduction).",
            ratio,
            full / saved
        );
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
/// `lat_resolved` and `lon_resolved` come from degree-based resolution and
/// take precedence over the corresponding `args.lat` / `args.lon` index
/// strings when present.
fn build_constraint(
    variable: &str,
    args: &GetArgs,
    lat_resolved: Option<IndexRange>,
    lon_resolved: Option<IndexRange>,
) -> Result<Constraint> {
    let mut slices: Vec<Slice> = Vec::new();
    if let Some(spec) = &args.time {
        slices.push(spec.parse()?);
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
