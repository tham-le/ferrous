//! ESGF (Earth System Grid Federation) dataset search client.
//!
//! ESGF exposes a Solr-based REST search endpoint that powers the familiar
//! ESGF web portals. Ferrous calls this endpoint directly with JSON output
//! (`format=application/solr+json`) to discover datasets matching a query —
//! variable, model, scenario, frequency, etc.
//!
//! The response includes a per-dataset `url` array where each element is a
//! pipe-delimited triple `url|mime|service`. The `OPENDAP` service entries
//! are what Ferrous then hands to the OPeNDAP constraint builder to fetch
//! only the needed slice.
//!
//! # Node selection
//!
//! Classic `/esg-search/search` endpoints still exist on the federated nodes
//! that have not yet migrated to the 1.5 bridge. Known-good defaults as of
//! 2026-04:
//!
//! * `https://esgf.ceda.ac.uk/esg-search/search` — **reliable, current default**
//! * `https://esgf-data.dkrz.de/esg-search/search`
//! * `https://esgf-node.ipsl.upmc.fr/esg-search/search` — currently returns
//!   HTTP 500 on Dataset queries, kept listed for when it's fixed
//!
//! The LLNL node (`esgf-node.llnl.gov`) now 302-redirects to the ORNL 1.5
//! bridge, which uses a different API shape and is not yet supported.

use serde::Deserialize;

use crate::http::Client;
use crate::{Error, Result};

/// Minimal query-string percent-encoder covering the characters we actually
/// emit in ESGF queries (dataset ids contain `|`, URLs contain `:` and `/`).
///
/// Deliberately not pulling in the `url` crate for this; the input space is
/// narrow (ESGF dataset ids + CMIP6 facet values) and the rule is "encode
/// anything that isn't unreserved per RFC 3986".
fn percent_encode_value(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Default search endpoint used when the caller does not pick a node.
///
/// Live-verified working 2026-04. IPSL was the original default but is
/// currently 500-ing on Dataset queries.
pub const DEFAULT_SEARCH_ENDPOINT: &str = "https://esgf.ceda.ac.uk/esg-search/search";

/// Maximum number of datasets returned per search request.
pub const DEFAULT_LIMIT: usize = 50;

/// Whether the query targets Dataset records (aggregated) or File records
/// (individual NetCDF files).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SearchType {
    /// Aggregated Dataset records. Use for discovery.
    #[default]
    Dataset,
    /// Individual File records. Use after picking a dataset to enumerate its
    /// OPeNDAP endpoints.
    File,
}

impl SearchType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Dataset => "Dataset",
            Self::File => "File",
        }
    }
}

/// A search query against an ESGF node — covers both Dataset and File
/// searches.
///
/// Facet fields are optional; the Solr backend matches any combination the
/// caller provides. Only `project` has a default (`CMIP6`) because every
/// other project uses an entirely different schema and a naked cross-project
/// search is rarely what a user means.
#[derive(Clone, Debug)]
pub struct SearchQuery {
    /// ESGF project (default `CMIP6`).
    pub project: String,
    /// CMIP6 variable id (`tos`, `tas`, `pr`, ...).
    pub variable_id: Option<String>,
    /// CMIP6 experiment id (`historical`, `ssp245`, ...).
    pub experiment_id: Option<String>,
    /// CMIP6 source model id (`CNRM-CM6-1`, `IPSL-CM6A-LR`, ...).
    pub source_id: Option<String>,
    /// Output frequency (`mon`, `day`, `yr`, ...).
    pub frequency: Option<String>,
    /// Constrain a File search to a specific parent dataset id. Ignored for
    /// Dataset searches.
    pub dataset_id: Option<String>,
    /// Dataset vs File search. Defaults to Dataset.
    pub search_type: SearchType,
    /// Number of results to return (default [`DEFAULT_LIMIT`]).
    pub limit: usize,
    /// Result offset for pagination.
    pub offset: usize,
}

impl Default for SearchQuery {
    fn default() -> Self {
        Self {
            project: "CMIP6".into(),
            variable_id: None,
            experiment_id: None,
            source_id: None,
            frequency: None,
            dataset_id: None,
            search_type: SearchType::Dataset,
            limit: DEFAULT_LIMIT,
            offset: 0,
        }
    }
}

impl SearchQuery {
    /// Start a CMIP6 Dataset search.
    pub fn cmip6() -> Self {
        Self::default()
    }

    /// Start a CMIP6 File search filtered to the given parent dataset id.
    ///
    /// Dataset records expose an `access` array (which services are
    /// available) but no URLs; the URLs themselves live on File records. Call
    /// this to enumerate those URLs once a dataset has been picked.
    pub fn cmip6_files_of(dataset_id: impl Into<String>) -> Self {
        Self {
            search_type: SearchType::File,
            dataset_id: Some(dataset_id.into()),
            ..Self::default()
        }
    }

    /// Set the variable id facet.
    pub fn variable(mut self, v: impl Into<String>) -> Self {
        self.variable_id = Some(v.into());
        self
    }

    /// Set the experiment id facet.
    pub fn experiment(mut self, v: impl Into<String>) -> Self {
        self.experiment_id = Some(v.into());
        self
    }

    /// Set the source model id facet.
    pub fn source(mut self, v: impl Into<String>) -> Self {
        self.source_id = Some(v.into());
        self
    }

    /// Set the frequency facet.
    pub fn frequency(mut self, v: impl Into<String>) -> Self {
        self.frequency = Some(v.into());
        self
    }

    /// Override the result page size.
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = n;
        self
    }

    /// Override the pagination offset.
    pub fn offset(mut self, n: usize) -> Self {
        self.offset = n;
        self
    }

    /// Render the query as URL-encoded parameters appended to an ESGF search
    /// endpoint.
    pub fn to_query_string(&self) -> String {
        // CMIP6 facet values are `[A-Za-z0-9._-]` and safe unescaped. Dataset
        // IDs contain `|` (pipe between instance-id and data-node) which must
        // be percent-encoded.
        let mut parts: Vec<String> = Vec::new();
        parts.push(format!("project={}", self.project));
        if let Some(v) = &self.variable_id {
            parts.push(format!("variable_id={v}"));
        }
        if let Some(v) = &self.experiment_id {
            parts.push(format!("experiment_id={v}"));
        }
        if let Some(v) = &self.source_id {
            parts.push(format!("source_id={v}"));
        }
        if let Some(v) = &self.frequency {
            parts.push(format!("frequency={v}"));
        }
        if let Some(v) = &self.dataset_id {
            parts.push(format!("dataset_id={}", percent_encode_value(v)));
        }
        parts.push(format!("limit={}", self.limit));
        parts.push(format!("offset={}", self.offset));
        parts.push(format!("type={}", self.search_type.as_str()));
        parts.push("format=application%2Fsolr%2Bjson".into());
        parts.join("&")
    }

    /// Full search URL (endpoint + `?` + query string).
    pub fn to_url(&self, endpoint: &str) -> String {
        format!("{endpoint}?{}", self.to_query_string())
    }
}

/// Summary of a single dataset returned by ESGF search.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Dataset {
    /// ESGF dataset id (e.g. `CMIP6.ScenarioMIP.CNRM-CERFACS.CNRM-CM6-1.ssp245.r1i1p1f2.Omon.tos.gn.v20190219`).
    pub id: String,
    /// Dataset title as shown in the ESGF portal.
    pub title: String,
    /// CMIP6 variable ids present in the dataset.
    pub variable_id: Vec<String>,
    /// CMIP6 source (model) ids.
    pub source_id: Vec<String>,
    /// CMIP6 experiment ids.
    pub experiment_id: Vec<String>,
    /// Output frequencies.
    pub frequency: Vec<String>,
    /// Parsed access URLs grouped by service type.
    pub urls: Vec<DatasetUrl>,
}

impl Dataset {
    /// Return every URL advertised by this dataset for the given service type
    /// (case-insensitive, e.g. `"OPENDAP"` or `"HTTPServer"`).
    pub fn urls_for_service(&self, service: &str) -> Vec<&DatasetUrl> {
        self.urls
            .iter()
            .filter(|u| u.service.eq_ignore_ascii_case(service))
            .collect()
    }

    /// Convenience: first OPeNDAP access URL if any. The `.html` suffix some
    /// nodes append to OPeNDAP URLs is stripped so the result is directly
    /// usable by [`crate::opendap::Constraint::append_to_url`].
    pub fn opendap_url(&self) -> Option<String> {
        self.urls_for_service("OPENDAP")
            .first()
            .map(|u| u.url.trim_end_matches(".html").to_string())
    }
}

/// A single access URL for a dataset — one row of the ESGF `url` array.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatasetUrl {
    /// Access URL.
    pub url: String,
    /// MIME type advertised by ESGF.
    pub mime: String,
    /// Service type (`OPENDAP`, `HTTPServer`, `Globus`, `GridFTP`, ...).
    pub service: String,
}

impl DatasetUrl {
    /// Parse the pipe-delimited `url|mime|service` string used by ESGF.
    pub fn parse(raw: &str) -> Result<Self> {
        let parts: Vec<&str> = raw.splitn(3, '|').collect();
        if parts.len() != 3 {
            return Err(Error::Parse(format!(
                "malformed ESGF url triple (expected url|mime|service): {raw}"
            )));
        }
        Ok(Self {
            url: parts[0].trim().to_string(),
            mime: parts[1].trim().to_string(),
            service: parts[2].trim().to_string(),
        })
    }
}

/// Paginated set of matching datasets.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchResults {
    /// Total matches at the server (may exceed `datasets.len()`).
    pub total: usize,
    /// Datasets returned in this page.
    pub datasets: Vec<Dataset>,
}

/// Summary of a single File record — one NetCDF file in a CMIP6 dataset.
///
/// Unlike [`Dataset`], File records carry the actual per-file OPeNDAP and
/// HTTPServer URLs. [`File::opendap_url`] gives back the URL ready for
/// [`crate::opendap::Constraint::append_to_url`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct File {
    /// Full ESGF file id (instance_id + `|` + data_node).
    pub id: String,
    /// File name (usually the NetCDF filename).
    pub title: String,
    /// Parent dataset id.
    pub dataset_id: String,
    /// File size in bytes, when advertised by the node.
    pub size: Option<u64>,
    /// Earliest timestamp covered by the file, if advertised.
    pub datetime_start: Option<String>,
    /// Latest timestamp covered by the file, if advertised.
    pub datetime_stop: Option<String>,
    /// SHA256 (or other) checksum reported by ESGF.
    pub checksum: Option<String>,
    /// Checksum algorithm name (e.g. `"SHA256"`).
    pub checksum_type: Option<String>,
    /// Parsed access URLs grouped by service type.
    pub urls: Vec<DatasetUrl>,
}

impl File {
    /// Return every URL advertised by this file for the given service type
    /// (case-insensitive).
    pub fn urls_for_service(&self, service: &str) -> Vec<&DatasetUrl> {
        self.urls
            .iter()
            .filter(|u| u.service.eq_ignore_ascii_case(service))
            .collect()
    }

    /// Convenience: first OPeNDAP URL with the `.html` suffix stripped so it
    /// drops directly into [`crate::opendap::Constraint::append_to_url`].
    pub fn opendap_url(&self) -> Option<String> {
        self.urls_for_service("OPENDAP")
            .first()
            .map(|u| u.url.trim_end_matches(".html").to_string())
    }

    /// Convenience: first HTTPServer URL (for non-sliced full downloads).
    pub fn http_url(&self) -> Option<String> {
        self.urls_for_service("HTTPServer")
            .first()
            .map(|u| u.url.clone())
    }
}

/// Paginated set of matching files.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileSearchResults {
    /// Total matches at the server (may exceed `files.len()`).
    pub total: usize,
    /// Files returned in this page.
    pub files: Vec<File>,
}

// Solr response deserialization. Kept private — callers see the clean
// `SearchResults` / `Dataset` types above.
#[derive(Deserialize)]
struct SolrResponse {
    response: SolrInner,
}

#[derive(Deserialize)]
struct SolrInner {
    #[serde(rename = "numFound")]
    num_found: usize,
    docs: Vec<SolrDoc>,
}

#[derive(Deserialize)]
struct SolrDoc {
    id: String,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    variable_id: Vec<String>,
    #[serde(default)]
    source_id: Vec<String>,
    #[serde(default)]
    experiment_id: Vec<String>,
    #[serde(default)]
    frequency: Vec<String>,
    #[serde(default)]
    url: Vec<String>,
}

impl SolrDoc {
    fn into_dataset(self) -> Dataset {
        let title = self.title.unwrap_or_else(|| self.id.clone());
        let urls = self
            .url
            .iter()
            .filter_map(|raw| DatasetUrl::parse(raw).ok())
            .collect();
        Dataset {
            id: self.id,
            title,
            variable_id: self.variable_id,
            source_id: self.source_id,
            experiment_id: self.experiment_id,
            frequency: self.frequency,
            urls,
        }
    }
}

/// Parse a Solr JSON response body into [`SearchResults`] (Dataset shape).
///
/// Exposed for tests and advanced callers that have obtained the raw body
/// through some other path (e.g. a fixture). Normal users should call
/// [`SearchClient::search`].
pub fn parse_response(body: &str) -> Result<SearchResults> {
    let parsed: SolrResponse =
        serde_json::from_str(body).map_err(|e| Error::Parse(e.to_string()))?;
    Ok(SearchResults {
        total: parsed.response.num_found,
        datasets: parsed
            .response
            .docs
            .into_iter()
            .map(SolrDoc::into_dataset)
            .collect(),
    })
}

// File-shaped Solr doc. ESGF returns different fields for File records than
// Dataset records, so we parse it as a separate shape rather than trying to
// share a struct with `Option<…>` everywhere.
#[derive(Deserialize)]
struct SolrFileResponse {
    response: SolrFileInner,
}

#[derive(Deserialize)]
struct SolrFileInner {
    #[serde(rename = "numFound")]
    num_found: usize,
    docs: Vec<SolrFileDoc>,
}

#[derive(Deserialize)]
struct SolrFileDoc {
    id: String,
    #[serde(default)]
    title: Option<String>,
    dataset_id: String,
    #[serde(default)]
    size: Option<u64>,
    #[serde(default)]
    datetime_start: Option<String>,
    #[serde(default)]
    datetime_stop: Option<String>,
    #[serde(default)]
    checksum: Vec<String>,
    #[serde(default)]
    checksum_type: Vec<String>,
    #[serde(default)]
    url: Vec<String>,
}

impl SolrFileDoc {
    fn into_file(self) -> File {
        let title = self.title.unwrap_or_else(|| self.id.clone());
        let urls = self
            .url
            .iter()
            .filter_map(|raw| DatasetUrl::parse(raw).ok())
            .collect();
        File {
            id: self.id,
            title,
            dataset_id: self.dataset_id,
            size: self.size,
            datetime_start: self.datetime_start,
            datetime_stop: self.datetime_stop,
            checksum: self.checksum.into_iter().next(),
            checksum_type: self.checksum_type.into_iter().next(),
            urls,
        }
    }
}

/// Parse a Solr JSON response body into [`FileSearchResults`].
pub fn parse_file_response(body: &str) -> Result<FileSearchResults> {
    let parsed: SolrFileResponse =
        serde_json::from_str(body).map_err(|e| Error::Parse(e.to_string()))?;
    Ok(FileSearchResults {
        total: parsed.response.num_found,
        files: parsed
            .response
            .docs
            .into_iter()
            .map(SolrFileDoc::into_file)
            .collect(),
    })
}

/// Async ESGF search client.
#[derive(Clone, Debug)]
pub struct SearchClient {
    http: Client,
    endpoint: String,
}

impl SearchClient {
    /// Construct a client against the given endpoint URL (without a trailing
    /// query string).
    pub fn new(http: Client, endpoint: impl Into<String>) -> Self {
        Self {
            http,
            endpoint: endpoint.into(),
        }
    }

    /// Client against [`DEFAULT_SEARCH_ENDPOINT`] (currently CEDA).
    pub fn with_default_endpoint(http: Client) -> Self {
        Self::new(http, DEFAULT_SEARCH_ENDPOINT)
    }

    /// Configured endpoint.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Execute `query` against the configured endpoint.
    pub async fn search(&self, query: &SearchQuery) -> Result<SearchResults> {
        let url = query.to_url(&self.endpoint);
        let body = self.http.get_text(&url).await?;
        let results = parse_response(&body)?;
        if results.datasets.is_empty() {
            return Err(Error::NoResults);
        }
        Ok(results)
    }

    /// Enumerate File records for a given dataset.
    ///
    /// Builds a File-type query with the dataset_id filter and parses the
    /// response with [`parse_file_response`]. Callers typically pipe each
    /// returned [`File::opendap_url`] into
    /// [`crate::opendap::Constraint::append_to_url`].
    pub async fn search_files(&self, query: &SearchQuery) -> Result<FileSearchResults> {
        // Defensive: if the caller forgot to switch search_type, do it for
        // them. File search without SearchType::File silently returns Dataset
        // records, which would fail to parse here.
        let query = if query.search_type == SearchType::File {
            query.clone()
        } else {
            let mut q = query.clone();
            q.search_type = SearchType::File;
            q
        };
        let url = query.to_url(&self.endpoint);
        let body = self.http.get_text(&url).await?;
        let results = parse_file_response(&body)?;
        if results.files.is_empty() {
            return Err(Error::NoResults);
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_defaults_to_cmip6() {
        let q = SearchQuery::cmip6();
        assert_eq!(q.project, "CMIP6");
    }

    #[test]
    fn query_string_includes_all_facets() {
        let q = SearchQuery::cmip6()
            .variable("tos")
            .experiment("ssp245")
            .source("CNRM-CM6-1")
            .frequency("mon");
        let s = q.to_query_string();
        assert!(s.contains("project=CMIP6"), "{s}");
        assert!(s.contains("variable_id=tos"), "{s}");
        assert!(s.contains("experiment_id=ssp245"), "{s}");
        assert!(s.contains("source_id=CNRM-CM6-1"), "{s}");
        assert!(s.contains("frequency=mon"), "{s}");
        assert!(s.contains("type=Dataset"), "{s}");
        assert!(s.contains("format=application%2Fsolr%2Bjson"), "{s}");
    }

    #[test]
    fn query_string_omits_unset_facets() {
        let s = SearchQuery::cmip6().variable("tos").to_query_string();
        assert!(!s.contains("experiment_id="));
        assert!(!s.contains("source_id="));
        assert!(!s.contains("frequency="));
    }

    #[test]
    fn query_to_url_concatenates_endpoint_and_params() {
        let q = SearchQuery::cmip6().variable("tos").limit(10);
        let url = q.to_url("https://example.org/search");
        assert!(url.starts_with("https://example.org/search?"));
        assert!(url.contains("variable_id=tos"));
        assert!(url.contains("limit=10"));
    }

    #[test]
    fn dataset_url_parses_pipe_triple() {
        let raw = "https://example.org/data.nc|application/netcdf|HTTPServer";
        let u = DatasetUrl::parse(raw).unwrap();
        assert_eq!(u.url, "https://example.org/data.nc");
        assert_eq!(u.mime, "application/netcdf");
        assert_eq!(u.service, "HTTPServer");
    }

    #[test]
    fn dataset_url_rejects_malformed() {
        assert!(DatasetUrl::parse("no pipes here").is_err());
        assert!(DatasetUrl::parse("one|pipe").is_err());
    }

    #[test]
    fn dataset_opendap_url_strips_html_suffix() {
        let ds = Dataset {
            id: "x".into(),
            title: "x".into(),
            variable_id: vec![],
            source_id: vec![],
            experiment_id: vec![],
            frequency: vec![],
            urls: vec![DatasetUrl {
                url: "https://example.org/data.nc.html".into(),
                mime: "application/opendap-html".into(),
                service: "OPENDAP".into(),
            }],
        };
        assert_eq!(
            ds.opendap_url().as_deref(),
            Some("https://example.org/data.nc")
        );
    }

    #[test]
    fn dataset_urls_for_service_is_case_insensitive() {
        let ds = Dataset {
            id: "x".into(),
            title: "x".into(),
            variable_id: vec![],
            source_id: vec![],
            experiment_id: vec![],
            frequency: vec![],
            urls: vec![
                DatasetUrl {
                    url: "a".into(),
                    mime: "m".into(),
                    service: "OPENDAP".into(),
                },
                DatasetUrl {
                    url: "b".into(),
                    mime: "m".into(),
                    service: "HTTPServer".into(),
                },
            ],
        };
        assert_eq!(ds.urls_for_service("opendap").len(), 1);
        assert_eq!(ds.urls_for_service("HTTPSERVER").len(), 1);
    }

    #[test]
    fn parse_response_extracts_datasets() {
        let body = r#"
        {
          "responseHeader": {"status": 0},
          "response": {
            "numFound": 2,
            "start": 0,
            "docs": [
              {
                "id": "CMIP6.ScenarioMIP.CNRM.CNRM-CM6-1.ssp245.r1.Omon.tos.gn.v20190219",
                "title": "tos ssp245",
                "variable_id": ["tos"],
                "source_id": ["CNRM-CM6-1"],
                "experiment_id": ["ssp245"],
                "frequency": ["mon"],
                "url": [
                  "https://example.org/tos.nc|application/netcdf|HTTPServer",
                  "https://example.org/tos.nc.html|application/opendap-html|OPENDAP"
                ]
              },
              {
                "id": "CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r1.Omon.tos.gn.v20180803"
              }
            ]
          }
        }"#;
        let results = parse_response(body).unwrap();
        assert_eq!(results.total, 2);
        assert_eq!(results.datasets.len(), 2);

        let first = &results.datasets[0];
        assert_eq!(first.variable_id, vec!["tos".to_string()]);
        assert_eq!(first.source_id, vec!["CNRM-CM6-1".to_string()]);
        assert_eq!(first.urls.len(), 2);
        assert_eq!(
            first.opendap_url().as_deref(),
            Some("https://example.org/tos.nc")
        );

        // Minimal doc with missing title falls back to id, and missing url
        // array is tolerated.
        let second = &results.datasets[1];
        assert_eq!(second.title, second.id);
        assert!(second.urls.is_empty());
    }

    #[test]
    fn parse_response_rejects_malformed_json() {
        assert!(parse_response("not json").is_err());
    }

    #[test]
    fn files_query_emits_type_file_and_encoded_dataset_id() {
        let q = SearchQuery::cmip6_files_of(
            "CMIP6.ScenarioMIP.CNRM-CERFACS.CNRM-CM6-1.ssp245.r1i1p1f2.Omon.tos.gn.v20190219|esgf.ceda.ac.uk",
        );
        let s = q.to_query_string();
        assert!(s.contains("type=File"), "{s}");
        // `|` must be percent-encoded as %7C; `.` is unreserved, stays literal.
        assert!(s.contains("dataset_id="), "{s}");
        assert!(s.contains("%7Cesgf.ceda.ac.uk"), "{s}");
        assert!(!s.contains("type=Dataset"), "{s}");
    }

    #[test]
    fn percent_encoder_handles_cmip6_charset() {
        // Letters, digits, and `._-~` survive; `|`, `:`, `/` are encoded.
        assert_eq!(percent_encode_value("abc.DEF_123-xyz~"), "abc.DEF_123-xyz~");
        assert_eq!(percent_encode_value("a|b"), "a%7Cb");
        assert_eq!(percent_encode_value("a:b/c"), "a%3Ab%2Fc");
    }

    #[test]
    fn parse_file_response_extracts_files() {
        let body = r#"
        {
          "responseHeader": {"status": 0},
          "response": {
            "numFound": 1,
            "start": 0,
            "docs": [
              {
                "id": "CMIP6.x.tos_Omon_CNRM-CM6-1_ssp245_gn_201501-210012.nc|esgf.ceda.ac.uk",
                "title": "tos_Omon_CNRM-CM6-1_ssp245_gn_201501-210012.nc",
                "dataset_id": "CMIP6.x.v20190219|esgf.ceda.ac.uk",
                "size": 210893610,
                "datetime_start": "2015-01-01T12:00:00Z",
                "datetime_stop": "2100-12-31T12:00:00Z",
                "checksum": ["25fbde020da252da82bd85c75c5ff72ad8570434bb8d9461b77d0eec4d3d980e"],
                "checksum_type": ["SHA256"],
                "url": [
                  "https://example.org/fileServer/tos.nc|application/netcdf|HTTPServer",
                  "https://example.org/dodsC/tos.nc.html|application/opendap-html|OPENDAP"
                ]
              }
            ]
          }
        }"#;
        let results = parse_file_response(body).unwrap();
        assert_eq!(results.total, 1);
        assert_eq!(results.files.len(), 1);
        let f = &results.files[0];
        assert_eq!(f.size, Some(210_893_610));
        assert_eq!(f.datetime_start.as_deref(), Some("2015-01-01T12:00:00Z"));
        assert_eq!(f.checksum_type.as_deref(), Some("SHA256"));
        assert_eq!(
            f.opendap_url().as_deref(),
            Some("https://example.org/dodsC/tos.nc")
        );
        assert_eq!(
            f.http_url().as_deref(),
            Some("https://example.org/fileServer/tos.nc")
        );
    }

    #[test]
    fn parse_file_response_rejects_non_file_payload() {
        // A Dataset-shaped doc has no `dataset_id` field, so the File parser
        // refuses it rather than silently constructing a File with an empty
        // parent. This defends against a caller accidentally pointing
        // search_files() at the wrong URL.
        let body = r#"
        {
          "response": {
            "numFound": 1,
            "docs": [
              { "id": "x", "variable_id": ["tos"] }
            ]
          }
        }"#;
        assert!(parse_file_response(body).is_err());
    }
}
