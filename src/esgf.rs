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

/// Default search endpoint used when the caller does not pick a node.
///
/// Live-verified working 2026-04. IPSL was the original default but is
/// currently 500-ing on Dataset queries.
pub const DEFAULT_SEARCH_ENDPOINT: &str = "https://esgf.ceda.ac.uk/esg-search/search";

/// Maximum number of datasets returned per search request.
pub const DEFAULT_LIMIT: usize = 50;

/// A dataset search query against an ESGF node.
///
/// Fields are optional "facets" — the ESGF Solr backend matches any combination
/// the caller provides. Only `project` has a default (`CMIP6`) because every
/// other project uses an entirely different schema and a naked search across
/// all projects is rarely what a user means.
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
            limit: DEFAULT_LIMIT,
            offset: 0,
        }
    }
}

impl SearchQuery {
    /// Start a CMIP6 query.
    pub fn cmip6() -> Self {
        Self::default()
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
        // All facets we emit are safe ASCII (CMIP6 identifiers are
        // `[A-Za-z0-9._-]`), so naive formatting is sufficient and avoids a
        // url-crate dependency at this stage. If we ever accept user-facing
        // free-text queries this must be revisited.
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
        parts.push(format!("limit={}", self.limit));
        parts.push(format!("offset={}", self.offset));
        // type=Dataset keeps aggregated datasets out of the File response.
        parts.push("type=Dataset".into());
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

/// Parse a Solr JSON response body into [`SearchResults`].
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
}
