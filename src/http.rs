//! Rate-limited HTTP client used by every outbound request in Ferrous.
//!
//! ESGF data nodes run on academic infrastructure — not cloud autoscaling — so
//! Ferrous defaults to a polite request cadence even when a user spawns the
//! CLI in a loop. Every call goes through a per-host [`RateLimiter`] that
//! enforces a minimum inter-request interval.
//!
//! Users who accept responsibility for heavier load can bypass this via the
//! forthcoming `--parallel` / `--no-rate-limit` CLI flags, which will
//! construct a [`Client`] with [`RateLimiter::unlimited`].

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::time::{sleep_until, Instant};

use crate::{Error, Result};

/// Default inter-request interval in polite mode.
///
/// Chosen conservatively: ESGF operators have publicly asked clients to keep
/// request rates "human-paced", so one per second per host is a safe default.
pub const DEFAULT_POLITE_INTERVAL: Duration = Duration::from_secs(1);

/// Default overall request timeout.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Default `User-Agent` sent with every request.
pub fn default_user_agent() -> String {
    format!(
        "ferrous/{} (+https://github.com/tham-le/ferrous)",
        crate::VERSION
    )
}

/// Simple per-host rate limiter based on a minimum inter-request interval.
///
/// Implemented as a map of `host -> last_release_instant` behind an async
/// mutex. [`RateLimiter::acquire`] sleeps until the next slot for the given
/// host is free, then records the release time.
#[derive(Debug, Clone)]
pub struct RateLimiter {
    min_interval: Duration,
    state: Arc<Mutex<HashMap<String, Instant>>>,
}

impl RateLimiter {
    /// Polite mode — enforce a minimum interval between requests to the same
    /// host. `min_interval = 0` disables rate limiting.
    pub fn new(min_interval: Duration) -> Self {
        Self {
            min_interval,
            state: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Default polite cadence (see [`DEFAULT_POLITE_INTERVAL`]).
    pub fn polite() -> Self {
        Self::new(DEFAULT_POLITE_INTERVAL)
    }

    /// No rate limiting. Use only when the caller has accepted responsibility
    /// for the load they produce.
    pub fn unlimited() -> Self {
        Self::new(Duration::ZERO)
    }

    /// Block until it is OK to send the next request to `host`. Records the
    /// release timestamp so the following caller waits the full interval.
    pub async fn acquire(&self, host: &str) {
        if self.min_interval.is_zero() {
            return;
        }
        let wait_until = {
            let mut state = self.state.lock().await;
            let now = Instant::now();
            let next = state
                .get(host)
                .map(|&last| last + self.min_interval)
                .unwrap_or(now);
            let release = next.max(now);
            state.insert(host.to_owned(), release);
            release
        };
        sleep_until(wait_until).await;
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::polite()
    }
}

/// HTTP client that routes every request through a [`RateLimiter`].
#[derive(Debug, Clone)]
pub struct Client {
    inner: reqwest::Client,
    limiter: RateLimiter,
}

impl Client {
    /// Build a polite client with sensible defaults.
    pub fn polite() -> Result<Self> {
        Self::builder().build()
    }

    /// Start a [`ClientBuilder`] for custom configuration.
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// Access the underlying `reqwest::Client` for advanced cases. Callers who
    /// use this bypass [`RateLimiter`] and are responsible for pacing.
    pub fn inner(&self) -> &reqwest::Client {
        &self.inner
    }

    /// Rate-limited GET returning the response body as bytes.
    pub async fn get_bytes(&self, url: &str) -> Result<Vec<u8>> {
        let host = host_of(url);
        self.limiter.acquire(&host).await;
        let resp = self
            .inner
            .get(url)
            .send()
            .await
            .map_err(|e| Error::Http(e.to_string()))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(Error::HttpStatus {
                status: status.as_u16(),
                url: url.to_owned(),
            });
        }
        let bytes = resp.bytes().await.map_err(|e| Error::Http(e.to_string()))?;
        Ok(bytes.to_vec())
    }

    /// Rate-limited GET returning the body as UTF-8 text.
    pub async fn get_text(&self, url: &str) -> Result<String> {
        let bytes = self.get_bytes(url).await?;
        String::from_utf8(bytes).map_err(|e| Error::Parse(e.to_string()))
    }
}

/// Builder for a configured [`Client`].
#[derive(Debug)]
pub struct ClientBuilder {
    user_agent: String,
    timeout: Duration,
    limiter: RateLimiter,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            user_agent: default_user_agent(),
            timeout: DEFAULT_REQUEST_TIMEOUT,
            limiter: RateLimiter::polite(),
        }
    }
}

impl ClientBuilder {
    /// Override the `User-Agent` header.
    pub fn user_agent(mut self, ua: impl Into<String>) -> Self {
        self.user_agent = ua.into();
        self
    }

    /// Override the request timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Override the rate limiter.
    pub fn rate_limiter(mut self, limiter: RateLimiter) -> Self {
        self.limiter = limiter;
        self
    }

    /// Shortcut to disable rate limiting entirely.
    pub fn no_rate_limit(self) -> Self {
        self.rate_limiter(RateLimiter::unlimited())
    }

    /// Finalise and build the [`Client`].
    pub fn build(self) -> Result<Client> {
        let inner = reqwest::Client::builder()
            .user_agent(self.user_agent)
            .timeout(self.timeout)
            .build()
            .map_err(|e| Error::Http(e.to_string()))?;
        Ok(Client {
            inner,
            limiter: self.limiter,
        })
    }
}

/// Extract the host portion from a URL, falling back to the full URL if
/// parsing fails. Used as the keying strategy for [`RateLimiter`].
fn host_of(url: &str) -> String {
    // Cheap and dependency-free: split on "://" then on the next '/'.
    let after_scheme = url.split_once("://").map(|(_, rest)| rest).unwrap_or(url);
    let host = after_scheme.split('/').next().unwrap_or(after_scheme);
    // Strip optional port + userinfo to key on the bare host.
    let host = host.split('@').next_back().unwrap_or(host);
    let host = host.split(':').next().unwrap_or(host);
    host.to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_of_extracts_simple_host() {
        assert_eq!(
            host_of("https://esgf-node.ipsl.upmc.fr/esg-search/search"),
            "esgf-node.ipsl.upmc.fr"
        );
    }

    #[test]
    fn host_of_strips_port() {
        assert_eq!(host_of("http://localhost:8080/data"), "localhost");
    }

    #[test]
    fn host_of_strips_userinfo() {
        assert_eq!(
            host_of("https://user:pass@host.example.com/path"),
            "host.example.com"
        );
    }

    #[test]
    fn host_of_lowercases() {
        assert_eq!(
            host_of("HTTPS://ESGF-NODE.LLNL.GOV/path"),
            "esgf-node.llnl.gov"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn rate_limiter_spaces_requests() {
        let limiter = RateLimiter::new(Duration::from_millis(500));
        let start = Instant::now();

        limiter.acquire("host.example").await;
        limiter.acquire("host.example").await;
        limiter.acquire("host.example").await;

        let elapsed = start.elapsed();
        // Three back-to-back requests with 500ms spacing: the first runs
        // immediately, the next two each wait ~500ms, so total >= 1s.
        assert!(
            elapsed >= Duration::from_millis(1000),
            "expected at least 1s, got {elapsed:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn rate_limiter_is_per_host() {
        let limiter = RateLimiter::new(Duration::from_millis(500));
        let start = Instant::now();

        limiter.acquire("a.example").await;
        limiter.acquire("b.example").await;
        limiter.acquire("c.example").await;

        // All three go to different hosts, so no throttling applies.
        assert!(start.elapsed() < Duration::from_millis(100));
    }

    #[tokio::test]
    async fn unlimited_limiter_never_blocks() {
        let limiter = RateLimiter::unlimited();
        let start = std::time::Instant::now();
        for _ in 0..100 {
            limiter.acquire("host.example").await;
        }
        assert!(start.elapsed() < Duration::from_millis(100));
    }

    #[test]
    fn default_user_agent_contains_version() {
        let ua = default_user_agent();
        assert!(ua.starts_with("ferrous/"));
        assert!(ua.contains(crate::VERSION));
    }

    #[test]
    fn client_builds_with_defaults() {
        let client = Client::polite().expect("build must succeed with defaults");
        let _ = client.inner(); // smoke: accessor works
    }
}
