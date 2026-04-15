//! Error types for the `ferrous` crate.
//!
//! A single [`Error`] enum covers every fallible operation in the library. The
//! [`Result`] alias is the default return type used throughout the crate.

use std::io;

/// Crate-wide result alias.
pub type Result<T> = std::result::Result<T, Error>;

/// Top-level error type for every fallible operation in `ferrous`.
///
/// Variants are kept deliberately narrow so callers can match on specific
/// failure modes (e.g. rate-limit vs. upstream outage vs. malformed response)
/// and react appropriately.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Underlying HTTP transport failed (DNS, TLS, connection reset, etc.).
    #[error("HTTP transport error: {0}")]
    Http(String),

    /// An HTTP response was received but its status indicates failure.
    #[error("HTTP {status} from {url}")]
    HttpStatus {
        /// HTTP status code returned by the server.
        status: u16,
        /// URL that produced the failing response.
        url: String,
    },

    /// Upstream server returned a payload we couldn't parse.
    #[error("failed to parse response: {0}")]
    Parse(String),

    /// An OPeNDAP constraint expression was constructed from invalid inputs.
    #[error("invalid OPeNDAP constraint: {0}")]
    InvalidConstraint(String),

    /// A user-supplied argument was outside the allowed range or shape.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// An ESGF search query matched no datasets.
    #[error("no datasets match the query")]
    NoResults,

    /// Local filesystem or I/O failure.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

impl Error {
    /// Returns `true` if the error is worth retrying (transient network /
    /// upstream issues). Caller side decides on backoff strategy.
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Http(_) => true,
            Self::HttpStatus { status, .. } => matches!(status, 500..=599 | 429),
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_errors_are_transient() {
        let err = Error::Http("connection reset".into());
        assert!(err.is_transient());
    }

    #[test]
    fn server_5xx_is_transient() {
        let err = Error::HttpStatus {
            status: 503,
            url: "https://example.com".into(),
        };
        assert!(err.is_transient());
    }

    #[test]
    fn rate_limit_is_transient() {
        let err = Error::HttpStatus {
            status: 429,
            url: "https://example.com".into(),
        };
        assert!(err.is_transient());
    }

    #[test]
    fn client_4xx_is_not_transient() {
        let err = Error::HttpStatus {
            status: 404,
            url: "https://example.com".into(),
        };
        assert!(!err.is_transient());
    }

    #[test]
    fn parse_error_is_not_transient() {
        let err = Error::Parse("bad json".into());
        assert!(!err.is_transient());
    }

    #[test]
    fn io_errors_convert_automatically() {
        let io_err = io::Error::new(io::ErrorKind::PermissionDenied, "nope");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Io(_)));
    }
}
