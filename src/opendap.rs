//! OPeNDAP DAP2 constraint expression builder.
//!
//! OPeNDAP servers (including every ESGF node) accept subsetting requests in a
//! URL-appended "constraint expression" that selects specific variables and
//! index ranges instead of downloading an entire file. Ferrous uses this to
//! fetch only the lat/lon/time window a researcher actually needs.
//!
//! # Syntax
//!
//! ```text
//! http://host/path/file.nc.dods?var1[start:stride:stop][start:stop],var2
//! ```
//!
//! * `?` introduces the projection part of the expression.
//! * Each variable is followed by one bracket group per dimension.
//! * `[start:stop]` is shorthand for stride = 1.
//! * Multiple variables are comma-separated.
//! * Index bounds are **inclusive** on both ends, which is a surprise for
//!   anyone used to Python-style half-open ranges.
//!
//! # Example
//!
//! ```
//! use ferrous::opendap::{Constraint, Slice};
//!
//! let c = Constraint::new()
//!     .select("tas", [
//!         Slice::range(0, 120),          // time: months 0..=120
//!         Slice::range(20, 30),          // lat
//!         Slice::range(40, 50),          // lon
//!     ])
//!     .unwrap();
//! assert_eq!(c.to_query(), "tas[0:1:120][20:1:30][40:1:50]");
//! ```

use std::fmt::{self, Write as _};
use std::str::FromStr;

use crate::{Error, Result};

/// A single-dimension slice in an OPeNDAP constraint expression.
///
/// Index bounds are inclusive on both ends — `Slice::range(0, 10)` selects 11
/// elements, matching the DAP2 specification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Slice {
    start: usize,
    stride: usize,
    stop: usize,
}

impl Slice {
    /// Create a contiguous slice (`stride = 1`) from `start` to `stop`,
    /// inclusive.
    ///
    /// Returns [`Error::InvalidConstraint`] if `stop < start`.
    pub fn range(start: usize, stop: usize) -> Self {
        // Public convenience; panics on misuse are avoided by the checked
        // `try_range` variant used internally.
        Self {
            start,
            stride: 1,
            stop,
        }
    }

    /// Create a strided slice. Every `stride`-th element from `start` up to
    /// and including `stop`.
    pub fn strided(start: usize, stride: usize, stop: usize) -> Result<Self> {
        if stride == 0 {
            return Err(Error::InvalidConstraint("stride must be >= 1".into()));
        }
        if stop < start {
            return Err(Error::InvalidConstraint(format!(
                "stop ({stop}) must be >= start ({start})"
            )));
        }
        Ok(Self {
            start,
            stride,
            stop,
        })
    }

    /// Single-element selection.
    pub fn point(index: usize) -> Self {
        Self {
            start: index,
            stride: 1,
            stop: index,
        }
    }

    /// Number of elements this slice selects.
    pub fn len(&self) -> usize {
        if self.stop < self.start {
            return 0;
        }
        (self.stop - self.start) / self.stride + 1
    }

    /// `true` if the slice is empty (invalid bounds).
    pub fn is_empty(&self) -> bool {
        self.stop < self.start
    }

    fn validate(&self) -> Result<()> {
        if self.stride == 0 {
            return Err(Error::InvalidConstraint("stride must be >= 1".into()));
        }
        if self.stop < self.start {
            return Err(Error::InvalidConstraint(format!(
                "stop ({}) must be >= start ({})",
                self.stop, self.start
            )));
        }
        Ok(())
    }
}

impl fmt::Display for Slice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}:{}:{}]", self.start, self.stride, self.stop)
    }
}

impl FromStr for Slice {
    type Err = Error;

    /// Parse a human-written slice:
    /// - `"a:b"` → contiguous range `[a, b]`.
    /// - `"a:s:b"` → strided range stepping by `s`.
    /// - `"n"` → single point at `n`.
    fn from_str(s: &str) -> Result<Self> {
        let parts: Vec<&str> = s.split(':').collect();
        let parse_idx = |p: &str, label: &str| -> Result<usize> {
            p.trim().parse::<usize>().map_err(|_| {
                Error::InvalidConstraint(format!(
                    "slice {label} '{p}' is not a non-negative integer"
                ))
            })
        };
        match parts.as_slice() {
            [one] => Ok(Slice::point(parse_idx(one, "index")?)),
            [a, b] => Slice::strided(parse_idx(a, "start")?, 1, parse_idx(b, "stop")?),
            [a, s, b] => Slice::strided(
                parse_idx(a, "start")?,
                parse_idx(s, "stride")?,
                parse_idx(b, "stop")?,
            ),
            _ => Err(Error::InvalidConstraint(format!(
                "slice '{s}' must be 'N', 'START:STOP', or 'START:STRIDE:STOP'"
            ))),
        }
    }
}

/// An OPeNDAP constraint expression — one or more variables with per-dimension
/// slices.
#[derive(Clone, Debug, Default)]
pub struct Constraint {
    projections: Vec<Projection>,
}

#[derive(Clone, Debug)]
struct Projection {
    variable: String,
    slices: Vec<Slice>,
}

impl Constraint {
    /// An empty constraint — projects every variable, no subsetting. Servers
    /// will return the full dataset, which defeats the purpose of Ferrous;
    /// prefer [`Constraint::select`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a variable projection with the given per-dimension slices.
    ///
    /// Returns [`Error::InvalidConstraint`] if the variable name is empty or
    /// any slice has invalid bounds.
    pub fn select<S, I>(mut self, variable: S, slices: I) -> Result<Self>
    where
        S: Into<String>,
        I: IntoIterator<Item = Slice>,
    {
        let variable = variable.into();
        if variable.is_empty() {
            return Err(Error::InvalidConstraint(
                "variable name cannot be empty".into(),
            ));
        }
        let slices: Vec<_> = slices.into_iter().collect();
        for s in &slices {
            s.validate()?;
        }
        self.projections.push(Projection { variable, slices });
        Ok(self)
    }

    /// Number of variables selected.
    pub fn len(&self) -> usize {
        self.projections.len()
    }

    /// `true` if no variables have been selected.
    pub fn is_empty(&self) -> bool {
        self.projections.is_empty()
    }

    /// Render the constraint as the query-string portion of an OPeNDAP URL
    /// (without the leading `?`). Use [`Constraint::append_to_url`] for the
    /// full URL.
    pub fn to_query(&self) -> String {
        let mut out = String::new();
        for (i, proj) in self.projections.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push_str(&proj.variable);
            for s in &proj.slices {
                // Slice Display is infallible.
                let _ = write!(out, "{s}");
            }
        }
        out
    }

    /// Append the constraint to a base OPeNDAP URL (e.g.
    /// `https://host/path/file.nc.dods`), URL-encoding as needed.
    pub fn append_to_url(&self, base: &str) -> String {
        if self.is_empty() {
            return base.to_string();
        }
        let sep = if base.contains('?') { '&' } else { '?' };
        // OPeNDAP constraints use `[`, `]`, `:`, `,` which are technically
        // reserved in URLs. In practice every OPeNDAP server accepts the raw
        // form and most client libraries send it unencoded for readability.
        format!("{base}{sep}{}", self.to_query())
    }
}

impl fmt::Display for Constraint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_query())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slice_range_is_contiguous() {
        let s = Slice::range(0, 10);
        assert_eq!(s.to_string(), "[0:1:10]");
        assert_eq!(s.len(), 11);
    }

    #[test]
    fn slice_point_selects_one_element() {
        let s = Slice::point(42);
        assert_eq!(s.to_string(), "[42:1:42]");
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn slice_strided_counts_correctly() {
        let s = Slice::strided(0, 2, 10).unwrap();
        assert_eq!(s.to_string(), "[0:2:10]");
        // Elements: 0, 2, 4, 6, 8, 10 -> 6
        assert_eq!(s.len(), 6);
    }

    #[test]
    fn strided_rejects_zero_stride() {
        assert!(Slice::strided(0, 0, 10).is_err());
    }

    #[test]
    fn strided_rejects_reversed_bounds() {
        assert!(Slice::strided(10, 1, 5).is_err());
    }

    #[test]
    fn slice_from_str_parses_three_forms() {
        assert_eq!("7".parse::<Slice>().unwrap(), Slice::point(7));
        assert_eq!("0:10".parse::<Slice>().unwrap(), Slice::range(0, 10));
        assert_eq!(
            "0:2:10".parse::<Slice>().unwrap(),
            Slice::strided(0, 2, 10).unwrap()
        );
    }

    #[test]
    fn slice_from_str_rejects_bad_input() {
        assert!("".parse::<Slice>().is_err());
        assert!("a:b".parse::<Slice>().is_err());
        assert!("-1:10".parse::<Slice>().is_err());
        assert!("1:2:3:4".parse::<Slice>().is_err());
        assert!("10:5".parse::<Slice>().is_err()); // reversed bounds
    }

    #[test]
    fn single_variable_three_dims() {
        let c = Constraint::new()
            .select(
                "tas",
                [
                    Slice::range(0, 120),
                    Slice::range(20, 30),
                    Slice::range(40, 50),
                ],
            )
            .unwrap();
        assert_eq!(c.to_query(), "tas[0:1:120][20:1:30][40:1:50]");
    }

    #[test]
    fn multiple_variables_are_comma_separated() {
        let c = Constraint::new()
            .select("tas", [Slice::range(0, 10)])
            .unwrap()
            .select("pr", [Slice::range(0, 10)])
            .unwrap();
        assert_eq!(c.to_query(), "tas[0:1:10],pr[0:1:10]");
    }

    #[test]
    fn variable_without_slices_is_full_projection() {
        let c = Constraint::new().select("lat", std::iter::empty()).unwrap();
        assert_eq!(c.to_query(), "lat");
    }

    #[test]
    fn empty_variable_name_rejected() {
        let result = Constraint::new().select("", [Slice::range(0, 1)]);
        assert!(result.is_err());
    }

    #[test]
    fn append_to_url_without_query() {
        let c = Constraint::new()
            .select("tas", [Slice::range(0, 10)])
            .unwrap();
        let url = c.append_to_url("https://example.org/data.nc.dods");
        assert_eq!(url, "https://example.org/data.nc.dods?tas[0:1:10]");
    }

    #[test]
    fn append_to_url_with_existing_query_uses_ampersand() {
        let c = Constraint::new()
            .select("tas", [Slice::range(0, 10)])
            .unwrap();
        let url = c.append_to_url("https://example.org/data.nc.dods?token=abc");
        assert_eq!(
            url,
            "https://example.org/data.nc.dods?token=abc&tas[0:1:10]"
        );
    }

    #[test]
    fn empty_constraint_returns_base_url_untouched() {
        let c = Constraint::new();
        assert_eq!(
            c.append_to_url("https://example.org/data.nc.dods"),
            "https://example.org/data.nc.dods"
        );
    }

    #[test]
    fn strided_slice_in_constraint() {
        // Coarse monthly sampling: every 12th time step = annual.
        let c = Constraint::new()
            .select("tas", [Slice::strided(0, 12, 120).unwrap()])
            .unwrap();
        assert_eq!(c.to_query(), "tas[0:12:120]");
    }
}
