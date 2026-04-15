//! Coordinate-axis resolution: convert human-readable ranges (degrees, dates)
//! into integer array indices.
//!
//! Today this module handles the simple-but-common case: **1D monotonic
//! coordinate arrays**. CMIP6 atmospheric data (tas, pr, ua, va, …) almost
//! always uses 1D lat/lon, so this covers a large fraction of real queries.
//!
//! 2D curvilinear coordinates (CMIP6 ocean tri-polar grids — `nav_lat[y, x]`,
//! `nav_lon[y, x]`) are out of scope here; the caller is expected to detect
//! the shape and either ask the user for index ranges directly or invoke a
//! future curvilinear resolver.

use crate::{Error, Result};

/// Inclusive index range `[start ..= stop]` as understood by the DAP2 spec.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexRange {
    pub start: usize,
    pub stop: usize,
}

impl IndexRange {
    /// Number of elements the range selects.
    pub fn len(&self) -> usize {
        self.stop.saturating_sub(self.start) + 1
    }

    /// `true` if the range is empty (start > stop).
    pub fn is_empty(&self) -> bool {
        self.stop < self.start
    }
}

/// Resolve a target value range against a 1D coordinate axis.
///
/// Returns the inclusive index range `[i_start ..= i_stop]` covering every
/// axis element whose value lies in `[target_min, target_max]`.
///
/// The axis must be **monotonic** (every adjacent pair is either strictly
/// increasing or strictly decreasing). Real coordinate arrays satisfy this
/// — latitudes go -90 → +90 (or +90 → -90), longitudes 0 → 360 (or
/// -180 → +180). We don't currently auto-rotate longitude conventions; the
/// caller passes a target in the same convention as the axis.
pub fn resolve_range(axis: &[f64], target_min: f64, target_max: f64) -> Result<IndexRange> {
    if axis.len() < 2 {
        return Err(Error::InvalidArgument(format!(
            "coordinate axis has only {} element(s); need >= 2 to resolve a range",
            axis.len()
        )));
    }
    if !target_min.is_finite() || !target_max.is_finite() {
        return Err(Error::InvalidArgument(
            "target range bounds must be finite numbers".into(),
        ));
    }
    if target_max < target_min {
        return Err(Error::InvalidArgument(format!(
            "target max ({target_max}) is less than target min ({target_min})"
        )));
    }

    let increasing = is_increasing(axis)?;
    let (lo, hi) = if increasing {
        (target_min, target_max)
    } else {
        // For a descending axis, "min..max" in axis-value terms covers the
        // same elements but the array indices are reversed; we still return
        // start <= stop.
        (target_min, target_max)
    };

    // For both increasing and decreasing axes:
    //   start = first index whose value >= lo (increasing)
    //         = first index whose value <= hi (decreasing)
    //   stop  = last index in the axis whose value is still in range
    let mut start: Option<usize> = None;
    let mut stop: Option<usize> = None;
    for (i, &v) in axis.iter().enumerate() {
        if (lo..=hi).contains(&v) {
            if start.is_none() {
                start = Some(i);
            }
            stop = Some(i);
        }
    }
    match (start, stop) {
        (Some(s), Some(e)) => Ok(IndexRange { start: s, stop: e }),
        _ => Err(Error::InvalidArgument(format!(
            "no axis values fall in [{target_min}, {target_max}] (axis spans {} to {})",
            axis.first().copied().unwrap_or(f64::NAN),
            axis.last().copied().unwrap_or(f64::NAN),
        ))),
    }
}

/// `true` if every adjacent pair is strictly increasing.
/// Errors if the axis is not strictly monotonic in either direction.
fn is_increasing(axis: &[f64]) -> Result<bool> {
    let increasing = axis[1] > axis[0];
    for w in axis.windows(2) {
        let (a, b) = (w[0], w[1]);
        if increasing && b <= a {
            return Err(Error::InvalidArgument(format!(
                "coordinate axis is not strictly monotonic: axis[i]={a}, axis[i+1]={b}"
            )));
        }
        if !increasing && b >= a {
            return Err(Error::InvalidArgument(format!(
                "coordinate axis is not strictly monotonic: axis[i]={a}, axis[i+1]={b}"
            )));
        }
    }
    Ok(increasing)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lats_increasing() -> Vec<f64> {
        // 19 points from -90 to +90, 10° spacing.
        (0..19).map(|i| -90.0 + 10.0 * i as f64).collect()
    }

    fn lats_decreasing() -> Vec<f64> {
        let mut v = lats_increasing();
        v.reverse();
        v
    }

    #[test]
    fn resolves_sub_range_on_increasing_axis() {
        // Mediterranean-ish: 30-50N. With 10° spacing starting at -90:
        //   index  0   1   2   3   4   5   6   7   8   9  10  11  12  13  14  15  16  17  18
        //   value -90 -80 -70 -60 -50 -40 -30 -20 -10  0  10  20  30  40  50  60  70  80  90
        // 30 -> idx 12; 50 -> idx 14.
        let axis = lats_increasing();
        let r = resolve_range(&axis, 30.0, 50.0).unwrap();
        assert_eq!(
            r,
            IndexRange {
                start: 12,
                stop: 14
            }
        );
        assert_eq!(r.len(), 3);
    }

    #[test]
    fn resolves_sub_range_on_decreasing_axis() {
        // Same range on a top-down axis: indices flip but length is the same.
        let axis = lats_decreasing();
        let r = resolve_range(&axis, 30.0, 50.0).unwrap();
        // axis[4] = 50, axis[5] = 40, axis[6] = 30
        assert_eq!(r, IndexRange { start: 4, stop: 6 });
        assert_eq!(r.len(), 3);
    }

    #[test]
    fn single_value_range_picks_one_index() {
        // [target, target] on an exact axis value: 1 element.
        let axis = lats_increasing();
        let r = resolve_range(&axis, 0.0, 0.0).unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(axis[r.start], 0.0);
    }

    #[test]
    fn full_range_covers_every_index() {
        let axis = lats_increasing();
        let r = resolve_range(&axis, -90.0, 90.0).unwrap();
        assert_eq!(
            r,
            IndexRange {
                start: 0,
                stop: axis.len() - 1
            }
        );
    }

    #[test]
    fn no_match_errors_with_axis_span() {
        let axis = lats_increasing();
        // 100..120 is entirely above the +90 axis end.
        let err = resolve_range(&axis, 100.0, 120.0).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("no axis values fall in"), "{msg}");
    }

    #[test]
    fn rejects_reversed_target() {
        let axis = lats_increasing();
        assert!(resolve_range(&axis, 50.0, 30.0).is_err());
    }

    #[test]
    fn rejects_non_finite_bounds() {
        let axis = lats_increasing();
        assert!(resolve_range(&axis, f64::NAN, 30.0).is_err());
        assert!(resolve_range(&axis, 0.0, f64::INFINITY).is_err());
    }

    #[test]
    fn rejects_short_axis() {
        assert!(resolve_range(&[10.0], 0.0, 20.0).is_err());
        assert!(resolve_range(&[], 0.0, 20.0).is_err());
    }

    #[test]
    fn rejects_non_monotonic_axis() {
        // Up then down: not monotonic.
        let axis = vec![0.0, 10.0, 5.0, 15.0];
        assert!(resolve_range(&axis, 0.0, 20.0).is_err());
    }

    #[test]
    fn rejects_axis_with_repeated_values() {
        // Strict monotonicity required — equal adjacent values fail.
        let axis = vec![0.0, 10.0, 10.0, 20.0];
        assert!(resolve_range(&axis, 0.0, 20.0).is_err());
    }
}
