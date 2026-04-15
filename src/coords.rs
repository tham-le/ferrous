//! Coordinate-axis resolution: convert human-readable ranges (degrees, dates)
//! into integer array indices.
//!
//! Two entry points:
//!
//! * [`resolve_range`] — 1D monotonic axis → `IndexRange`. Covers CMIP6
//!   atmospheric data and regular-grid ocean / land datasets.
//! * [`resolve_2d_bbox`] — 2D curvilinear `lat[y,x]` / `lon[y,x]` →
//!   `(y_range, x_range)` bounding box. Covers CMIP6 ocean tri-polar grids
//!   and rotated-pole atmospheric models.
//!
//! The caller picks which based on the coordinate variable's dimensionality
//! (1D vs 2D) after decoding it via [`crate::dap2`].

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

/// Resolve a target lat/lon range against **2D curvilinear** coordinate
/// arrays — CMIP6 ocean tri-polar grids, Arakawa grids, and rotated-pole
/// atmosphere models.
///
/// `lat_2d` and `lon_2d` are flat, row-major, shape `(ny, nx)` — the natural
/// in-memory layout of a `lat[y][x]` / `lon[y][x]` DAP2 array. Returns the
/// **bounding box** (`y_range`, `x_range`) over all cells whose value is in
/// both the lat and lon target ranges.
///
/// Because curvilinear grids don't have separable axes, the bounding box may
/// over-select by a few rows or columns — cells at the corners of the box
/// can fall outside the target region. This is tolerable: the DAP2
/// constraint language only supports rectangular slices, so bbox is the
/// tightest expressible request.
pub fn resolve_2d_bbox(
    lat_2d: &[f64],
    lon_2d: &[f64],
    shape: (usize, usize),
    lat_range: (f64, f64),
    lon_range: (f64, f64),
) -> Result<(IndexRange, IndexRange)> {
    let (ny, nx) = shape;
    if ny == 0 || nx == 0 {
        return Err(Error::InvalidArgument(format!(
            "2D coord shape must be non-zero, got {ny}x{nx}"
        )));
    }
    let expected = ny
        .checked_mul(nx)
        .ok_or_else(|| Error::InvalidArgument("2D coord shape overflows usize".into()))?;
    if lat_2d.len() != expected || lon_2d.len() != expected {
        return Err(Error::InvalidArgument(format!(
            "2D coord arrays have {}/{} elements, expected {} for shape {ny}x{nx}",
            lat_2d.len(),
            lon_2d.len(),
            expected
        )));
    }
    let (lat_lo, lat_hi) = lat_range;
    let (lon_lo, lon_hi) = lon_range;
    if lat_hi < lat_lo || lon_hi < lon_lo {
        return Err(Error::InvalidArgument(
            "target range max must be >= min for both lat and lon".into(),
        ));
    }
    for (name, v) in [
        ("lat", lat_lo),
        ("lat", lat_hi),
        ("lon", lon_lo),
        ("lon", lon_hi),
    ] {
        if !v.is_finite() {
            return Err(Error::InvalidArgument(format!(
                "{name} bound {v} is not finite"
            )));
        }
    }

    let mut min_y = usize::MAX;
    let mut max_y = 0usize;
    let mut min_x = usize::MAX;
    let mut max_x = 0usize;
    let mut found = false;
    for y in 0..ny {
        let row = y * nx;
        for x in 0..nx {
            let i = row + x;
            let la = lat_2d[i];
            let lo = lon_2d[i];
            if la >= lat_lo && la <= lat_hi && lo >= lon_lo && lo <= lon_hi {
                if y < min_y {
                    min_y = y;
                }
                if y > max_y {
                    max_y = y;
                }
                if x < min_x {
                    min_x = x;
                }
                if x > max_x {
                    max_x = x;
                }
                found = true;
            }
        }
    }
    if !found {
        return Err(Error::InvalidArgument(format!(
            "no 2D coord cells fall in lat {lat_lo}..{lat_hi}, lon {lon_lo}..{lon_hi} \
             (grid is {ny}x{nx})"
        )));
    }
    Ok((
        IndexRange {
            start: min_y,
            stop: max_y,
        },
        IndexRange {
            start: min_x,
            stop: max_x,
        },
    ))
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

    /// Build a synthetic 4x5 regular grid for 2D tests. Lat varies with y
    /// (0, 10, 20, 30) and lon varies with x (0, 10, 20, 30, 40). Even on a
    /// regular grid, `resolve_2d_bbox` must return the correct (y, x) bbox.
    fn regular_grid_4x5() -> (Vec<f64>, Vec<f64>, (usize, usize)) {
        let ny = 4;
        let nx = 5;
        let mut lat = vec![0.0; ny * nx];
        let mut lon = vec![0.0; ny * nx];
        for y in 0..ny {
            for x in 0..nx {
                let i = y * nx + x;
                lat[i] = 10.0 * y as f64;
                lon[i] = 10.0 * x as f64;
            }
        }
        (lat, lon, (ny, nx))
    }

    #[test]
    fn resolves_bbox_on_regular_2d_grid() {
        // Target lat 10..20, lon 20..30 -> y ∈ {1, 2}, x ∈ {2, 3}
        let (lat, lon, shape) = regular_grid_4x5();
        let (y, x) = resolve_2d_bbox(&lat, &lon, shape, (10.0, 20.0), (20.0, 30.0)).unwrap();
        assert_eq!(y, IndexRange { start: 1, stop: 2 });
        assert_eq!(x, IndexRange { start: 2, stop: 3 });
    }

    #[test]
    fn single_cell_bbox_has_start_equals_stop() {
        let (lat, lon, shape) = regular_grid_4x5();
        // Target pin-point at (y=2, x=3): lat=20, lon=30.
        let (y, x) = resolve_2d_bbox(&lat, &lon, shape, (20.0, 20.0), (30.0, 30.0)).unwrap();
        assert_eq!(y, IndexRange { start: 2, stop: 2 });
        assert_eq!(x, IndexRange { start: 3, stop: 3 });
    }

    #[test]
    fn full_grid_bbox() {
        let (lat, lon, shape) = regular_grid_4x5();
        let (y, x) = resolve_2d_bbox(&lat, &lon, shape, (-10.0, 100.0), (-10.0, 100.0)).unwrap();
        assert_eq!(y, IndexRange { start: 0, stop: 3 });
        assert_eq!(x, IndexRange { start: 0, stop: 4 });
    }

    #[test]
    fn out_of_range_target_errors() {
        let (lat, lon, shape) = regular_grid_4x5();
        assert!(resolve_2d_bbox(&lat, &lon, shape, (100.0, 200.0), (0.0, 10.0)).is_err());
    }

    #[test]
    fn rejects_mismatched_shape() {
        let (mut lat, lon, shape) = regular_grid_4x5();
        lat.pop();
        assert!(resolve_2d_bbox(&lat, &lon, shape, (0.0, 10.0), (0.0, 10.0)).is_err());
    }

    #[test]
    fn rejects_zero_shape() {
        assert!(resolve_2d_bbox(&[], &[], (0, 5), (0.0, 1.0), (0.0, 1.0)).is_err());
        assert!(resolve_2d_bbox(&[], &[], (5, 0), (0.0, 1.0), (0.0, 1.0)).is_err());
    }

    #[test]
    fn rejects_reversed_target_range() {
        let (lat, lon, shape) = regular_grid_4x5();
        assert!(resolve_2d_bbox(&lat, &lon, shape, (20.0, 10.0), (0.0, 10.0)).is_err());
        assert!(resolve_2d_bbox(&lat, &lon, shape, (0.0, 10.0), (20.0, 10.0)).is_err());
    }

    #[test]
    fn bbox_2d_rejects_non_finite_bounds() {
        let (lat, lon, shape) = regular_grid_4x5();
        assert!(resolve_2d_bbox(&lat, &lon, shape, (f64::NAN, 10.0), (0.0, 10.0)).is_err());
    }

    #[test]
    fn bbox_over_selects_on_curvilinear_grid() {
        // Tri-polar-ish synthetic grid: warp lon such that a single lon range
        // maps to a non-rectangular set of cells. The bbox should include the
        // y ∈ {0, 1} extent even though only (0, 0) and (1, 1) are strictly
        // inside the target.
        let lat = vec![0.0, 0.0, 10.0, 10.0];
        let lon = vec![0.0, 50.0, 50.0, 0.0];
        let (y, x) = resolve_2d_bbox(&lat, &lon, (2, 2), (0.0, 10.0), (0.0, 10.0)).unwrap();
        // Cells at (0,0) lat=0 lon=0 and (1,1) lat=10 lon=0 are inside.
        assert_eq!(y, IndexRange { start: 0, stop: 1 });
        assert_eq!(x, IndexRange { start: 0, stop: 1 });
        // Note: the bbox over-selects (0,1) and (1,0) which are outside the
        // target region — this is expected on a curvilinear grid.
    }
}
