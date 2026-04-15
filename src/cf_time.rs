//! CF-compliant time-axis handling for ESGF NetCDF datasets.
//!
//! CMIP6 time coordinates use the [CF Metadata Conventions](https://cfconventions.org)
//! encoding:
//!
//! ```text
//! time:units    = "days since 1850-01-01 00:00:00"
//! time:calendar = "gregorian"  (or "standard", "noleap", "360_day", ...)
//! ```
//!
//! Axis values are numbers in whatever unit `units` advertises since whatever
//! epoch it advertises. To convert a user-facing ISO date range (e.g.
//! `2020-01-01:2050-12-31`) into array indices we must:
//!
//! 1. Fetch the DAS response and extract the time variable's `units` and
//!    `calendar` attributes.
//! 2. Parse those into a [`CfTimeAxis`] that knows how to convert dates.
//! 3. Convert user ISO endpoints to axis-value numbers.
//! 4. Hand the axis-value range to [`crate::coords::resolve_range`].
//!
//! This module handles the Gregorian / standard / proleptic_gregorian
//! calendars via [`chrono`] and the `noleap` / `365_day` calendar via a
//! manual day-count. Other calendars (`360_day`, `all_leap`, `julian`) error
//! eagerly — they're rare enough that erroring is better than silently
//! off-by-a-few-days resolution.

use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime};

use crate::{Error, Result};

/// Supported CF calendars.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Calendar {
    /// Proleptic Gregorian — chrono's default. Covers `"gregorian"`,
    /// `"standard"`, and `"proleptic_gregorian"`.
    Gregorian,
    /// 365-day calendar with no leap days (CMIP6 `"noleap"` / `"365_day"`).
    NoLeap,
}

impl Calendar {
    fn from_cf(s: &str) -> Result<Self> {
        match s {
            "gregorian" | "standard" | "proleptic_gregorian" => Ok(Self::Gregorian),
            "noleap" | "365_day" => Ok(Self::NoLeap),
            other => Err(Error::Parse(format!(
                "CF calendar '{other}' is not yet supported by ferrous \
                 (supported: gregorian / standard / proleptic_gregorian, noleap / 365_day)"
            ))),
        }
    }
}

/// Unit of a CF "<unit> since <epoch>" spec.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Unit {
    Days,
    Hours,
    Minutes,
    Seconds,
}

impl Unit {
    fn from_cf(s: &str) -> Result<Self> {
        // CF allows "day" / "days", "hour" / "hours", etc.
        match s {
            "day" | "days" => Ok(Self::Days),
            "hour" | "hours" => Ok(Self::Hours),
            "minute" | "minutes" => Ok(Self::Minutes),
            "second" | "seconds" => Ok(Self::Seconds),
            other => Err(Error::Parse(format!(
                "CF time unit '{other}' is not supported"
            ))),
        }
    }

    /// Convert a floating-point number of seconds to this unit.
    fn convert_from_seconds(self, seconds: f64) -> f64 {
        match self {
            Self::Days => seconds / 86_400.0,
            Self::Hours => seconds / 3_600.0,
            Self::Minutes => seconds / 60.0,
            Self::Seconds => seconds,
        }
    }
}

/// A parsed CF time axis.
#[derive(Clone, Debug)]
pub struct CfTimeAxis {
    epoch: NaiveDateTime,
    unit: Unit,
    calendar: Calendar,
}

impl CfTimeAxis {
    /// Look up the time variable's attributes inside a DAS response, then
    /// parse them. Uses the shared [`crate::das`] parser.
    pub fn from_das(das: &str, time_var: &str) -> Result<Self> {
        let parsed = crate::das::parse(das);
        let block = parsed
            .get(time_var)
            .ok_or_else(|| Error::Parse(format!("DAS does not contain a '{time_var}' block")))?;

        let mut units: Option<&str> = None;
        let mut calendar_str: Option<&str> = None;
        for (name, value) in block {
            if let crate::das::DasValue::Text(s) = value {
                match name.as_str() {
                    "units" => units = Some(s),
                    "calendar" => calendar_str = Some(s),
                    _ => {}
                }
            }
        }
        let units = units.ok_or_else(|| {
            Error::Parse(format!(
                "time variable '{time_var}' has no 'units' attribute"
            ))
        })?;
        let calendar = Calendar::from_cf(calendar_str.unwrap_or("gregorian"))?;
        let (unit, epoch) = parse_units(units)?;
        Ok(Self {
            epoch,
            unit,
            calendar,
        })
    }

    /// Advertised calendar.
    pub fn calendar(&self) -> Calendar {
        self.calendar
    }

    /// Convert a calendar date into an axis value in the axis's native unit.
    pub fn date_to_axis(&self, d: NaiveDate) -> Result<f64> {
        self.datetime_to_axis(d.and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap()))
    }

    /// Convert a calendar datetime into an axis value.
    pub fn datetime_to_axis(&self, dt: NaiveDateTime) -> Result<f64> {
        let seconds_since_epoch = match self.calendar {
            Calendar::Gregorian => {
                let delta = dt.signed_duration_since(self.epoch);
                delta.num_seconds() as f64 + (delta.subsec_nanos() as f64) / 1e9
            }
            Calendar::NoLeap => noleap_seconds_since(&self.epoch, &dt)?,
        };
        Ok(self.unit.convert_from_seconds(seconds_since_epoch))
    }
}

/// Parse a CF units string: `"<unit> since <YYYY-MM-DD[ HH:MM:SS[.f][ TZ]]>"`.
fn parse_units(s: &str) -> Result<(Unit, NaiveDateTime)> {
    let (unit_part, date_part) = s
        .split_once(" since ")
        .ok_or_else(|| Error::Parse(format!("CF units '{s}' must be '<unit> since <date>'")))?;
    let unit = Unit::from_cf(unit_part.trim())?;
    let date_part = date_part.trim();

    // Peel off an optional timezone suffix before calendar parsing; we treat
    // everything as UTC for the purposes of axis arithmetic (CF assumes UTC
    // when no TZ is given).
    let date_part = date_part.trim_end_matches(" UTC");

    // chrono's standard formats.
    let candidates = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
    ];
    for fmt in candidates {
        if let Ok(dt) = NaiveDateTime::parse_from_str(date_part, fmt) {
            return Ok((unit, dt));
        }
        // Plain date: upgrade to midnight.
        if let Ok(d) = NaiveDate::parse_from_str(date_part, fmt) {
            return Ok((unit, d.and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap())));
        }
    }
    Err(Error::Parse(format!(
        "could not parse CF epoch '{date_part}' from units '{s}'"
    )))
}

/// Noleap calendar seconds between two datetimes. Treats every year as 365
/// days (no February 29). Only `dt >= epoch` is required to be correct; the
/// opposite direction is symmetric.
fn noleap_seconds_since(epoch: &NaiveDateTime, dt: &NaiveDateTime) -> Result<f64> {
    let epoch_doy = noleap_day_of_year(epoch.date())?;
    let dt_doy = noleap_day_of_year(dt.date())?;
    let year_days = 365i64 * (dt.year() as i64 - epoch.year() as i64);
    let day_delta = year_days + (dt_doy as i64 - epoch_doy as i64);
    let seconds_in_day = (dt.time() - epoch.time()).num_seconds();
    Ok((day_delta * 86_400 + seconds_in_day) as f64)
}

/// Day-of-year (1-indexed) treating Feb 29 as illegal.
fn noleap_day_of_year(d: NaiveDate) -> Result<u32> {
    if d.month() == 2 && d.day() == 29 {
        return Err(Error::InvalidArgument(
            "noleap calendar has no February 29 — pick Feb 28 or Mar 1".into(),
        ));
    }
    const DAYS: [u32; 12] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    Ok(DAYS[d.month0() as usize] + d.day())
}

#[cfg(test)]
mod tests {
    use super::*;

    const CMIP6_DAS_EXCERPT: &str = r#"
Attributes {
    lat {
        String axis "Y";
        String units "degrees_north";
    }
    time {
        String axis "T";
        String standard_name "time";
        String calendar "gregorian";
        String units "days since 1850-01-01 00:00:00";
    }
    tas {
        String units "K";
        Float32 _FillValue 1.0E20;
    }
}
"#;

    #[test]
    fn parses_units_with_hh_mm_ss_epoch() {
        let (u, dt) = parse_units("days since 1850-01-01 00:00:00").unwrap();
        assert_eq!(u, Unit::Days);
        assert_eq!(
            dt,
            NaiveDate::from_ymd_opt(1850, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
        );
    }

    #[test]
    fn parses_units_with_bare_date_epoch() {
        let (u, dt) = parse_units("hours since 2015-01-15").unwrap();
        assert_eq!(u, Unit::Hours);
        assert_eq!(dt.date(), NaiveDate::from_ymd_opt(2015, 1, 15).unwrap());
    }

    #[test]
    fn units_without_since_errors() {
        assert!(parse_units("days 1850-01-01").is_err());
    }

    #[test]
    fn unknown_unit_errors() {
        assert!(parse_units("fortnights since 1850-01-01").is_err());
    }

    #[test]
    fn cf_time_axis_gregorian_roundtrip() {
        let axis = CfTimeAxis::from_das(CMIP6_DAS_EXCERPT, "time").unwrap();
        assert_eq!(axis.calendar(), Calendar::Gregorian);

        // 2020-01-01 is 170 years and no fractional day after 1850-01-01.
        // Gregorian-correct count: 62091 days.
        let v = axis
            .date_to_axis(NaiveDate::from_ymd_opt(2020, 1, 1).unwrap())
            .unwrap();
        assert_eq!(v, 62091.0);
    }

    #[test]
    fn cf_time_axis_noleap_drops_leap_days() {
        let das = r#"
Attributes {
    time {
        String calendar "noleap";
        String units "days since 2000-01-01";
    }
}"#;
        let axis = CfTimeAxis::from_das(das, "time").unwrap();
        assert_eq!(axis.calendar(), Calendar::NoLeap);

        // 2001-01-01 on a noleap calendar is exactly 365 days after
        // 2000-01-01 (would be 366 on gregorian because 2000 is leap).
        let v = axis
            .date_to_axis(NaiveDate::from_ymd_opt(2001, 1, 1).unwrap())
            .unwrap();
        assert_eq!(v, 365.0);
    }

    #[test]
    fn noleap_calendar_rejects_feb_29() {
        let das = r#"
Attributes {
    time {
        String calendar "noleap";
        String units "days since 2000-01-01";
    }
}"#;
        let axis = CfTimeAxis::from_das(das, "time").unwrap();
        assert!(axis
            .date_to_axis(NaiveDate::from_ymd_opt(2004, 2, 29).unwrap())
            .is_err());
    }

    #[test]
    fn unsupported_calendar_errors() {
        let das = r#"
Attributes {
    time {
        String calendar "360_day";
        String units "days since 2000-01-01";
    }
}"#;
        let err = CfTimeAxis::from_das(das, "time").unwrap_err();
        assert!(err.to_string().contains("360_day"), "{err}");
    }

    #[test]
    fn missing_units_errors() {
        let das = r#"
Attributes {
    time {
        String calendar "gregorian";
    }
}"#;
        assert!(CfTimeAxis::from_das(das, "time").is_err());
    }
}
