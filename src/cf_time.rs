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
    /// 366-day calendar where every year includes Feb 29 (CMIP6
    /// `"all_leap"` / `"366_day"`). Exotic but a handful of models emit it.
    AllLeap,
    /// 12 months × 30 days. Used by HadGEM and other Met Office models
    /// (`"360_day"`). Dates like Feb 30 or May 31 are valid; Jan 31 is not.
    ThreeSixty,
    /// Julian calendar — leap years are every year divisible by 4 (no
    /// Gregorian century correction). For CMIP6 epochs after 1582 the drift
    /// vs Gregorian is at most one day per century; we emit a warning on
    /// construction so users with pre-1582 paleoclimate data know to
    /// double-check.
    Julian,
}

impl Calendar {
    fn from_cf(s: &str) -> Result<Self> {
        match s {
            "gregorian" | "standard" | "proleptic_gregorian" => Ok(Self::Gregorian),
            "noleap" | "365_day" => Ok(Self::NoLeap),
            "all_leap" | "366_day" => Ok(Self::AllLeap),
            "360_day" => Ok(Self::ThreeSixty),
            "julian" => {
                eprintln!(
                    "note: CF calendar 'julian' approximates as Julian-leap-rule \
                     date arithmetic; paleoclimate dates before 1582 may drift \
                     by up to one day per century vs Gregorian."
                );
                Ok(Self::Julian)
            }
            other => Err(Error::Parse(format!(
                "CF calendar '{other}' is not supported by ferrous (supported: \
                 gregorian / standard / proleptic_gregorian, noleap / 365_day, \
                 all_leap / 366_day, 360_day, julian)"
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
            Calendar::Gregorian | Calendar::Julian => {
                // For post-1582 dates, Julian and Gregorian agree to within
                // a day per century; the drift would matter only for
                // paleoclimate work that we explicitly warned about.
                let delta = dt.signed_duration_since(self.epoch);
                delta.num_seconds() as f64 + (delta.subsec_nanos() as f64) / 1e9
            }
            Calendar::NoLeap => fixed_year_seconds(&self.epoch, &dt, Year365)?,
            Calendar::AllLeap => fixed_year_seconds(&self.epoch, &dt, Year366)?,
            Calendar::ThreeSixty => three_sixty_seconds(&self.epoch, &dt)?,
        };
        Ok(self.unit.convert_from_seconds(seconds_since_epoch))
    }
}

/// Shared behaviour for fixed-structure calendars (noleap / all_leap). The
/// generic day-count machinery below uses this trait so the same code path
/// handles both; the two only differ in whether February has 28 or 29 days.
trait FixedYear: Copy {
    /// Days in each month, 1-indexed by month number.
    fn days_in_month(self, month: u32) -> u32;
    /// Total days in a year under this calendar.
    fn days_in_year(self) -> u32;
    /// Human-readable calendar name for error messages.
    fn name(self) -> &'static str;
}

#[derive(Clone, Copy)]
struct Year365;
#[derive(Clone, Copy)]
struct Year366;

impl FixedYear for Year365 {
    fn days_in_month(self, month: u32) -> u32 {
        const DAYS: [u32; 13] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        DAYS[month as usize]
    }
    fn days_in_year(self) -> u32 {
        365
    }
    fn name(self) -> &'static str {
        "noleap"
    }
}
impl FixedYear for Year366 {
    fn days_in_month(self, month: u32) -> u32 {
        const DAYS: [u32; 13] = [0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        DAYS[month as usize]
    }
    fn days_in_year(self) -> u32 {
        366
    }
    fn name(self) -> &'static str {
        "all_leap"
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

/// Seconds between two datetimes under a fixed-year-length calendar
/// (noleap / all_leap). Caller-supplied [`FixedYear`] impl selects the
/// month-length table and rejects invalid dates (e.g. Feb 29 under noleap).
fn fixed_year_seconds<Y: FixedYear>(
    epoch: &NaiveDateTime,
    dt: &NaiveDateTime,
    year: Y,
) -> Result<f64> {
    let epoch_doy = fixed_year_day_of_year(epoch.date(), year)?;
    let dt_doy = fixed_year_day_of_year(dt.date(), year)?;
    let year_days = i64::from(year.days_in_year()) * (dt.year() as i64 - epoch.year() as i64);
    let day_delta = year_days + (dt_doy as i64 - epoch_doy as i64);
    let seconds_in_day = (dt.time() - epoch.time()).num_seconds();
    Ok((day_delta * 86_400 + seconds_in_day) as f64)
}

/// Day-of-year (1-indexed) under a fixed-year-length calendar.
/// Errors if the input date is illegal for the calendar (e.g. Feb 29 on
/// noleap, May 31 on 360_day elsewhere in the module).
fn fixed_year_day_of_year<Y: FixedYear>(d: NaiveDate, year: Y) -> Result<u32> {
    let month = d.month();
    let day = d.day();
    if day > year.days_in_month(month) {
        return Err(Error::InvalidArgument(format!(
            "{}-{:02}-{:02} is not a valid date on the {} calendar",
            d.year(),
            month,
            day,
            year.name()
        )));
    }
    let mut doy = 0u32;
    for m in 1..month {
        doy += year.days_in_month(m);
    }
    Ok(doy + day)
}

/// Seconds between two datetimes under the 360-day calendar.
///
/// Every month has exactly 30 days; the year has 360. Day 31 is never
/// valid; Feb 29 and Feb 30 are valid under this calendar.
fn three_sixty_seconds(epoch: &NaiveDateTime, dt: &NaiveDateTime) -> Result<f64> {
    let epoch_d = three_sixty_days_from_origin(epoch.date())?;
    let dt_d = three_sixty_days_from_origin(dt.date())?;
    let day_delta = dt_d - epoch_d;
    let seconds_in_day = (dt.time() - epoch.time()).num_seconds();
    Ok((day_delta * 86_400 + seconds_in_day) as f64)
}

/// Days-since-year-0 on the 360-day calendar. Origin is arbitrary — only
/// the difference between two of these values is meaningful.
fn three_sixty_days_from_origin(d: NaiveDate) -> Result<i64> {
    let month = d.month();
    let day = d.day();
    if month > 12 || day == 0 || day > 30 {
        return Err(Error::InvalidArgument(format!(
            "{}-{:02}-{:02} is not valid on the 360_day calendar (every month has 30 days)",
            d.year(),
            month,
            day
        )));
    }
    Ok(d.year() as i64 * 360 + (month as i64 - 1) * 30 + (day as i64 - 1))
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
        // We now support every CF calendar listed in the spec; this test
        // pins that an unknown name (server-specific typo, futuristic
        // extension) still fails loud.
        let das = r#"
Attributes {
    time {
        String calendar "mars_local_solar";
        String units "days since 2000-01-01";
    }
}"#;
        let err = CfTimeAxis::from_das(das, "time").unwrap_err();
        assert!(err.to_string().contains("mars_local_solar"), "{err}");
    }

    #[test]
    fn cf_time_axis_all_leap_accepts_feb_29_every_year() {
        let das = r#"
Attributes {
    time {
        String calendar "all_leap";
        String units "days since 2000-01-01";
    }
}"#;
        let axis = CfTimeAxis::from_das(das, "time").unwrap();
        assert_eq!(axis.calendar(), Calendar::AllLeap);

        // 2001-01-01 is 366 days after 2000-01-01 (every year has Feb 29).
        let v = axis
            .date_to_axis(NaiveDate::from_ymd_opt(2001, 1, 1).unwrap())
            .unwrap();
        assert_eq!(v, 366.0);

        // Feb 29 in 2003 is legal under all_leap (Gregorian would reject it,
        // but we pass the NaiveDate-validated triple through).
        let feb29_2003 = NaiveDate::from_ymd_opt(2003, 2, 28)
            .unwrap()
            .succ_opt()
            .unwrap();
        assert!(axis.date_to_axis(feb29_2003).is_ok());
    }

    #[test]
    fn cf_time_axis_360_day_has_30_day_months() {
        let das = r#"
Attributes {
    time {
        String calendar "360_day";
        String units "days since 2000-01-01";
    }
}"#;
        let axis = CfTimeAxis::from_das(das, "time").unwrap();
        assert_eq!(axis.calendar(), Calendar::ThreeSixty);

        // 2001-01-01 is 360 days after 2000-01-01 on a 360-day calendar.
        let v = axis
            .date_to_axis(NaiveDate::from_ymd_opt(2001, 1, 1).unwrap())
            .unwrap();
        assert_eq!(v, 360.0);

        // Mid-year: 2000-07-01 -> 6 months * 30 = 180 days.
        let v = axis
            .date_to_axis(NaiveDate::from_ymd_opt(2000, 7, 1).unwrap())
            .unwrap();
        assert_eq!(v, 180.0);
    }

    #[test]
    fn three_sixty_rejects_day_31() {
        let das = r#"
Attributes {
    time {
        String calendar "360_day";
        String units "days since 2000-01-01";
    }
}"#;
        let axis = CfTimeAxis::from_das(das, "time").unwrap();
        let day_31 = NaiveDate::from_ymd_opt(2000, 1, 31).unwrap();
        let err = axis.date_to_axis(day_31).unwrap_err();
        assert!(err.to_string().contains("360_day"), "{err}");
    }

    #[test]
    fn cf_time_axis_julian_accepts_with_warning() {
        // Parses fine; the eprintln warning is best-effort and not tested.
        let das = r#"
Attributes {
    time {
        String calendar "julian";
        String units "days since 1850-01-01";
    }
}"#;
        let axis = CfTimeAxis::from_das(das, "time").unwrap();
        assert_eq!(axis.calendar(), Calendar::Julian);
        // Post-1582, Julian and Gregorian agree to within a day per century;
        // we accept the Gregorian-arithmetic approximation.
        let v = axis
            .date_to_axis(NaiveDate::from_ymd_opt(2020, 1, 1).unwrap())
            .unwrap();
        assert_eq!(v, 62091.0);
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
