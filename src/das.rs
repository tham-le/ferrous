//! Minimal parser for OPeNDAP DAS (Dataset Attribute Structure) responses.
//!
//! DAS is the companion of the DAP2 DDS that exposes CF-style metadata
//! attributes (`units`, `calendar`, `long_name`, `_FillValue`, …). Ferrous
//! reads it in two places:
//!
//! * [`crate::cf_time`] — extracts the time variable's `units` and
//!   `calendar` attributes to resolve `--time-iso` against.
//! * [`crate::nc_out`] — copies attributes across when writing a NetCDF
//!   file so downstream tools (xarray, PyFerret, ncdump) see `_FillValue`,
//!   `units`, and friends.
//!
//! The grammar handled here is the subset of the [DAP2 DAS format][das-spec]
//! that CMIP6 servers actually emit:
//!
//! ```text
//! Attributes {
//!     <var_name> {
//!         <Type> <attr_name> <value[, value ...]>;
//!         ...
//!     }
//!     NC_GLOBAL {
//!         ...
//!     }
//! }
//! ```
//!
//! Supported `<Type>` keywords: `String`, `Float32`, `Float64`, `Int32`,
//! `Int16`, `Byte`. Unsigned and unrecognised types (`UInt32 _ChunkSizes …`,
//! `Url`, …) are skipped — the attribute is dropped rather than poisoning
//! the whole parse.
//!
//! [das-spec]: https://docs.opendap.org/index.php?title=Documentation/Manuals/DAP-Protocol-Specification#Dataset_Attribute_Structure

use std::collections::HashMap;

/// Decoded value of one DAS attribute.
///
/// Integer-family types collapse into `F64` because NetCDF-3 receivers
/// usually store them as doubles anyway; this keeps the downstream
/// attribute-writing path single-type.
#[derive(Clone, Debug, PartialEq)]
pub enum DasValue {
    /// `String <name> "<value>";`
    Text(String),
    /// `Float32 <name> a, b, c;`
    F32(Vec<f32>),
    /// `Float64 <name> a, b, c;` (and `Int32` / `Int16` / `Byte`).
    F64(Vec<f64>),
}

/// Parsed DAS: `variable_name -> [(attr_name, value), ...]`.
///
/// The special key `"NC_GLOBAL"` holds global attributes. Each variable's
/// attribute list preserves declaration order from the DAS text.
pub type Attributes = HashMap<String, Vec<(String, DasValue)>>;

/// Parse a DAS response body.
///
/// Unknown attribute types are silently dropped (keeps parsing robust
/// against server-specific extensions); malformed lines log no errors —
/// DAS doesn't have a version field and spec deviations are common.
pub fn parse(das: &str) -> Attributes {
    let mut out: Attributes = HashMap::new();
    let mut iter = das.lines().peekable();
    // Skip up to the outer `Attributes {` opener.
    for line in iter.by_ref() {
        if line.trim().starts_with("Attributes") {
            break;
        }
    }
    // Now parse top-level variable blocks.
    while let Some(raw) = iter.next() {
        let line = raw.trim();
        if line.is_empty() || line == "}" {
            continue;
        }
        // Expect `<var_name> {`. Split on whitespace.
        let Some((name, rest)) = line.split_once(char::is_whitespace) else {
            continue;
        };
        if rest.trim() != "{" {
            continue;
        }
        let attrs = parse_block(&mut iter);
        if !attrs.is_empty() {
            out.insert(name.to_string(), attrs);
        }
    }
    out
}

/// Parse the attribute list inside a variable block, consuming lines up to
/// and including the matching `}`.
fn parse_block<'a, I>(iter: &mut std::iter::Peekable<I>) -> Vec<(String, DasValue)>
where
    I: Iterator<Item = &'a str>,
{
    let mut attrs = Vec::new();
    for raw in iter.by_ref() {
        let line = raw.trim().trim_end_matches(';');
        if line == "}" {
            break;
        }
        if line.is_empty() {
            continue;
        }
        if let Some((name, value)) = parse_attr_line(line) {
            attrs.push((name, value));
        }
    }
    attrs
}

/// Parse one line: `<Type> <name> <value...>`.
fn parse_attr_line(line: &str) -> Option<(String, DasValue)> {
    let line = line.trim();
    // Split into (type, rest).
    let (ty, rest) = line.split_once(char::is_whitespace)?;
    let rest = rest.trim();
    // Split (name, values).
    let (name, values_raw) = rest.split_once(char::is_whitespace)?;
    let name = name.trim();
    let values_raw = values_raw.trim();

    match ty {
        "String" => parse_string_value(values_raw).map(|s| (name.into(), DasValue::Text(s))),
        "Float32" => parse_float_list::<f32>(values_raw).map(|v| (name.into(), DasValue::F32(v))),
        "Float64" => parse_float_list::<f64>(values_raw).map(|v| (name.into(), DasValue::F64(v))),
        "Int32" | "Int16" | "Byte" => {
            // Treat as f64 so the writer doesn't need NC_INT / NC_SHORT /
            // NC_BYTE support for attributes.
            parse_float_list::<f64>(values_raw).map(|v| (name.into(), DasValue::F64(v)))
        }
        // UInt32, Url, etc. — skipped.
        _ => None,
    }
}

/// Strip the surrounding quotes from a String attribute value. Returns the
/// string contents untouched by escape-handling — CMIP6 DAS values don't
/// use embedded escapes in practice.
fn parse_string_value(s: &str) -> Option<String> {
    let s = s.trim();
    let s = s.strip_prefix('"')?;
    let s = s.strip_suffix('"')?;
    Some(s.to_string())
}

/// Parse a comma-separated list of numbers. Empty or malformed input
/// returns `None` so the caller can skip the attribute.
fn parse_float_list<T: std::str::FromStr>(s: &str) -> Option<Vec<T>> {
    let mut out = Vec::new();
    for part in s.split(',') {
        let v = part.trim().parse::<T>().ok()?;
        out.push(v);
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CMIP6_DAS: &str = r#"
Attributes {
    lat {
        String axis "Y";
        String units "degrees_north";
    }
    time {
        String calendar "gregorian";
        String units "days since 1850-01-01 00:00:00";
    }
    tas {
        Float32 _FillValue 1.0E20;
        Float32 missing_value 1.0E20;
        String units "K";
        String standard_name "air_temperature";
        UInt32 _ChunkSizes 1, 128, 256;
    }
    NC_GLOBAL {
        String Conventions "CF-1.7 CMIP-6.2";
        Int32 forcing_index 2;
    }
}
"#;

    #[test]
    fn parses_string_attrs_on_variable() {
        let attrs = parse(CMIP6_DAS);
        let tas = attrs.get("tas").expect("tas block present");
        let units = tas
            .iter()
            .find(|(n, _)| n == "units")
            .map(|(_, v)| v)
            .unwrap();
        assert_eq!(units, &DasValue::Text("K".into()));
    }

    #[test]
    fn parses_float32_fill_value() {
        let attrs = parse(CMIP6_DAS);
        let tas = attrs.get("tas").unwrap();
        let fill = tas
            .iter()
            .find(|(n, _)| n == "_FillValue")
            .map(|(_, v)| v)
            .unwrap();
        match fill {
            DasValue::F32(v) => assert_eq!(v, &[1.0e20_f32]),
            other => panic!("expected F32, got {other:?}"),
        }
    }

    #[test]
    fn preserves_attribute_order_per_variable() {
        let attrs = parse(CMIP6_DAS);
        let tas = attrs.get("tas").unwrap();
        let names: Vec<&str> = tas.iter().map(|(n, _)| n.as_str()).collect();
        // _ChunkSizes is skipped (UInt32 unsupported), so we expect four
        // entries in order: _FillValue, missing_value, units, standard_name.
        assert_eq!(
            names,
            vec!["_FillValue", "missing_value", "units", "standard_name"]
        );
    }

    #[test]
    fn captures_global_attributes() {
        let attrs = parse(CMIP6_DAS);
        let global = attrs.get("NC_GLOBAL").expect("NC_GLOBAL block present");
        let conv = global.iter().find(|(n, _)| n == "Conventions").unwrap();
        assert_eq!(conv.1, DasValue::Text("CF-1.7 CMIP-6.2".into()));

        let forcing = global
            .iter()
            .find(|(n, _)| n == "forcing_index")
            .map(|(_, v)| v)
            .unwrap();
        // Int32 gets coerced into F64 for writer simplicity.
        match forcing {
            DasValue::F64(v) => assert_eq!(v, &[2.0]),
            other => panic!("expected F64, got {other:?}"),
        }
    }

    #[test]
    fn ignores_unsupported_types_gracefully() {
        let attrs = parse(CMIP6_DAS);
        let tas = attrs.get("tas").unwrap();
        // _ChunkSizes was UInt32 — must not appear.
        assert!(!tas.iter().any(|(n, _)| n == "_ChunkSizes"));
    }

    #[test]
    fn empty_input_parses_to_empty_map() {
        let attrs = parse("");
        assert!(attrs.is_empty());
    }

    #[test]
    fn rejects_malformed_string_without_quotes() {
        let malformed = r#"
Attributes {
    x {
        String name nope_no_quotes;
    }
}
"#;
        let attrs = parse(malformed);
        // The `x` block has no valid attributes; it should be dropped.
        assert!(!attrs.contains_key("x"));
    }

    #[test]
    fn multi_value_float_list() {
        let das = r#"
Attributes {
    v {
        Float32 valid_range -1.5, 3.25, 999.0;
    }
}
"#;
        let attrs = parse(das);
        let v = attrs.get("v").unwrap();
        match &v[0].1 {
            DasValue::F32(values) => assert_eq!(values, &[-1.5_f32, 3.25, 999.0]),
            other => panic!("expected F32 list, got {other:?}"),
        }
    }
}
