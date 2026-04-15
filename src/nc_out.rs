//! Minimal NetCDF-3 classic-format writer for [`crate::dap2::DapResponse`].
//!
//! Writes a `.nc` file that xarray, PyFerret, MATLAB, R, and every other
//! classic NetCDF consumer can open directly. Scope is deliberately narrow:
//!
//! * Fixed dimensions only (no record / unlimited dimension).
//! * Float32 / Float64 / Int32 / Int16 variables — the types [`crate::dap2`]
//!   actually decodes.
//! * No attributes in this version — adding global + per-variable attributes
//!   (units, calendar, `_FillValue`, …) is a follow-up once the DAS parser is
//!   exposed to the writer.
//!
//! The format spec we implement is the "NetCDF classic" subset of the
//! [Unidata file-format document][spec]. It is fully documented at that link;
//! we follow it tag-for-tag.
//!
//! [spec]: https://docs.unidata.ucar.edu/netcdf-c/current/file_format_specifications.html#classic_format_spec

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use crate::dap2::{DapData, DapResponse, DapType, DapVariable};
use crate::{Error, Result};

// Tag constants from the NetCDF-3 classic spec.
const MAGIC: [u8; 4] = *b"CDF\x01"; // classic (32-bit offsets)
const ABSENT: [u8; 8] = [0; 8]; // ZERO ZERO — empty list
const NC_DIMENSION: u32 = 0x0A;
const NC_VARIABLE: u32 = 0x0B;
const NC_ATTRIBUTE: u32 = 0x0C;

// Type codes.
const NC_BYTE: u32 = 1;
const NC_CHAR: u32 = 2;
const NC_SHORT: u32 = 3;
const NC_INT: u32 = 4;
const NC_FLOAT: u32 = 5;
const NC_DOUBLE: u32 = 6;

/// Value of a NetCDF-3 attribute.
///
/// NetCDF-3 also supports `NC_BYTE` / `NC_SHORT` / `NC_INT` arrays, but those
/// are rare as attribute types in CMIP6 DAS output; `Text` (from `String`
/// DAS attributes), `F32` (`_FillValue`, `missing_value`, `scale_factor`, …)
/// and `F64` (some coord bounds) cover the practical need.
#[derive(Clone, Debug, PartialEq)]
pub enum AttrValue {
    /// UTF-8 string, stored as NC_CHAR.
    Text(String),
    /// One or more `f32` values.
    F32(Vec<f32>),
    /// One or more `f64` values.
    F64(Vec<f64>),
}

impl AttrValue {
    fn nc_type(&self) -> u32 {
        match self {
            Self::Text(_) => NC_CHAR,
            Self::F32(_) => NC_FLOAT,
            Self::F64(_) => NC_DOUBLE,
        }
    }

    fn nelems(&self) -> u32 {
        let n = match self {
            Self::Text(s) => s.len(),
            Self::F32(v) => v.len(),
            Self::F64(v) => v.len(),
        };
        n as u32
    }

    /// On-disk byte size of the values block (before padding to 4).
    fn raw_bytes(&self) -> usize {
        match self {
            Self::Text(s) => s.len(),
            Self::F32(v) => v.len() * 4,
            Self::F64(v) => v.len() * 8,
        }
    }

    /// Total on-disk size including 4-byte padding.
    fn padded_bytes(&self) -> usize {
        padded4(self.raw_bytes())
    }

    fn write_values(&self, out: &mut Vec<u8>) {
        match self {
            Self::Text(s) => out.extend_from_slice(s.as_bytes()),
            Self::F32(v) => {
                for x in v {
                    out.extend_from_slice(&x.to_be_bytes());
                }
            }
            Self::F64(v) => {
                for x in v {
                    out.extend_from_slice(&x.to_be_bytes());
                }
            }
        }
        pad_to_4(out);
    }
}

/// Collection of attributes to emit: global (`NC_GLOBAL`) plus per-variable.
///
/// Declaration order of both global and per-variable attrs is preserved in
/// the written file, which matters because some downstream tools display
/// attributes in the order they appear.
#[derive(Clone, Debug, Default)]
pub struct Attrs {
    /// `NC_GLOBAL` attributes in order.
    pub global: Vec<(String, AttrValue)>,
    /// Per-variable attributes keyed by variable name; insertion order within
    /// each variable is preserved.
    pub per_var: HashMap<String, Vec<(String, AttrValue)>>,
}

impl Attrs {
    /// Handy empty constructor for callers that don't have DAS data.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Attributes declared for `var_name`, in insertion order.
    fn for_var(&self, var_name: &str) -> &[(String, AttrValue)] {
        self.per_var.get(var_name).map(Vec::as_slice).unwrap_or(&[])
    }
}

/// Write `response` as a NetCDF-3 classic file at `path`, no attributes.
pub fn write(path: &Path, response: &DapResponse) -> Result<()> {
    write_with_attrs(path, response, &Attrs::empty())
}

/// Write `response` with the supplied global + per-variable attributes.
///
/// Attribute names that don't correspond to any declared variable are
/// silently ignored — this lets callers pass in attribute maps extracted
/// from a DAS that has more variables than were projected.
pub fn write_with_attrs(path: &Path, response: &DapResponse, attrs: &Attrs) -> Result<()> {
    let plan = Plan::from_response(response, attrs)?;
    let mut out = Vec::with_capacity(plan.total_size);
    plan.write_header(&mut out)?;
    debug_assert_eq!(out.len(), plan.header_size, "header size prediction");
    for var in &response.variables {
        write_variable_data(&mut out, var)?;
    }
    debug_assert_eq!(out.len(), plan.total_size, "total size prediction");
    fs::write(path, &out)?;
    Ok(())
}

/// Pre-computed file layout — dimensions, attributes, per-variable offsets,
/// and the final file size. Split out of `write` so header assembly and
/// data emission can be ordered independently.
struct Plan<'a> {
    dims: Vec<Dim>,
    globals: &'a [(String, AttrValue)],
    vars: Vec<VarPlan<'a>>,
    header_size: usize,
    total_size: usize,
}

struct Dim {
    name: String,
    size: u32,
}

struct VarPlan<'a> {
    var: &'a DapVariable,
    /// Index in the dim list for every dimension this variable uses.
    dim_ids: Vec<u32>,
    /// Attributes to emit for this variable, in order.
    attrs: &'a [(String, AttrValue)],
    /// Offset (from file start) of the variable's data block. Part of the
    /// header and referenced as `begin` per the spec.
    begin: u32,
    /// Padded size in bytes of the variable's data block.
    vsize: u32,
}

impl<'a> Plan<'a> {
    fn from_response(response: &'a DapResponse, attrs: &'a Attrs) -> Result<Self> {
        // 1. Collect unique (name, size) dimensions in first-seen order.
        let mut dims: Vec<Dim> = Vec::new();
        let mut dim_index: HashMap<String, usize> = HashMap::new();
        for var in &response.variables {
            for d in &var.dimensions {
                match dim_index.get(&d.name) {
                    Some(&i) => {
                        if dims[i].size as usize != d.size {
                            return Err(Error::Parse(format!(
                                "dimension '{}' has inconsistent sizes across variables \
                                 ({} vs {})",
                                d.name, dims[i].size, d.size
                            )));
                        }
                    }
                    None => {
                        if d.size > u32::MAX as usize {
                            return Err(Error::Parse(format!(
                                "dimension '{}' size {} exceeds NetCDF-3 classic u32 limit",
                                d.name, d.size
                            )));
                        }
                        dim_index.insert(d.name.clone(), dims.len());
                        dims.push(Dim {
                            name: d.name.clone(),
                            size: d.size as u32,
                        });
                    }
                }
            }
        }

        // 2. Reject types we can't encode (none of ours today, but keep the
        //    check so adding a new DapType surfaces here loud).
        for var in &response.variables {
            nc_type_code(var.dtype)?;
        }

        // 3. Compute header size (now including attributes), then assign
        //    each variable's begin + vsize.
        let header_size = header_size(&dims, &attrs.global, &response.variables, attrs);
        let mut vars: Vec<VarPlan<'a>> = Vec::with_capacity(response.variables.len());
        let mut cursor = header_size;
        for var in &response.variables {
            let dim_ids: Vec<u32> = var
                .dimensions
                .iter()
                .map(|d| dim_index[&d.name] as u32)
                .collect();
            let nbytes = data_bytes(var);
            let padded = padded4(nbytes);
            if padded > u32::MAX as usize {
                return Err(Error::Parse(format!(
                    "variable '{}' data size {} exceeds NetCDF-3 classic u32 limit",
                    var.name, padded
                )));
            }
            if cursor > u32::MAX as usize {
                return Err(Error::Parse(
                    "file size would exceed NetCDF-3 classic 32-bit offset limit (~4 GiB); \
                     switch to netcdf4 or shrink your request"
                        .into(),
                ));
            }
            vars.push(VarPlan {
                var,
                dim_ids,
                attrs: attrs.for_var(&var.name),
                begin: cursor as u32,
                vsize: padded as u32,
            });
            cursor += padded;
        }

        Ok(Self {
            dims,
            globals: attrs.global.as_slice(),
            vars,
            header_size,
            total_size: cursor,
        })
    }

    fn write_header(&self, out: &mut Vec<u8>) -> Result<()> {
        // magic
        out.extend_from_slice(&MAGIC);
        // numrecs — zero, no record dim.
        write_u32(out, 0);
        // dim_list
        if self.dims.is_empty() {
            out.extend_from_slice(&ABSENT);
        } else {
            write_u32(out, NC_DIMENSION);
            write_u32(out, self.dims.len() as u32);
            for d in &self.dims {
                write_name(out, &d.name);
                write_u32(out, d.size);
            }
        }
        // gatt_list
        write_attr_list(out, self.globals);
        // var_list
        if self.vars.is_empty() {
            out.extend_from_slice(&ABSENT);
        } else {
            write_u32(out, NC_VARIABLE);
            write_u32(out, self.vars.len() as u32);
            for vp in &self.vars {
                write_name(out, &vp.var.name);
                write_u32(out, vp.dim_ids.len() as u32);
                for id in &vp.dim_ids {
                    write_u32(out, *id);
                }
                // vatt_list
                write_attr_list(out, vp.attrs);
                // nc_type + vsize + begin
                write_u32(out, nc_type_code(vp.var.dtype)?);
                write_u32(out, vp.vsize);
                write_u32(out, vp.begin);
            }
        }
        Ok(())
    }
}

/// Emit an attribute list (either gatt_list or vatt_list). Empty lists are
/// written as ABSENT per spec.
fn write_attr_list(out: &mut Vec<u8>, attrs: &[(String, AttrValue)]) {
    if attrs.is_empty() {
        out.extend_from_slice(&ABSENT);
        return;
    }
    write_u32(out, NC_ATTRIBUTE);
    write_u32(out, attrs.len() as u32);
    for (name, value) in attrs {
        write_name(out, name);
        write_u32(out, value.nc_type());
        write_u32(out, value.nelems());
        value.write_values(out);
    }
}

/// Size in bytes of one attribute on disk.
fn attr_bytes(name: &str, value: &AttrValue) -> usize {
    name_bytes(name) + 4 /* type */ + 4 /* nelems */ + value.padded_bytes()
}

/// Size of an attribute list: either 8 (ABSENT) or tag+nelems+sum(attr).
fn attr_list_bytes(attrs: &[(String, AttrValue)]) -> usize {
    if attrs.is_empty() {
        return 8;
    }
    let mut n = 4 /* tag */ + 4 /* nelems */;
    for (name, value) in attrs {
        n += attr_bytes(name, value);
    }
    n
}

fn nc_type_code(t: DapType) -> Result<u32> {
    match t {
        DapType::Float32 => Ok(NC_FLOAT),
        DapType::Float64 => Ok(NC_DOUBLE),
        DapType::Int32 => Ok(NC_INT),
        DapType::Int16 => Ok(NC_SHORT),
        DapType::Byte => Ok(NC_BYTE),
    }
}

/// Header size in bytes. Does include attributes on both the global and
/// per-variable slots; does NOT include the data section.
fn header_size(
    dims: &[Dim],
    globals: &[(String, AttrValue)],
    vars: &[DapVariable],
    attrs: &Attrs,
) -> usize {
    let mut n = 4 /* magic */ + 4 /* numrecs */;
    // dim_list
    if dims.is_empty() {
        n += 8;
    } else {
        n += 4 /* tag */ + 4 /* nelems */;
        for d in dims {
            n += name_bytes(&d.name) + 4 /* size */;
        }
    }
    // gatt_list
    n += attr_list_bytes(globals);
    // var_list
    if vars.is_empty() {
        n += 8;
    } else {
        n += 4 /* tag */ + 4 /* nelems */;
        for v in vars {
            n += name_bytes(&v.name);
            n += 4 /* ndims */ + 4 * v.dimensions.len() /* dimids */;
            n += attr_list_bytes(attrs.for_var(&v.name));
            n += 4 /* type */ + 4 /* vsize */ + 4 /* begin */;
        }
    }
    n
}

fn data_bytes(var: &DapVariable) -> usize {
    var.element_count() * var.dtype.size()
}

fn padded4(n: usize) -> usize {
    n.div_ceil(4) * 4
}

/// Size on disk of a NetCDF name: `u32 length + UTF-8 bytes + padding to 4`.
fn name_bytes(s: &str) -> usize {
    4 + padded4(s.len())
}

fn write_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_be_bytes());
}

fn write_name(out: &mut Vec<u8>, s: &str) {
    write_u32(out, s.len() as u32);
    out.extend_from_slice(s.as_bytes());
    pad_to_4(out);
}

fn pad_to_4(out: &mut Vec<u8>) {
    while out.len() % 4 != 0 {
        out.push(0);
    }
}

fn write_variable_data(out: &mut Vec<u8>, var: &DapVariable) -> Result<()> {
    match &var.data {
        DapData::F32(v) => {
            for x in v {
                out.extend_from_slice(&x.to_be_bytes());
            }
        }
        DapData::F64(v) => {
            for x in v {
                out.extend_from_slice(&x.to_be_bytes());
            }
        }
        DapData::I32(v) => {
            for x in v {
                out.extend_from_slice(&x.to_be_bytes());
            }
        }
        DapData::I16(v) => {
            for x in v {
                out.extend_from_slice(&x.to_be_bytes());
            }
        }
        DapData::U8(v) => {
            out.extend_from_slice(v);
        }
    }
    pad_to_4(out);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dap2::{DapDimension, DapVariable};

    fn f32_var(name: &str, dims: Vec<(&str, usize)>, values: Vec<f32>) -> DapVariable {
        DapVariable {
            name: name.to_string(),
            dtype: DapType::Float32,
            dimensions: dims
                .into_iter()
                .map(|(n, s)| DapDimension {
                    name: n.into(),
                    size: s,
                })
                .collect(),
            data: DapData::F32(values),
        }
    }

    fn f64_var(name: &str, dims: Vec<(&str, usize)>, values: Vec<f64>) -> DapVariable {
        DapVariable {
            name: name.to_string(),
            dtype: DapType::Float64,
            dimensions: dims
                .into_iter()
                .map(|(n, s)| DapDimension {
                    name: n.into(),
                    size: s,
                })
                .collect(),
            data: DapData::F64(values),
        }
    }

    fn write_to_tmp(response: &DapResponse) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "ferrous-nc-test-{}-{}.nc",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        write(&p, response).expect("write ok");
        p
    }

    #[test]
    fn writes_magic_and_numrecs() {
        let response = DapResponse {
            dds: "Dataset { Float32 x[d = 2]; } t;".into(),
            variables: vec![f32_var("x", vec![("d", 2)], vec![1.0, 2.0])],
        };
        let path = write_to_tmp(&response);
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(&bytes[..4], b"CDF\x01", "classic magic");
        assert_eq!(
            u32::from_be_bytes(bytes[4..8].try_into().unwrap()),
            0,
            "numrecs = 0 (no record dim)"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn single_variable_file_layout() {
        // Header byte budget for a 1-dim, 1-var, zero-attr file:
        //   magic(4) numrecs(4)                                       =  8
        //   NC_DIMENSION(4) nelems=1(4) name("d" -> 4+4=8) size(4)    = 20
        //   gatt_list ABSENT                                          =  8
        //   NC_VARIABLE(4) nelems=1(4)
        //     name("x" -> 8) ndims(4) dimid(4)                         = 16
        //     vatt_list ABSENT                                         =  8
        //     type(4) vsize(4) begin(4)                                = 12
        //                                                              ----
        //                                                    total var entry = 36
        //   var_list section                                 = 8 + 36 = 44
        //   header total                                     = 8 + 20 + 8 + 44 = 80
        // data: 2 floats * 4 = 8 (already 4-byte aligned)
        let response = DapResponse {
            dds: "…".into(),
            variables: vec![f32_var("x", vec![("d", 2)], vec![1.0, 2.0])],
        };
        let path = write_to_tmp(&response);
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(bytes.len(), 80 + 8);
        // Data should decode as two big-endian f32s at offset 80.
        let v0 = f32::from_be_bytes(bytes[80..84].try_into().unwrap());
        let v1 = f32::from_be_bytes(bytes[84..88].try_into().unwrap());
        assert_eq!(v0, 1.0);
        assert_eq!(v1, 2.0);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn shared_dimensions_are_deduplicated() {
        // Two variables both use `time[time=3]`; NetCDF-3 must declare only
        // one `time` dimension and have both vars reference its dimid.
        let time_vals = f64_var("time", vec![("time", 3)], vec![0.0, 1.0, 2.0]);
        let tas = f32_var(
            "tas",
            vec![("time", 3), ("lat", 2)],
            vec![290.0, 291.0, 292.0, 293.0, 294.0, 295.0],
        );
        let lat = f64_var("lat", vec![("lat", 2)], vec![10.0, 20.0]);
        let response = DapResponse {
            dds: "…".into(),
            variables: vec![tas, time_vals, lat],
        };
        let path = write_to_tmp(&response);
        let empty = Attrs::empty();
        let plan = Plan::from_response(&response, &empty).unwrap();
        assert_eq!(plan.dims.len(), 2, "time + lat, deduped");
        assert_eq!(plan.dims[0].name, "time");
        assert_eq!(plan.dims[1].name, "lat");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn rejects_inconsistent_dimension_sizes() {
        // Same dim name with different sizes would corrupt the file silently;
        // the writer must refuse.
        let a = f32_var("a", vec![("d", 3)], vec![1.0, 2.0, 3.0]);
        let b = f32_var("b", vec![("d", 4)], vec![1.0, 2.0, 3.0, 4.0]);
        let response = DapResponse {
            dds: "…".into(),
            variables: vec![a, b],
        };
        assert!(Plan::from_response(&response, &Attrs::empty()).is_err());
    }

    #[test]
    fn name_padding_rounds_to_four() {
        // 3-char name "abc" + u32 length = 4 + 3 = 7 bytes; pad to 8.
        assert_eq!(name_bytes("abc"), 8);
        // 4-char name → 4 + 4 = 8, no pad needed.
        assert_eq!(name_bytes("abcd"), 8);
        // 5-char name → 4 + 5 = 9, pad to 12.
        assert_eq!(name_bytes("abcde"), 12);
        // Empty name (pathological) → 4 + 0 = 4.
        assert_eq!(name_bytes(""), 4);
    }

    #[test]
    fn padded4_rounds_up() {
        assert_eq!(padded4(0), 0);
        assert_eq!(padded4(1), 4);
        assert_eq!(padded4(4), 4);
        assert_eq!(padded4(5), 8);
        assert_eq!(padded4(15), 16);
    }

    #[test]
    fn var_begin_offsets_are_monotonic() {
        // Sanity: once the header is pinned, each var's begin should be
        // strictly increasing and land exactly after the previous var's
        // padded data.
        let response = DapResponse {
            dds: "…".into(),
            variables: vec![
                f32_var("a", vec![("d", 3)], vec![1.0, 2.0, 3.0]),
                f64_var("b", vec![("d", 3)], vec![10.0, 20.0, 30.0]),
                f32_var("c", vec![("d", 3)], vec![100.0, 200.0, 300.0]),
            ],
        };
        let empty = Attrs::empty();
        let plan = Plan::from_response(&response, &empty).unwrap();
        let begins: Vec<u32> = plan.vars.iter().map(|v| v.begin).collect();
        assert!(begins[0] < begins[1] && begins[1] < begins[2]);
        assert_eq!(begins[1] - begins[0], plan.vars[0].vsize);
        assert_eq!(begins[2] - begins[1], plan.vars[1].vsize);
    }

    #[test]
    fn attrs_round_trip_global_text() {
        let response = DapResponse {
            dds: "…".into(),
            variables: vec![f32_var("x", vec![("d", 1)], vec![1.0])],
        };
        let mut attrs = Attrs::empty();
        attrs
            .global
            .push(("Conventions".into(), AttrValue::Text("CF-1.7".into())));
        let path = write_to_tmp_with_attrs(&response, &attrs);
        let bytes = std::fs::read(&path).unwrap();

        // Header should contain the tag + literal string somewhere.
        assert!(
            bytes.windows(6).any(|w| w == b"CF-1.7"),
            "global Conventions text should appear verbatim in the header"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn attrs_round_trip_var_fill_value_f32() {
        let response = DapResponse {
            dds: "…".into(),
            variables: vec![f32_var("tas", vec![("d", 1)], vec![290.0])],
        };
        let mut attrs = Attrs::empty();
        attrs
            .per_var
            .entry("tas".into())
            .or_default()
            .push(("_FillValue".into(), AttrValue::F32(vec![1.0e20])));
        let path = write_to_tmp_with_attrs(&response, &attrs);
        let bytes = std::fs::read(&path).unwrap();

        // The f32 1e20 encoded big-endian is [0x60, 0xad, 0x78, 0xec].
        let needle = 1.0e20_f32.to_be_bytes();
        assert!(
            bytes.windows(4).any(|w| w == needle),
            "_FillValue bytes should appear in the header"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn attr_list_sizing_matches_written_bytes() {
        // Walk-through: header_size() must agree with how many bytes
        // write_header() actually emits. Build a plan with every attr kind
        // on both the global and per-var slots; compare predicted to
        // actual header length.
        let tas = f32_var("tas", vec![("d", 1)], vec![290.0]);
        let response = DapResponse {
            dds: "…".into(),
            variables: vec![tas],
        };
        let mut attrs = Attrs::empty();
        attrs
            .global
            .push(("Conv".into(), AttrValue::Text("CF-1.7".into())));
        attrs
            .global
            .push(("x".into(), AttrValue::F32(vec![1.0, 2.0])));
        attrs.global.push(("y".into(), AttrValue::F64(vec![3.0])));
        attrs.per_var.entry("tas".into()).or_default().extend([
            ("units".into(), AttrValue::Text("K".into())),
            ("_FillValue".into(), AttrValue::F32(vec![1.0e20])),
        ]);
        let plan = Plan::from_response(&response, &attrs).unwrap();
        let mut out = Vec::new();
        plan.write_header(&mut out).unwrap();
        assert_eq!(out.len(), plan.header_size);
    }

    /// Shared helper used by attribute tests.
    fn write_to_tmp_with_attrs(response: &DapResponse, attrs: &Attrs) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "ferrous-nc-attr-test-{}-{}.nc",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        write_with_attrs(&p, response, attrs).expect("write ok");
        p
    }

    #[test]
    fn written_file_opens_with_minimal_classic_parser() {
        // Round-trip via our own re-parse of the header — not a full NetCDF
        // parser, just enough to confirm our layout matches the spec.
        let response = DapResponse {
            dds: "…".into(),
            variables: vec![f32_var("x", vec![("d", 4)], vec![1.0, 2.0, 3.0, 4.0])],
        };
        let path = write_to_tmp(&response);
        let bytes = std::fs::read(&path).unwrap();

        // magic + numrecs
        assert_eq!(&bytes[..4], b"CDF\x01");
        let numrecs = u32::from_be_bytes(bytes[4..8].try_into().unwrap());
        assert_eq!(numrecs, 0);
        // NC_DIMENSION tag
        assert_eq!(
            u32::from_be_bytes(bytes[8..12].try_into().unwrap()),
            NC_DIMENSION
        );
        // Number of dims = 1
        assert_eq!(u32::from_be_bytes(bytes[12..16].try_into().unwrap()), 1);
        // First dim name length = 1
        assert_eq!(u32::from_be_bytes(bytes[16..20].try_into().unwrap()), 1);
        // Name "d" + 3 pad bytes
        assert_eq!(&bytes[20..21], b"d");
        // Dim size = 4
        assert_eq!(u32::from_be_bytes(bytes[24..28].try_into().unwrap()), 4);

        let _ = std::fs::remove_file(&path);
    }
}
