//! DAP2 binary response decoder.
//!
//! OPeNDAP `.dods` responses have a fixed shape:
//!
//! ```text
//! <DDS text>
//! Data:
//! <XDR-encoded variables>
//! ```
//!
//! The DDS portion is human-readable and lists each projected variable with
//! its element type and dimensions. The data portion is XDR-encoded
//! big-endian, with array variables prefixed by their element count repeated
//! twice (the standard XDR variable-array marker).
//!
//! Scope of this module today: enough of the DAP2 spec to decode the
//! variables Ferrous actually fetches — Float32 / Float64 / Int32 / Int16
//! arrays (1D, 2D, 3D), one or more per response. That covers CMIP6 data
//! variables and the lat/lon coordinate arrays we'll need for degree-based
//! resolution. Strings, structures, sequences, and DAP4 are out of scope.

use crate::{Error, Result};

/// One scalar element type that may appear in a DDS.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DapType {
    Float32,
    Float64,
    Int32,
    Int16,
    Byte,
}

impl DapType {
    /// Wire size in bytes for one element.
    pub fn size(self) -> usize {
        match self {
            Self::Float32 | Self::Int32 => 4,
            Self::Float64 => 8,
            Self::Int16 => 2,
            Self::Byte => 1,
        }
    }

    fn from_dds_keyword(s: &str) -> Result<Self> {
        match s {
            "Float32" => Ok(Self::Float32),
            "Float64" => Ok(Self::Float64),
            "Int32" => Ok(Self::Int32),
            "Int16" => Ok(Self::Int16),
            "Byte" => Ok(Self::Byte),
            other => Err(Error::Parse(format!(
                "DAP2 type '{other}' is not supported by ferrous"
            ))),
        }
    }
}

/// One dimension declaration parsed from a DDS array.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DapDimension {
    pub name: String,
    pub size: usize,
}

/// Decoded variable contents — strongly typed by element kind.
#[derive(Clone, Debug, PartialEq)]
pub enum DapData {
    F32(Vec<f32>),
    F64(Vec<f64>),
    I32(Vec<i32>),
    I16(Vec<i16>),
    U8(Vec<u8>),
}

impl DapData {
    /// Number of elements (not bytes).
    pub fn len(&self) -> usize {
        match self {
            Self::F32(v) => v.len(),
            Self::F64(v) => v.len(),
            Self::I32(v) => v.len(),
            Self::I16(v) => v.len(),
            Self::U8(v) => v.len(),
        }
    }

    /// `true` if no elements were decoded.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Borrow the contents as `f32` if and only if the variable was Float32.
    pub fn as_f32(&self) -> Option<&[f32]> {
        if let Self::F32(v) = self {
            Some(v)
        } else {
            None
        }
    }

    /// Borrow the contents as `f64` if and only if the variable was Float64.
    pub fn as_f64(&self) -> Option<&[f64]> {
        if let Self::F64(v) = self {
            Some(v)
        } else {
            None
        }
    }
}

/// One decoded variable: name, type, shape, data.
#[derive(Clone, Debug, PartialEq)]
pub struct DapVariable {
    pub name: String,
    pub dtype: DapType,
    pub dimensions: Vec<DapDimension>,
    pub data: DapData,
}

impl DapVariable {
    /// Total element count = product of dimension sizes (1 for scalars).
    pub fn element_count(&self) -> usize {
        if self.dimensions.is_empty() {
            1
        } else {
            self.dimensions.iter().map(|d| d.size).product()
        }
    }
}

/// A fully-decoded DAP2 response.
#[derive(Clone, Debug, PartialEq)]
pub struct DapResponse {
    /// Original DDS text (handy for debugging / `ferrous inspect`).
    pub dds: String,
    /// Variables in declaration order.
    pub variables: Vec<DapVariable>,
}

/// The byte sequence separating the DDS text from the binary data section.
///
/// OPeNDAP servers emit this exact sequence; we look for it case-sensitively.
const DATA_SEPARATOR: &[u8] = b"\nData:\n";

/// Decode a complete `.dods` response.
pub fn decode(bytes: &[u8]) -> Result<DapResponse> {
    let sep_pos = find_subslice(bytes, DATA_SEPARATOR)
        .ok_or_else(|| Error::Parse("DAP2 response is missing the 'Data:' separator".into()))?;
    // DDS text excludes the trailing "\nData:\n" header.
    let dds_bytes = &bytes[..sep_pos];
    let dds_text = std::str::from_utf8(dds_bytes)
        .map_err(|e| Error::Parse(format!("DAP2 DDS is not valid UTF-8: {e}")))?
        .to_string();

    let var_specs = parse_dds(&dds_text)?;

    let data_start = sep_pos + DATA_SEPARATOR.len();
    let mut cursor = data_start;
    let mut variables = Vec::with_capacity(var_specs.len());
    for spec in var_specs {
        let var = decode_variable(bytes, &mut cursor, spec)?;
        variables.push(var);
    }

    Ok(DapResponse {
        dds: dds_text,
        variables,
    })
}

/// Variable shape and type extracted from the DDS, before binary decoding.
struct VarSpec {
    name: String,
    dtype: DapType,
    dimensions: Vec<DapDimension>,
}

/// Parse the DDS text. Recognises declarations of the form:
///
/// ```text
/// Dataset {
///     <Type> <name>[<dim> = <N>][...];
///     ...
/// } <path>;
/// ```
///
/// Also handles `Grid` blocks (THREDDS's default wrapper for variables with
/// associated coordinate axes):
///
/// ```text
/// Grid {
///   ARRAY:
///     <Type> <name>[<dim> = <N>][...];
///   MAPS:
///     <Type> <map_name>[<dim> = <N>];
///     ...
/// } <grid_name>;
/// ```
///
/// On the wire a Grid encodes as the array's XDR followed by each MAP's XDR
/// in declaration order, so we flatten the grid into N+1 sequential
/// `VarSpec`s and the decoder reads them sequentially. The grid name
/// (`} tas;`) is dropped — the inner variables retain their own names.
///
/// Other container types (`Structure`, `Sequence`) error eagerly because
/// their wire layouts differ.
fn parse_dds(dds: &str) -> Result<Vec<VarSpec>> {
    let mut specs = Vec::new();
    for raw_line in dds.lines() {
        let line = raw_line.trim();
        if line.is_empty()
            || line.starts_with("Dataset")
            || line.starts_with('{')
            || line.starts_with('}')
            // Grid wrapper + section labels: structural noise, no data.
            || line.starts_with("Grid")
            || line == "ARRAY:"
            || line == "MAPS:"
        {
            continue;
        }
        // Reject container types whose XDR layout we don't yet support.
        for unsupported in ["Structure", "Sequence"] {
            if line.starts_with(unsupported) {
                return Err(Error::Parse(format!(
                    "DAP2 container type '{unsupported}' is not supported"
                )));
            }
        }
        // Strip trailing ';' if present.
        let line = line.trim_end_matches(';').trim();
        let (type_kw, rest) = line
            .split_once(char::is_whitespace)
            .ok_or_else(|| Error::Parse(format!("malformed DDS line: '{line}'")))?;
        let dtype = match DapType::from_dds_keyword(type_kw) {
            Ok(t) => t,
            // Lines that don't start with a recognised type keyword are
            // skipped — they're documentation tail like `} path/to/file`.
            Err(_) => continue,
        };

        // rest = "name[dim = N][dim2 = M]"  or  "name"
        let (name, dims_part) = match rest.find('[') {
            Some(i) => (&rest[..i], &rest[i..]),
            None => (rest, ""),
        };
        let name = name.trim().to_string();
        if name.is_empty() {
            return Err(Error::Parse(format!(
                "DDS line has empty variable name: '{line}'"
            )));
        }
        let dimensions = parse_dimensions(dims_part)?;
        specs.push(VarSpec {
            name,
            dtype,
            dimensions,
        });
    }
    if specs.is_empty() {
        return Err(Error::Parse(
            "DDS contained no variable declarations".into(),
        ));
    }
    Ok(specs)
}

/// Parse `[dim_name = N][dim2 = M]...` into a vector of `DapDimension`.
fn parse_dimensions(spec: &str) -> Result<Vec<DapDimension>> {
    let mut dims = Vec::new();
    let mut rest = spec.trim();
    while !rest.is_empty() {
        let close = rest.find(']').ok_or_else(|| {
            Error::Parse(format!("unmatched '[' in DDS dimension spec: '{spec}'"))
        })?;
        let inner = &rest[1..close]; // strip '['
        let (name, n_str) = inner
            .split_once('=')
            .ok_or_else(|| Error::Parse(format!("expected 'name = N' in dimension '[{inner}]'")))?;
        let size: usize = n_str.trim().parse().map_err(|_| {
            Error::Parse(format!("dimension size '{}' is not a number", n_str.trim()))
        })?;
        dims.push(DapDimension {
            name: name.trim().to_string(),
            size,
        });
        rest = rest[close + 1..].trim();
    }
    Ok(dims)
}

fn decode_variable(buf: &[u8], cursor: &mut usize, spec: VarSpec) -> Result<DapVariable> {
    let element_count = if spec.dimensions.is_empty() {
        1
    } else {
        spec.dimensions.iter().map(|d| d.size).product()
    };

    if !spec.dimensions.is_empty() {
        // XDR variable-array marker: count repeated twice. Verify both copies
        // match the DDS-declared element count, otherwise the response is
        // inconsistent and decoding the rest would corrupt downstream state.
        let n1 = read_u32_be(buf, cursor)? as usize;
        let n2 = read_u32_be(buf, cursor)? as usize;
        if n1 != n2 {
            return Err(Error::Parse(format!(
                "DAP2 array '{}' has mismatched length markers ({} vs {})",
                spec.name, n1, n2
            )));
        }
        if n1 != element_count {
            return Err(Error::Parse(format!(
                "DAP2 array '{}' length {} does not match declared shape {}",
                spec.name, n1, element_count
            )));
        }
    }

    let payload_bytes = element_count * spec.dtype.size();
    let payload = read_bytes(buf, cursor, payload_bytes, &spec.name)?;
    let data = decode_payload(spec.dtype, payload)?;

    // XDR pads variable-length data to a 4-byte boundary. For Float32 / Int32
    // / Float64 arrays, payload is already aligned; only Int16 and Byte need
    // explicit pad-skip when the count isn't already a multiple of 4 / 2.
    if !spec.dimensions.is_empty() {
        let pad = padding(payload_bytes, 4);
        if pad > 0 {
            // Tolerate truncation here: some servers don't emit padding for
            // the final variable. Skip what we can and move on.
            *cursor = (*cursor + pad).min(buf.len());
        }
    }

    Ok(DapVariable {
        name: spec.name,
        dtype: spec.dtype,
        dimensions: spec.dimensions,
        data,
    })
}

fn decode_payload(dtype: DapType, payload: &[u8]) -> Result<DapData> {
    let size = dtype.size();
    debug_assert!(payload.len() % size == 0);
    let count = payload.len() / size;
    Ok(match dtype {
        DapType::Float32 => {
            let mut out = Vec::with_capacity(count);
            for chunk in payload.chunks_exact(4) {
                out.push(f32::from_be_bytes(chunk.try_into().unwrap()));
            }
            DapData::F32(out)
        }
        DapType::Float64 => {
            let mut out = Vec::with_capacity(count);
            for chunk in payload.chunks_exact(8) {
                out.push(f64::from_be_bytes(chunk.try_into().unwrap()));
            }
            DapData::F64(out)
        }
        DapType::Int32 => {
            let mut out = Vec::with_capacity(count);
            for chunk in payload.chunks_exact(4) {
                out.push(i32::from_be_bytes(chunk.try_into().unwrap()));
            }
            DapData::I32(out)
        }
        DapType::Int16 => {
            let mut out = Vec::with_capacity(count);
            for chunk in payload.chunks_exact(2) {
                out.push(i16::from_be_bytes(chunk.try_into().unwrap()));
            }
            DapData::I16(out)
        }
        DapType::Byte => DapData::U8(payload.to_vec()),
    })
}

fn read_u32_be(buf: &[u8], cursor: &mut usize) -> Result<u32> {
    let bytes = read_bytes(buf, cursor, 4, "u32 marker")?;
    Ok(u32::from_be_bytes(bytes.try_into().unwrap()))
}

fn read_bytes<'a>(buf: &'a [u8], cursor: &mut usize, n: usize, what: &str) -> Result<&'a [u8]> {
    let end = cursor
        .checked_add(n)
        .ok_or_else(|| Error::Parse(format!("DAP2 read overflow at {what}")))?;
    if end > buf.len() {
        return Err(Error::Parse(format!(
            "DAP2 response truncated reading {what}: need {n} bytes, have {}",
            buf.len() - *cursor
        )));
    }
    let slice = &buf[*cursor..end];
    *cursor = end;
    Ok(slice)
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn padding(len: usize, alignment: usize) -> usize {
    let r = len % alignment;
    if r == 0 {
        0
    } else {
        alignment - r
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a synthetic DAP2 response in memory: DDS + Data + an XDR-encoded
    /// Float32 array. Used to test the parser without a network round-trip.
    fn synthetic_float32(name: &str, dims: &[(&str, usize)], values: &[f32]) -> Vec<u8> {
        let total: usize = dims.iter().map(|(_, n)| n).product();
        assert_eq!(total, values.len(), "test fixture shape mismatch");
        let mut dds = format!("Dataset {{\n    Float32 {name}");
        for (n, s) in dims {
            dds.push_str(&format!("[{n} = {s}]"));
        }
        dds.push_str(";\n} test;");
        let mut out = dds.into_bytes();
        out.extend_from_slice(b"\nData:\n");
        if !dims.is_empty() {
            out.extend_from_slice(&(values.len() as u32).to_be_bytes());
            out.extend_from_slice(&(values.len() as u32).to_be_bytes());
        }
        for v in values {
            out.extend_from_slice(&v.to_be_bytes());
        }
        out
    }

    #[test]
    fn decodes_1d_float32() {
        let bytes = synthetic_float32("lat", &[("lat", 4)], &[-89.5, -0.5, 30.5, 89.5]);
        let r = decode(&bytes).unwrap();
        assert_eq!(r.variables.len(), 1);
        let v = &r.variables[0];
        assert_eq!(v.name, "lat");
        assert_eq!(v.dtype, DapType::Float32);
        assert_eq!(
            v.dimensions,
            vec![DapDimension {
                name: "lat".into(),
                size: 4
            }]
        );
        assert_eq!(v.data.as_f32().unwrap(), &[-89.5, -0.5, 30.5, 89.5]);
    }

    #[test]
    fn decodes_3d_float32() {
        // 2 × 2 × 3 = 12 floats.
        let values: Vec<f32> = (0..12).map(|i| i as f32 * 0.5).collect();
        let bytes = synthetic_float32("tos", &[("time", 2), ("lat", 2), ("lon", 3)], &values);
        let r = decode(&bytes).unwrap();
        let v = &r.variables[0];
        assert_eq!(v.element_count(), 12);
        assert_eq!(v.data.as_f32().unwrap(), values.as_slice());
    }

    #[test]
    fn rejects_truncated_payload() {
        // Build a valid header but chop the data section.
        let mut bytes = synthetic_float32("x", &[("d", 4)], &[1.0, 2.0, 3.0, 4.0]);
        bytes.truncate(bytes.len() - 4); // drop the last float
        assert!(decode(&bytes).is_err());
    }

    #[test]
    fn rejects_mismatched_length_markers() {
        // Build with declared count 4 but inject a different second marker.
        let mut bytes = synthetic_float32("x", &[("d", 4)], &[1.0, 2.0, 3.0, 4.0]);
        // Find the second marker (at the start of the data section) and mutate it.
        let sep = b"\nData:\n";
        let pos = bytes.windows(sep.len()).position(|w| w == sep).unwrap() + sep.len();
        // Bytes [pos..pos+4] = first marker (4); [pos+4..pos+8] = second.
        bytes[pos + 4..pos + 8].copy_from_slice(&5u32.to_be_bytes());
        assert!(decode(&bytes).is_err());
    }

    #[test]
    fn rejects_missing_data_separator() {
        let bytes = b"Dataset {\n    Float32 x[d = 1];\n} test;\n";
        assert!(decode(bytes).is_err());
    }

    #[test]
    fn rejects_empty_dds() {
        let bytes = b"\nData:\n";
        assert!(decode(bytes).is_err());
    }

    #[test]
    fn rejects_unsupported_container_type() {
        let bytes = b"Dataset {\n    Structure s { ... };\n} test;\nData:\n";
        assert!(decode(bytes).is_err());
    }

    #[test]
    fn decodes_grid_as_flat_sequence_of_array_then_maps() {
        // Synthetic Grid: tas[time = 2][lat = 2] + maps time + lat.
        let tas: Vec<f32> = vec![1.0, 2.0, 3.0, 4.0]; // 2 × 2
        let time_vals: Vec<f64> = vec![100.0, 200.0];
        let lat_vals: Vec<f64> = vec![-30.0, 30.0];

        let mut bytes = b"Dataset {\n    Grid {\n     ARRAY:\n        Float32 tas[time = 2][lat = 2];\n     MAPS:\n        Float64 time[time = 2];\n        Float64 lat[lat = 2];\n    } tas;\n} test;".to_vec();
        bytes.extend_from_slice(b"\nData:\n");
        // ARRAY: tas (4 elements, Float32)
        bytes.extend_from_slice(&4u32.to_be_bytes());
        bytes.extend_from_slice(&4u32.to_be_bytes());
        for v in &tas {
            bytes.extend_from_slice(&v.to_be_bytes());
        }
        // MAP: time (2 elements, Float64)
        bytes.extend_from_slice(&2u32.to_be_bytes());
        bytes.extend_from_slice(&2u32.to_be_bytes());
        for v in &time_vals {
            bytes.extend_from_slice(&v.to_be_bytes());
        }
        // MAP: lat (2 elements, Float64)
        bytes.extend_from_slice(&2u32.to_be_bytes());
        bytes.extend_from_slice(&2u32.to_be_bytes());
        for v in &lat_vals {
            bytes.extend_from_slice(&v.to_be_bytes());
        }

        let r = decode(&bytes).expect("Grid response should decode");
        assert_eq!(r.variables.len(), 3, "array + 2 maps");
        assert_eq!(r.variables[0].name, "tas");
        assert_eq!(r.variables[0].data.as_f32().unwrap(), tas.as_slice());
        assert_eq!(r.variables[1].name, "time");
        assert_eq!(r.variables[1].data.as_f64().unwrap(), time_vals.as_slice());
        assert_eq!(r.variables[2].name, "lat");
        assert_eq!(r.variables[2].data.as_f64().unwrap(), lat_vals.as_slice());
    }

    #[test]
    fn parse_dds_skips_blank_lines_and_dataset_braces() {
        let dds = "Dataset {\n\n    Float32 lat[lat = 3];\n    Float32 lon[lon = 4];\n} test;";
        let specs = parse_dds(dds).unwrap();
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].name, "lat");
        assert_eq!(specs[1].name, "lon");
    }

    #[test]
    fn dimension_parser_extracts_name_and_size() {
        let dims = parse_dimensions("[time = 12][lat = 41][lon = 31]").unwrap();
        assert_eq!(dims.len(), 3);
        assert_eq!(
            dims[1],
            DapDimension {
                name: "lat".into(),
                size: 41
            }
        );
    }

    #[test]
    fn padding_aligns_to_four_bytes() {
        assert_eq!(padding(0, 4), 0);
        assert_eq!(padding(4, 4), 0);
        assert_eq!(padding(5, 4), 3);
        assert_eq!(padding(7, 4), 1);
    }

    #[test]
    fn decodes_two_variables_in_one_response() {
        // Build "lat[3]" + "lon[4]" in one synthetic response.
        let mut bytes =
            b"Dataset {\n    Float32 lat[lat = 3];\n    Float32 lon[lon = 4];\n} test;".to_vec();
        bytes.extend_from_slice(b"\nData:\n");
        // lat
        bytes.extend_from_slice(&3u32.to_be_bytes());
        bytes.extend_from_slice(&3u32.to_be_bytes());
        for v in [-30.0_f32, 0.0, 30.0] {
            bytes.extend_from_slice(&v.to_be_bytes());
        }
        // lon
        bytes.extend_from_slice(&4u32.to_be_bytes());
        bytes.extend_from_slice(&4u32.to_be_bytes());
        for v in [0.0_f32, 90.0, 180.0, 270.0] {
            bytes.extend_from_slice(&v.to_be_bytes());
        }
        let r = decode(&bytes).unwrap();
        assert_eq!(r.variables.len(), 2);
        assert_eq!(r.variables[0].name, "lat");
        assert_eq!(r.variables[0].data.as_f32().unwrap(), &[-30.0, 0.0, 30.0]);
        assert_eq!(r.variables[1].name, "lon");
        assert_eq!(
            r.variables[1].data.as_f32().unwrap(),
            &[0.0, 90.0, 180.0, 270.0]
        );
    }
}
