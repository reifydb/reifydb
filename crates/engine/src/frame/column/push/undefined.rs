// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::frame::ColumnValues;

impl ColumnValues {
    pub fn push_undefined(&mut self) {
        match self {
            ColumnValues::Bool(values, validity) => {
                values.push(false);
                validity.push(false);
            }
            ColumnValues::Float4(values, validity) => {
                values.push(0.0);
                validity.push(false);
            }
            ColumnValues::Float8(values, validity) => {
                values.push(0.0);
                validity.push(false);
            }
            ColumnValues::Int1(values, validity) => {
                values.push(0);
                validity.push(false);
            }
            ColumnValues::Int2(values, validity) => {
                values.push(0);
                validity.push(false);
            }
            ColumnValues::Int4(values, validity) => {
                values.push(0);
                validity.push(false);
            }
            ColumnValues::Int8(values, validity) => {
                values.push(0);
                validity.push(false);
            }
            ColumnValues::Int16(values, validity) => {
                values.push(0);
                validity.push(false);
            }
            ColumnValues::String(values, validity) => {
                values.push(String::new());
                validity.push(false);
            }
            ColumnValues::Uint1(values, validity) => {
                values.push(0);
                validity.push(false);
            }
            ColumnValues::Uint2(values, validity) => {
                values.push(0);
                validity.push(false);
            }
            ColumnValues::Uint4(values, validity) => {
                values.push(0);
                validity.push(false);
            }
            ColumnValues::Uint8(values, validity) => {
                values.push(0);
                validity.push(false);
            }
            ColumnValues::Uint16(values, validity) => {
                values.push(0);
                validity.push(false);
            }
            ColumnValues::Undefined(len) => {
                *len += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
	use crate::frame::ColumnValues;

	#[test]
	fn test_bool() {
		let mut col = ColumnValues::bool(vec![true]);
		col.push_undefined();
		if let ColumnValues::Bool(v, valid) = col {
			assert_eq!(v.as_slice(), &[true, false]);
			assert_eq!(valid.as_slice(), &[true, false]);
		}
	}

	#[test]
	fn test_float4() {
		let mut col = ColumnValues::float4(vec![1.0]);
		col.push_undefined();
		if let ColumnValues::Float4(v, valid) = col {
			assert_eq!(v.as_slice(), &[1.0, 0.0]);
			assert_eq!(valid.as_slice(), &[true, false]);
		}
	}

	#[test]
	fn test_float8() {
		let mut col = ColumnValues::float8(vec![1.0]);
		col.push_undefined();
		if let ColumnValues::Float8(v, valid) = col {
			assert_eq!(v.as_slice(), &[1.0, 0.0]);
			assert_eq!(valid.as_slice(), &[true, false]);
		}
	}

	#[test]
	fn test_int1() {
		let mut col = ColumnValues::int1(vec![1]);
		col.push_undefined();
		if let ColumnValues::Int1(v, valid) = col {
			assert_eq!(v.as_slice(), &[1, 0]);
			assert_eq!(valid.as_slice(), &[true, false]);
		}
	}

	#[test]
	fn test_int2() {
		let mut col = ColumnValues::int2(vec![1]);
		col.push_undefined();
		if let ColumnValues::Int2(v, valid) = col {
			assert_eq!(v.as_slice(), &[1, 0]);
			assert_eq!(valid.as_slice(), &[true, false]);
		}
	}

	#[test]
	fn test_int4() {
		let mut col = ColumnValues::int4(vec![1]);
		col.push_undefined();
		if let ColumnValues::Int4(v, valid) = col {
			assert_eq!(v.as_slice(), &[1, 0]);
			assert_eq!(valid.as_slice(), &[true, false]);
		}
	}

	#[test]
	fn test_int8() {
		let mut col = ColumnValues::int8(vec![1]);
		col.push_undefined();
		if let ColumnValues::Int8(v, valid) = col {
			assert_eq!(v.as_slice(), &[1, 0]);
			assert_eq!(valid.as_slice(), &[true, false]);
		}
	}

	#[test]
	fn test_int16() {
		let mut col = ColumnValues::int16(vec![1]);
		col.push_undefined();
		if let ColumnValues::Int16(v, valid) = col {
			assert_eq!(v.as_slice(), &[1, 0]);
			assert_eq!(valid.as_slice(), &[true, false]);
		}
	}

	#[test]
	fn test_string() {
		let mut col = ColumnValues::string(vec!["a".to_string()]);
		col.push_undefined();
		if let ColumnValues::String(v, valid) = col {
			assert_eq!(v.as_slice(), &["a", ""]);
			assert_eq!(valid.as_slice(), &[true, false]);
		}
	}

	#[test]
	fn test_uint1() {
		let mut col = ColumnValues::uint1(vec![1]);
		col.push_undefined();
		if let ColumnValues::Uint1(v, valid) = col {
			assert_eq!(v.as_slice(), &[1, 0]);
			assert_eq!(valid.as_slice(), &[true, false]);
		}
	}

	#[test]
	fn test_uint2() {
		let mut col = ColumnValues::uint2(vec![1]);
		col.push_undefined();
		if let ColumnValues::Uint2(v, valid) = col {
			assert_eq!(v.as_slice(), &[1, 0]);
			assert_eq!(valid.as_slice(), &[true, false]);
		}
	}

	#[test]
	fn test_uint4() {
		let mut col = ColumnValues::uint4(vec![1]);
		col.push_undefined();
		if let ColumnValues::Uint4(v, valid) = col {
			assert_eq!(v.as_slice(), &[1, 0]);
			assert_eq!(valid.as_slice(), &[true, false]);
		}
	}

	#[test]
	fn test_uint8() {
		let mut col = ColumnValues::uint8(vec![1]);
		col.push_undefined();
		if let ColumnValues::Uint8(v, valid) = col {
			assert_eq!(v.as_slice(), &[1, 0]);
			assert_eq!(valid.as_slice(), &[true, false]);
		}
	}

	#[test]
	fn test_uint16() {
		let mut col = ColumnValues::uint16(vec![1]);
		col.push_undefined();
		if let ColumnValues::Uint16(v, valid) = col {
			assert_eq!(v.as_slice(), &[1, 0]);
			assert_eq!(valid.as_slice(), &[true, false]);
		}
	}

	#[test]
	fn test_undefined() {
		let mut col = ColumnValues::Undefined(5);
		col.push_undefined();
		if let ColumnValues::Undefined(len) = col {
			assert_eq!(len, 6);
		}
	}
}