// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{ScalarFunction, ScalarFunctionContext},
	value::{column::ColumnData, container::Utf8Container},
};
use reifydb_type::value::constraint::bytes::MaxBytes;

const IEC_UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
const SI_UNITS: [&str; 6] = ["B", "KB", "MB", "GB", "TB", "PB"];

fn format_bytes_internal(bytes: i64, base: f64, units: &[&str]) -> String {
	if bytes == 0 {
		return "0 B".to_string();
	}

	let bytes_abs = bytes.unsigned_abs() as f64;
	let sign = if bytes < 0 {
		"-"
	} else {
		""
	};

	let mut unit_index = 0;
	let mut value = bytes_abs;

	while value >= base && unit_index < units.len() - 1 {
		value /= base;
		unit_index += 1;
	}

	if unit_index == 0 {
		format!("{}{} {}", sign, bytes_abs as i64, units[0])
	} else if value == value.floor() {
		format!("{}{} {}", sign, value as i64, units[unit_index])
	} else {
		let formatted = format!("{:.2}", value);
		let trimmed = formatted.trim_end_matches('0').trim_end_matches('.');
		format!("{}{} {}", sign, trimmed, units[unit_index])
	}
}

macro_rules! process_int_column {
	($container:expr, $row_count:expr, $base:expr, $units:expr) => {{
		let mut result_data = Vec::with_capacity($row_count);
		let mut result_bitvec = Vec::with_capacity($row_count);

		for i in 0..$row_count {
			if let Some(&value) = $container.get(i) {
				result_data.push(format_bytes_internal(value as i64, $base, $units));
				result_bitvec.push(true);
			} else {
				result_data.push(String::new());
				result_bitvec.push(false);
			}
		}

		Ok(ColumnData::Utf8 {
			container: Utf8Container::new(result_data, result_bitvec.into()),
			max_bytes: MaxBytes::MAX,
		})
	}};
}

macro_rules! process_float_column {
	($container:expr, $row_count:expr, $base:expr, $units:expr) => {{
		let mut result_data = Vec::with_capacity($row_count);
		let mut result_bitvec = Vec::with_capacity($row_count);

		for i in 0..$row_count {
			if let Some(&value) = $container.get(i) {
				result_data.push(format_bytes_internal(value as i64, $base, $units));
				result_bitvec.push(true);
			} else {
				result_data.push(String::new());
				result_bitvec.push(false);
			}
		}

		Ok(ColumnData::Utf8 {
			container: Utf8Container::new(result_data, result_bitvec.into()),
			max_bytes: MaxBytes::MAX,
		})
	}};
}

macro_rules! process_decimal_column {
	($container:expr, $row_count:expr, $base:expr, $units:expr) => {{
		let mut result_data = Vec::with_capacity($row_count);
		let mut result_bitvec = Vec::with_capacity($row_count);

		for i in 0..$row_count {
			if let Some(value) = $container.get(i) {
				// Truncate decimal to integer by parsing the integer part
				let s = value.to_string();
				let int_part = s.split('.').next().unwrap_or("0");
				let bytes = int_part.parse::<i64>().unwrap_or(0);
				result_data.push(format_bytes_internal(bytes, $base, $units));
				result_bitvec.push(true);
			} else {
				result_data.push(String::new());
				result_bitvec.push(false);
			}
		}

		Ok(ColumnData::Utf8 {
			container: Utf8Container::new(result_data, result_bitvec.into()),
			max_bytes: MaxBytes::MAX,
		})
	}};
}

/// Formats bytes using binary units (1024-based: B, KiB, MiB, GiB, TiB, PiB)
pub struct FormatBytes;

impl FormatBytes {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for FormatBytes {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::Result<ColumnData> {
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.is_empty() {
			return Ok(ColumnData::utf8(Vec::<String>::new()));
		}

		let column = columns.get(0).unwrap();

		match &column.data() {
			ColumnData::Int1(container) => process_int_column!(container, row_count, 1024.0, &IEC_UNITS),
			ColumnData::Int2(container) => process_int_column!(container, row_count, 1024.0, &IEC_UNITS),
			ColumnData::Int4(container) => process_int_column!(container, row_count, 1024.0, &IEC_UNITS),
			ColumnData::Int8(container) => process_int_column!(container, row_count, 1024.0, &IEC_UNITS),
			ColumnData::Uint1(container) => process_int_column!(container, row_count, 1024.0, &IEC_UNITS),
			ColumnData::Uint2(container) => process_int_column!(container, row_count, 1024.0, &IEC_UNITS),
			ColumnData::Uint4(container) => process_int_column!(container, row_count, 1024.0, &IEC_UNITS),
			ColumnData::Uint8(container) => process_int_column!(container, row_count, 1024.0, &IEC_UNITS),
			ColumnData::Float4(container) => {
				process_float_column!(container, row_count, 1024.0, &IEC_UNITS)
			}
			ColumnData::Float8(container) => {
				process_float_column!(container, row_count, 1024.0, &IEC_UNITS)
			}
			ColumnData::Decimal {
				container,
				..
			} => {
				process_decimal_column!(container, row_count, 1024.0, &IEC_UNITS)
			}
			_ => unimplemented!("FormatBytes only supports numeric input"),
		}
	}
}

/// Formats bytes using SI/decimal units (1000-based: B, KB, MB, GB, TB, PB)
pub struct FormatBytesSi;

impl FormatBytesSi {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for FormatBytesSi {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::Result<ColumnData> {
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.is_empty() {
			return Ok(ColumnData::utf8(Vec::<String>::new()));
		}

		let column = columns.get(0).unwrap();

		match &column.data() {
			ColumnData::Int1(container) => process_int_column!(container, row_count, 1000.0, &SI_UNITS),
			ColumnData::Int2(container) => process_int_column!(container, row_count, 1000.0, &SI_UNITS),
			ColumnData::Int4(container) => process_int_column!(container, row_count, 1000.0, &SI_UNITS),
			ColumnData::Int8(container) => process_int_column!(container, row_count, 1000.0, &SI_UNITS),
			ColumnData::Uint1(container) => process_int_column!(container, row_count, 1000.0, &SI_UNITS),
			ColumnData::Uint2(container) => process_int_column!(container, row_count, 1000.0, &SI_UNITS),
			ColumnData::Uint4(container) => process_int_column!(container, row_count, 1000.0, &SI_UNITS),
			ColumnData::Uint8(container) => process_int_column!(container, row_count, 1000.0, &SI_UNITS),
			ColumnData::Float4(container) => process_float_column!(container, row_count, 1000.0, &SI_UNITS),
			ColumnData::Float8(container) => process_float_column!(container, row_count, 1000.0, &SI_UNITS),
			ColumnData::Decimal {
				container,
				..
			} => {
				process_decimal_column!(container, row_count, 1000.0, &SI_UNITS)
			}
			_ => unimplemented!("FormatBytesSi only supports numeric input"),
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::value::column::{Column, Columns};

	use super::*;

	#[tokio::test]
	async fn test_format_bytes_binary_basic() {
		let function = FormatBytes::new();

		let data = vec![0i64, 512, 1024, 1536, 1048576, 1073741824];
		let column = Column::int8("bytes", data);
		let columns = Columns::new(vec![column]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 6,
		};

		let result = function.scalar(ctx).unwrap();

		let ColumnData::Utf8 {
			container,
			..
		} = result
		else {
			panic!("Expected UTF8 column data");
		};

		assert_eq!(container[0], "0 B");
		assert_eq!(container[1], "512 B");
		assert_eq!(container[2], "1 KiB");
		assert_eq!(container[3], "1.5 KiB");
		assert_eq!(container[4], "1 MiB");
		assert_eq!(container[5], "1 GiB");
	}

	#[tokio::test]
	async fn test_format_bytes_si_basic() {
		let function = FormatBytesSi::new();

		let data = vec![0i64, 500, 1000, 1500, 1000000, 1000000000];
		let column = Column::int8("bytes", data);
		let columns = Columns::new(vec![column]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 6,
		};

		let result = function.scalar(ctx).unwrap();

		let ColumnData::Utf8 {
			container,
			..
		} = result
		else {
			panic!("Expected UTF8 column data");
		};

		assert_eq!(container[0], "0 B");
		assert_eq!(container[1], "500 B");
		assert_eq!(container[2], "1 KB");
		assert_eq!(container[3], "1.5 KB");
		assert_eq!(container[4], "1 MB");
		assert_eq!(container[5], "1 GB");
	}

	#[tokio::test]
	async fn test_format_bytes_int4() {
		let function = FormatBytes::new();

		let data = vec![1024i32, 2048, 1048576];
		let column = Column::int4("bytes", data);
		let columns = Columns::new(vec![column]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 3,
		};

		let result = function.scalar(ctx).unwrap();

		let ColumnData::Utf8 {
			container,
			..
		} = result
		else {
			panic!("Expected UTF8 column data");
		};

		assert_eq!(container[0], "1 KiB");
		assert_eq!(container[1], "2 KiB");
		assert_eq!(container[2], "1 MiB");
	}

	#[tokio::test]
	async fn test_format_bytes_with_decimals() {
		let function = FormatBytes::new();

		let data = vec![1536i64, 2560, 1572864];
		let column = Column::int8("bytes", data);
		let columns = Columns::new(vec![column]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 3,
		};

		let result = function.scalar(ctx).unwrap();

		let ColumnData::Utf8 {
			container,
			..
		} = result
		else {
			panic!("Expected UTF8 column data");
		};

		assert_eq!(container[0], "1.5 KiB");
		assert_eq!(container[1], "2.5 KiB");
		assert_eq!(container[2], "1.5 MiB");
	}

	#[tokio::test]
	async fn test_format_bytes_large_values() {
		let function = FormatBytes::new();

		let data = vec![
			1099511627776i64,    // 1 TiB
			1125899906842624i64, // 1 PiB
		];
		let column = Column::int8("bytes", data);
		let columns = Columns::new(vec![column]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 2,
		};

		let result = function.scalar(ctx).unwrap();

		let ColumnData::Utf8 {
			container,
			..
		} = result
		else {
			panic!("Expected UTF8 column data");
		};

		assert_eq!(container[0], "1 TiB");
		assert_eq!(container[1], "1 PiB");
	}

	#[tokio::test]
	async fn test_format_bytes_with_null() {
		use reifydb_core::BitVec;

		let function = FormatBytes::new();

		let data = vec![1024i64, 0, 2048];
		let mut bitvec = BitVec::repeat(3, true);
		bitvec.set(1, false);

		let column = Column::int8_with_bitvec("bytes", data, bitvec);
		let columns = Columns::new(vec![column]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 3,
		};

		let result = function.scalar(ctx).unwrap();

		let ColumnData::Utf8 {
			container,
			..
		} = result
		else {
			panic!("Expected UTF8 column data");
		};

		assert!(container.is_defined(0));
		assert!(!container.is_defined(1));
		assert!(container.is_defined(2));

		assert_eq!(container[0], "1 KiB");
		assert_eq!(container[2], "2 KiB");
	}

	#[tokio::test]
	async fn test_format_bytes_uint8() {
		let function = FormatBytes::new();

		let data = vec![0u64, 1024, 1048576, 1073741824];
		let column = Column::uint8("bytes", data);
		let columns = Columns::new(vec![column]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 4,
		};

		let result = function.scalar(ctx).unwrap();

		let ColumnData::Utf8 {
			container,
			..
		} = result
		else {
			panic!("Expected UTF8 column data");
		};

		assert_eq!(container[0], "0 B");
		assert_eq!(container[1], "1 KiB");
		assert_eq!(container[2], "1 MiB");
		assert_eq!(container[3], "1 GiB");
	}

	#[tokio::test]
	async fn test_format_bytes_uint4() {
		let function = FormatBytes::new();

		let data = vec![512u32, 1024, 2048];
		let column = Column::uint4("bytes", data);
		let columns = Columns::new(vec![column]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 3,
		};

		let result = function.scalar(ctx).unwrap();

		let ColumnData::Utf8 {
			container,
			..
		} = result
		else {
			panic!("Expected UTF8 column data");
		};

		assert_eq!(container[0], "512 B");
		assert_eq!(container[1], "1 KiB");
		assert_eq!(container[2], "2 KiB");
	}

	#[tokio::test]
	async fn test_format_bytes_float8() {
		let function = FormatBytes::new();

		let data = vec![1024.5f64, 1048576.0, 1572864.0];
		let column = Column::float8("bytes", data);
		let columns = Columns::new(vec![column]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 3,
		};

		let result = function.scalar(ctx).unwrap();

		let ColumnData::Utf8 {
			container,
			..
		} = result
		else {
			panic!("Expected UTF8 column data");
		};

		// Float 1024.5 truncates to 1024
		assert_eq!(container[0], "1 KiB");
		assert_eq!(container[1], "1 MiB");
		assert_eq!(container[2], "1.5 MiB");
	}

	#[tokio::test]
	async fn test_format_bytes_float4() {
		let function = FormatBytes::new();

		let data = vec![512.9f32, 1024.0, 2048.5];
		let column = Column::float4("bytes", data);
		let columns = Columns::new(vec![column]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 3,
		};

		let result = function.scalar(ctx).unwrap();

		let ColumnData::Utf8 {
			container,
			..
		} = result
		else {
			panic!("Expected UTF8 column data");
		};

		// Float 512.9 truncates to 512
		assert_eq!(container[0], "512 B");
		assert_eq!(container[1], "1 KiB");
		// Float 2048.5 truncates to 2048
		assert_eq!(container[2], "2 KiB");
	}

	#[tokio::test]
	async fn test_format_bytes_decimal() {
		use std::str::FromStr;

		use reifydb_type::Decimal;

		let function = FormatBytes::new();

		let data = vec![
			Decimal::from_str("1024").unwrap(),
			Decimal::from_str("1048576.5").unwrap(),
			Decimal::from_str("1572864").unwrap(),
		];
		let column = Column::new("bytes", ColumnData::decimal(data));
		let columns = Columns::new(vec![column]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 3,
		};

		let result = function.scalar(ctx).unwrap();

		let ColumnData::Utf8 {
			container,
			..
		} = result
		else {
			panic!("Expected UTF8 column data");
		};

		assert_eq!(container[0], "1 KiB");
		// Decimal 1048576.5 truncates to 1048576
		assert_eq!(container[1], "1 MiB");
		assert_eq!(container[2], "1.5 MiB");
	}
}
