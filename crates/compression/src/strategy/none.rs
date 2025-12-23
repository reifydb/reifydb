// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::value::column::{ColumnData, CompressedColumn, CompressionType};
use reifydb_type::{Result, diagnostic, error};

use crate::ColumnCompressor;

pub struct NoneCompressor {}

impl ColumnCompressor for NoneCompressor {
	fn compress(&self, data: &ColumnData) -> Result<CompressedColumn> {
		let serialized = postcard::to_stdvec(data)
			.map_err(|e| error!(diagnostic::serde::serde_serialize_error(e.to_string())))?;

		let uncompressed_size = serialized.len();

		Ok(CompressedColumn {
			data: serialized,
			compression: CompressionType::None,
			uncompressed_size,
			undefined_count: data.undefined_count(),
			row_count: data.len(),
		})
	}

	fn decompress(&self, compressed: &CompressedColumn) -> Result<ColumnData> {
		assert_eq!(compressed.compression, CompressionType::None);

		let result: ColumnData = postcard::from_bytes(&compressed.data)
			.map_err(|e| error!(diagnostic::serde::serde_deserialize_error(e.to_string())))?;

		Ok(result)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_compress_decompress_bool() {
		let compressor = NoneCompressor {};

		let data = ColumnData::bool_optional([Some(true), Some(false), None, Some(true), None]);

		let compressed = compressor.compress(&data).unwrap();
		assert_eq!(compressed.compression, CompressionType::None);
		assert_eq!(compressed.row_count, 5);
		assert_eq!(compressed.undefined_count, 2);
		assert_eq!(compressed.uncompressed_size, compressed.data.len());

		let decompressed = compressor.decompress(&compressed).unwrap();
		assert_eq!(data, decompressed);
	}

	#[test]
	fn test_compress_decompress_int() {
		let compressor = NoneCompressor {};

		let data = ColumnData::int4_optional([
			Some(42),
			Some(-100),
			None,
			Some(0),
			Some(i32::MAX),
			Some(i32::MIN),
		]);

		let compressed = compressor.compress(&data).unwrap();
		assert_eq!(compressed.compression, CompressionType::None);
		assert_eq!(compressed.row_count, 6);
		assert_eq!(compressed.undefined_count, 1);

		let decompressed = compressor.decompress(&compressed).unwrap();
		assert_eq!(data, decompressed);
	}

	#[test]
	fn test_compress_decompress_string() {
		let compressor = NoneCompressor {};

		let data = ColumnData::utf8_optional([
			Some("hello".to_string()),
			Some("world".to_string()),
			None,
			Some("".to_string()),
			Some("a very long string with special chars: 特殊文字 🎉".to_string()),
		]);

		let compressed = compressor.compress(&data).unwrap();
		assert_eq!(compressed.compression, CompressionType::None);
		assert_eq!(compressed.row_count, 5);
		assert_eq!(compressed.undefined_count, 1);

		let decompressed = compressor.decompress(&compressed).unwrap();
		assert_eq!(data, decompressed);
	}

	#[test]
	fn test_empty_column() {
		let compressor = NoneCompressor {};

		let data = ColumnData::int4_with_capacity(0);

		let compressed = compressor.compress(&data).unwrap();
		assert_eq!(compressed.row_count, 0);
		assert_eq!(compressed.undefined_count, 0);

		let decompressed = compressor.decompress(&compressed).unwrap();
		assert_eq!(data, decompressed);
	}

	#[test]
	fn test_all_undefined() {
		let compressor = NoneCompressor {};

		let data = ColumnData::float8_optional([None, None, None, None]);

		let compressed = compressor.compress(&data).unwrap();
		assert_eq!(compressed.row_count, 4);
		assert_eq!(compressed.undefined_count, 4);

		let decompressed = compressor.decompress(&compressed).unwrap();
		assert_eq!(data, decompressed);
	}

	#[test]
	fn test_various_numeric_types() {
		let compressor = NoneCompressor {};

		let data_i8 = ColumnData::int8_optional([Some(i64::MAX), Some(i64::MIN), None]);
		let compressed = compressor.compress(&data_i8).unwrap();
		let decompressed = compressor.decompress(&compressed).unwrap();
		assert_eq!(data_i8, decompressed);

		let data_u4 = ColumnData::uint4_optional([Some(u32::MAX), Some(0), None]);
		let compressed = compressor.compress(&data_u4).unwrap();
		let decompressed = compressor.decompress(&compressed).unwrap();
		assert_eq!(data_u4, decompressed);

		let data_f4 = ColumnData::float4_optional([Some(3.14), Some(-0.0), None, Some(f32::INFINITY)]);
		let compressed = compressor.compress(&data_f4).unwrap();
		let decompressed = compressor.decompress(&compressed).unwrap();
		assert_eq!(data_f4, decompressed);
	}

	#[test]
	fn test_round_trip_preserves_data() {
		let compressor = NoneCompressor {};

		let bool_data = ColumnData::bool([true, false, true]);
		let compressed = compressor.compress(&bool_data).unwrap();
		let decompressed = compressor.decompress(&compressed).unwrap();
		assert_eq!(bool_data, decompressed);

		let int_data = ColumnData::int4([1, 2, 3, 4, 5]);
		let compressed = compressor.compress(&int_data).unwrap();
		let decompressed = compressor.decompress(&compressed).unwrap();
		assert_eq!(int_data, decompressed);

		let string_data = ColumnData::utf8(["hello", "world", "test"]);
		let compressed = compressor.compress(&string_data).unwrap();
		let decompressed = compressor.decompress(&compressed).unwrap();
		assert_eq!(string_data, decompressed);
	}
}
