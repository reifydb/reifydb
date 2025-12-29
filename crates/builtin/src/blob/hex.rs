// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{ScalarFunction, ScalarFunctionContext},
	value::column::ColumnData,
};
use reifydb_type::{Fragment, value::Blob};

pub struct BlobHex;

impl BlobHex {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for BlobHex {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::Result<ColumnData> {
		let columns = ctx.columns;
		let row_count = ctx.row_count;
		let column = columns.get(0).unwrap();

		match &column.data() {
			ColumnData::Utf8 {
				container,
				..
			} => {
				let mut result_data = Vec::with_capacity(container.data().len());

				for i in 0..row_count {
					if container.is_defined(i) {
						let hex_str = &container[i];
						let blob = Blob::from_hex(Fragment::internal(hex_str))?;
						result_data.push(blob);
					} else {
						result_data.push(Blob::empty())
					}
				}

				Ok(ColumnData::blob_with_bitvec(result_data, container.bitvec().clone()))
			}
			_ => unimplemented!("BlobHex only supports text input"),
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::value::{
		column::{Column, Columns},
		container::Utf8Container,
	};
	use reifydb_type::{Fragment, value::constraint::bytes::MaxBytes};

	use super::*;
	use crate::ScalarFunctionContext;

	#[tokio::test]
	async fn test_blob_hex_valid_input() {
		let function = BlobHex::new();

		let hex_data = vec!["deadbeef".to_string()];
		let bitvec = vec![true];
		let input_column = Column {
			name: Fragment::internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(hex_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};

		let columns = Columns::new(vec![input_column]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 1,
		};
		let result = function.scalar(ctx).unwrap();

		let ColumnData::Blob {
			container,
			..
		} = result
		else {
			panic!("Expected BLOB column data");
		};
		assert_eq!(container.len(), 1);
		assert!(container.is_defined(0));
		assert_eq!(container[0].as_bytes(), &[0xde, 0xad, 0xbe, 0xef]);
	}

	#[tokio::test]
	async fn test_blob_hex_empty_string() {
		let function = BlobHex::new();

		let hex_data = vec!["".to_string()];
		let bitvec = vec![true];
		let input_column = Column {
			name: Fragment::internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(hex_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};

		let columns = Columns::new(vec![input_column]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 1,
		};
		let result = function.scalar(ctx).unwrap();

		let ColumnData::Blob {
			container,
			..
		} = result
		else {
			panic!("Expected BLOB column data");
		};
		assert_eq!(container.len(), 1);
		assert!(container.is_defined(0));
		assert_eq!(container[0].as_bytes(), &[] as &[u8]);
	}

	#[tokio::test]
	async fn test_blob_hex_uppercase() {
		let function = BlobHex::new();

		let hex_data = vec!["DEADBEEF".to_string()];
		let bitvec = vec![true];
		let input_column = Column {
			name: Fragment::internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(hex_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};

		let columns = Columns::new(vec![input_column]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 1,
		};
		let result = function.scalar(ctx).unwrap();

		let ColumnData::Blob {
			container,
			..
		} = result
		else {
			panic!("Expected BLOB column data");
		};
		assert_eq!(container.len(), 1);
		assert!(container.is_defined(0));
		assert_eq!(container[0].as_bytes(), &[0xde, 0xad, 0xbe, 0xef]);
	}

	#[tokio::test]
	async fn test_blob_hex_mixed_case() {
		let function = BlobHex::new();

		let hex_data = vec!["DeAdBeEf".to_string()];
		let bitvec = vec![true];
		let input_column = Column {
			name: Fragment::internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(hex_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};

		let columns = Columns::new(vec![input_column]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 1,
		};
		let result = function.scalar(ctx).unwrap();

		let ColumnData::Blob {
			container,
			..
		} = result
		else {
			panic!("Expected BLOB column data");
		};
		assert_eq!(container.len(), 1);
		assert!(container.is_defined(0));
		assert_eq!(container[0].as_bytes(), &[0xde, 0xad, 0xbe, 0xef]);
	}

	#[tokio::test]
	async fn test_blob_hex_multiple_rows() {
		let function = BlobHex::new();

		let hex_data = vec!["ff".to_string(), "00".to_string(), "deadbeef".to_string()];
		let bitvec = vec![true, true, true];
		let input_column = Column {
			name: Fragment::internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(hex_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};

		let columns = Columns::new(vec![input_column]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 3,
		};
		let result = function.scalar(ctx).unwrap();

		let ColumnData::Blob {
			container,
			..
		} = result
		else {
			panic!("Expected BLOB column data");
		};
		assert_eq!(container.len(), 3);
		assert!(container.is_defined(0));
		assert!(container.is_defined(1));
		assert!(container.is_defined(2));

		assert_eq!(container[0].as_bytes(), &[0xff]);
		assert_eq!(container[1].as_bytes(), &[0x00]);
		assert_eq!(container[2].as_bytes(), &[0xde, 0xad, 0xbe, 0xef]);
	}

	#[tokio::test]
	async fn test_blob_hex_with_null_data() {
		let function = BlobHex::new();

		let hex_data = vec!["ff".to_string(), "".to_string(), "deadbeef".to_string()];
		let bitvec = vec![true, false, true];
		let input_column = Column {
			name: Fragment::internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(hex_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};

		let columns = Columns::new(vec![input_column]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 3,
		};
		let result = function.scalar(ctx).unwrap();

		let ColumnData::Blob {
			container,
			..
		} = result
		else {
			panic!("Expected BLOB column data");
		};
		assert_eq!(container.len(), 3);
		assert!(container.is_defined(0));
		assert!(!container.is_defined(1));
		assert!(container.is_defined(2));

		assert_eq!(container[0].as_bytes(), &[0xff]);
		assert_eq!(container[1].as_bytes(), [].as_slice() as &[u8]);
		assert_eq!(container[2].as_bytes(), &[0xde, 0xad, 0xbe, 0xef]);
	}

	#[tokio::test]
	async fn test_blob_hex_invalid_input_should_error() {
		let function = BlobHex::new();

		let hex_data = vec!["invalid_hex".to_string()];
		let bitvec = vec![true];
		let input_column = Column {
			name: Fragment::internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(hex_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};

		let columns = Columns::new(vec![input_column]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 1,
		};
		let result = function.scalar(ctx);
		assert!(result.is_err(), "Expected error for invalid hex input");
	}

	#[tokio::test]
	async fn test_blob_hex_odd_length_should_error() {
		let function = BlobHex::new();

		let hex_data = vec!["abc".to_string()];
		let bitvec = vec![true];
		let input_column = Column {
			name: Fragment::internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(hex_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};

		let columns = Columns::new(vec![input_column]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 1,
		};
		let result = function.scalar(ctx);
		assert!(result.is_err(), "Expected error for odd length hex string");
	}
}
