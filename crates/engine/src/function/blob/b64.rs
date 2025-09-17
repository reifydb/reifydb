// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::value::columnar::ColumnData;
use reifydb_type::{OwnedFragment, value::Blob};

use crate::function::{ScalarFunction, ScalarFunctionContext};

pub struct BlobB64;

impl BlobB64 {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for BlobB64 {
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
						let b64_str = &container[i];
						let blob = Blob::from_b64(OwnedFragment::internal(b64_str))?;
						result_data.push(blob);
					} else {
						result_data.push(Blob::empty())
					}
				}

				Ok(ColumnData::blob_with_bitvec(result_data, container.bitvec().clone()))
			}
			_ => unimplemented!("BlobB64 only supports text input"),
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::value::{
		columnar::{Column, ColumnQualified, Columns},
		container::Utf8Container,
	};
	use reifydb_type::value::constraint::bytes::MaxBytes;

	use super::*;
	use crate::function::ScalarFunctionContext;

	#[test]
	fn test_blob_b64_valid_input() {
		let function = BlobB64::new();

		// "Hello!" in base64 is "SGVsbG8h"
		let b64_data = vec!["SGVsbG8h".to_string()];
		let bitvec = vec![true];
		let input_column = ColumnQualified {
			name: "input".to_string(),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(b64_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};

		let columns = Columns::new(vec![Column::ColumnQualified(input_column)]);
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
		assert_eq!(container[0].as_bytes(), "Hello!".as_bytes());
	}

	#[test]
	fn test_blob_b64_empty_string() {
		let function = BlobB64::new();

		let b64_data = vec!["".to_string()];
		let bitvec = vec![true];
		let input_column = ColumnQualified {
			name: "input".to_string(),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(b64_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};

		let columns = Columns::new(vec![Column::ColumnQualified(input_column)]);
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

	#[test]
	fn test_blob_b64_with_padding() {
		let function = BlobB64::new();

		// "Hello" in base64 is "SGVsbG8="
		let b64_data = vec!["SGVsbG8=".to_string()];
		let bitvec = vec![true];
		let input_column = ColumnQualified {
			name: "input".to_string(),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(b64_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};

		let columns = Columns::new(vec![Column::ColumnQualified(input_column)]);
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
		assert_eq!(container[0].as_bytes(), "Hello".as_bytes());
	}

	#[test]
	fn test_blob_b64_multiple_rows() {
		let function = BlobB64::new();

		// "A" = "QQ==", "BC" = "QkM=", "DEF" = "REVG"
		let b64_data = vec!["QQ==".to_string(), "QkM=".to_string(), "REVG".to_string()];
		let bitvec = vec![true, true, true];
		let input_column = ColumnQualified {
			name: "input".to_string(),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(b64_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};

		let columns = Columns::new(vec![Column::ColumnQualified(input_column)]);
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

		assert_eq!(container[0].as_bytes(), "A".as_bytes());
		assert_eq!(container[1].as_bytes(), "BC".as_bytes());
		assert_eq!(container[2].as_bytes(), "DEF".as_bytes());
	}

	#[test]
	fn test_blob_b64_with_null_data() {
		let function = BlobB64::new();

		let b64_data = vec!["QQ==".to_string(), "".to_string(), "REVG".to_string()];
		let bitvec = vec![true, false, true];
		let input_column = ColumnQualified {
			name: "input".to_string(),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(b64_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};

		let columns = Columns::new(vec![Column::ColumnQualified(input_column)]);
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

		assert_eq!(container[0].as_bytes(), "A".as_bytes());
		assert_eq!(container[1].as_bytes(), [].as_slice() as &[u8]);
		assert_eq!(container[2].as_bytes(), "DEF".as_bytes());
	}

	#[test]
	fn test_blob_b64_binary_data() {
		let function = BlobB64::new();

		// Binary data: [0xde, 0xad, 0xbe, 0xef] in base64 is "3q2+7w=="
		let b64_data = vec!["3q2+7w==".to_string()];
		let bitvec = vec![true];
		let input_column = ColumnQualified {
			name: "input".to_string(),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(b64_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};

		let columns = Columns::new(vec![Column::ColumnQualified(input_column)]);
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

	#[test]
	fn test_blob_b64_invalid_input_should_error() {
		let function = BlobB64::new();

		let b64_data = vec!["invalid@base64!".to_string()];
		let bitvec = vec![true];
		let input_column = ColumnQualified {
			name: "input".to_string(),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(b64_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};

		let columns = Columns::new(vec![Column::ColumnQualified(input_column)]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 1,
		};
		let result = function.scalar(ctx);
		assert!(result.is_err(), "Expected error for invalid base64 input");
	}

	#[test]
	fn test_blob_b64_malformed_padding_should_error() {
		let function = BlobB64::new();

		let b64_data = vec!["SGVsbG8===".to_string()]; // Too many padding characters
		let bitvec = vec![true];
		let input_column = ColumnQualified {
			name: "input".to_string(),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(b64_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};

		let columns = Columns::new(vec![Column::ColumnQualified(input_column)]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 1,
		};
		let result = function.scalar(ctx);
		assert!(result.is_err(), "Expected error for malformed base64 padding");
	}
}
