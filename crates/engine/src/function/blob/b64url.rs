// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::value::column::ColumnData;
use reifydb_type::{Fragment, value::Blob};

use crate::function::{ScalarFunction, ScalarFunctionContext};

pub struct BlobB64url;

impl BlobB64url {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for BlobB64url {
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
						let b64url_str = &container[i];
						let blob = Blob::from_b64url(Fragment::internal(b64url_str))?;
						result_data.push(blob);
					} else {
						result_data.push(Blob::empty())
					}
				}

				Ok(ColumnData::blob_with_bitvec(result_data, container.bitvec().clone()))
			}
			_ => unimplemented!("BlobB64url only supports text input"),
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
	use crate::function::ScalarFunctionContext;

	#[tokio::test]
	async fn test_blob_b64url_valid_input() {
		let function = BlobB64url::new();

		// "Hello!" in base64url is "SGVsbG8h" (no padding needed)
		let b64url_data = vec!["SGVsbG8h".to_string()];
		let bitvec = vec![true];
		let input_column = Column {
			name: Fragment::internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(b64url_data, bitvec.into()),
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
		assert_eq!(container[0].as_bytes(), "Hello!".as_bytes());
	}

	#[tokio::test]
	async fn test_blob_b64url_empty_string() {
		let function = BlobB64url::new();

		let b64url_data = vec!["".to_string()];
		let bitvec = vec![true];
		let input_column = Column {
			name: Fragment::internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(b64url_data, bitvec.into()),
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
	async fn test_blob_b64url_url_safe_characters() {
		let function = BlobB64url::new();

		// Base64url uses - and _ instead of + and /
		// This string contains URL-safe characters
		let b64url_data = vec!["SGVsbG9fV29ybGQtSGVsbG8".to_string()];
		let bitvec = vec![true];
		let input_column = Column {
			name: Fragment::internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(b64url_data, bitvec.into()),
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
		assert_eq!(container[0].as_bytes(), "Hello_World-Hello".as_bytes());
	}

	#[tokio::test]
	async fn test_blob_b64url_no_padding() {
		let function = BlobB64url::new();

		// Base64url typically omits padding characters
		// "Hello" in base64url without padding is "SGVsbG8"
		let b64url_data = vec!["SGVsbG8".to_string()];
		let bitvec = vec![true];
		let input_column = Column {
			name: Fragment::internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(b64url_data, bitvec.into()),
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
		assert_eq!(container[0].as_bytes(), "Hello".as_bytes());
	}

	#[tokio::test]
	async fn test_blob_b64url_multiple_rows() {
		let function = BlobB64url::new();

		// "A" = "QQ", "BC" = "QkM", "DEF" = "REVG" (no padding in
		// base64url)
		let b64url_data = vec!["QQ".to_string(), "QkM".to_string(), "REVG".to_string()];
		let bitvec = vec![true, true, true];
		let input_column = Column {
			name: Fragment::internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(b64url_data, bitvec.into()),
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

		assert_eq!(container[0].as_bytes(), "A".as_bytes());
		assert_eq!(container[1].as_bytes(), "BC".as_bytes());
		assert_eq!(container[2].as_bytes(), "DEF".as_bytes());
	}

	#[tokio::test]
	async fn test_blob_b64url_with_null_data() {
		let function = BlobB64url::new();

		let b64url_data = vec!["QQ".to_string(), "".to_string(), "REVG".to_string()];
		let bitvec = vec![true, false, true];
		let input_column = Column {
			name: Fragment::internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(b64url_data, bitvec.into()),
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

		assert_eq!(container[0].as_bytes(), "A".as_bytes());
		assert_eq!(container[1].as_bytes(), [].as_slice() as &[u8]);
		assert_eq!(container[2].as_bytes(), "DEF".as_bytes());
	}

	#[tokio::test]
	async fn test_blob_b64url_binary_data() {
		let function = BlobB64url::new();

		// Binary data: [0xde, 0xad, 0xbe, 0xef] in base64url is
		// "3q2-7w" (no padding)
		let b64url_data = vec!["3q2-7w".to_string()];
		let bitvec = vec![true];
		let input_column = Column {
			name: Fragment::internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(b64url_data, bitvec.into()),
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
	async fn test_blob_b64url_invalid_input_should_error() {
		let function = BlobB64url::new();

		// Using standard base64 characters that are invalid in
		// base64url
		let b64url_data = vec!["invalid+base64/chars".to_string()];
		let bitvec = vec![true];
		let input_column = Column {
			name: Fragment::internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(b64url_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};

		let columns = Columns::new(vec![input_column]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 1,
		};
		let result = function.scalar(ctx);
		assert!(result.is_err(), "Expected error for invalid base64url input");
	}

	#[tokio::test]
	async fn test_blob_b64url_with_standard_base64_padding_should_error() {
		let function = BlobB64url::new();

		// Base64url typically doesn't use padding, so this should error
		let b64url_data = vec!["SGVsbG8=".to_string()];
		let bitvec = vec![true];
		let input_column = Column {
			name: Fragment::internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(b64url_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};

		let columns = Columns::new(vec![input_column]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 1,
		};
		let result = function.scalar(ctx);
		assert!(result.is_err(), "Expected error for base64url with padding characters");
	}
}
