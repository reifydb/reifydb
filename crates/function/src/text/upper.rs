// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::{column::ColumnData, container::Utf8Container};

use crate::{ScalarFunction, ScalarFunctionContext};

pub struct TextUpper;

impl TextUpper {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for TextUpper {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::Result<ColumnData> {
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.is_empty() {
			return Ok(ColumnData::utf8(Vec::<String>::new()));
		}

		let column = columns.get(0).unwrap();

		match &column.data() {
			ColumnData::Utf8 {
				container,
				max_bytes,
			} => {
				let mut result_data = Vec::with_capacity(container.data().len());
				let mut result_bitvec = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if container.is_defined(i) {
						let original_str = &container[i];
						let upper_str = original_str.to_uppercase();
						result_data.push(upper_str);
						result_bitvec.push(true);
					} else {
						result_data.push(String::new());
						result_bitvec.push(false);
					}
				}

				Ok(ColumnData::Utf8 {
					container: Utf8Container::new(result_data, result_bitvec.into()),
					max_bytes: *max_bytes,
				})
			}
			_ => unimplemented!("TextUpper only supports text input"),
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

	#[tokio::test]
	async fn test_upper_simple() {
		let function = TextUpper::new();

		let utf8_data = vec!["hello world".to_string()];
		let bitvec = vec![true];
		let input_column = Column {
			name: Fragment::internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(utf8_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};
		let columns = Columns::new(vec![input_column]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 1,
		};

		let result = function.scalar(ctx).unwrap();

		let ColumnData::Utf8 {
			container,
			..
		} = result
		else {
			panic!("Expected UTF8 column data");
		};
		assert_eq!(container.len(), 1);
		assert!(container.is_defined(0));
		assert_eq!(container[0], "HELLO WORLD");
	}

	#[tokio::test]
	async fn test_upper_mixed_case() {
		let function = TextUpper::new();

		let utf8_data = vec![
			"Hello World".to_string(),
			"MiXeD cAsE".to_string(),
			"ALREADY UPPER".to_string(),
			"lowercase".to_string(),
		];
		let bitvec = vec![true, true, true, true];
		let input_column = Column {
			name: Fragment::internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(utf8_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};
		let columns = Columns::new(vec![input_column]);
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
		assert_eq!(container.len(), 4);
		assert_eq!(container[0], "HELLO WORLD");
		assert_eq!(container[1], "MIXED CASE");
		assert_eq!(container[2], "ALREADY UPPER");
		assert_eq!(container[3], "LOWERCASE");
	}

	#[tokio::test]
	async fn test_upper_special_characters() {
		let function = TextUpper::new();

		let utf8_data = vec![
			"hello@world.com".to_string(),
			"test-123_abc".to_string(),
			"with spaces & punctuation!".to_string(),
		];
		let bitvec = vec![true, true, true];
		let input_column = Column {
			name: Fragment::internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(utf8_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};
		let columns = Columns::new(vec![input_column]);
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
		assert_eq!(container.len(), 3);
		assert_eq!(container[0], "HELLO@WORLD.COM");
		assert_eq!(container[1], "TEST-123_ABC");
		assert_eq!(container[2], "WITH SPACES & PUNCTUATION!");
	}

	#[tokio::test]
	async fn test_upper_unicode() {
		let function = TextUpper::new();

		let utf8_data = vec![
			"café naïve".to_string(),
			"straße".to_string(), // German ß
			"ñoño".to_string(),   // Spanish ñ
		];
		let bitvec = vec![true, true, true];
		let input_column = Column {
			name: Fragment::internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(utf8_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};
		let columns = Columns::new(vec![input_column]);
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
		assert_eq!(container.len(), 3);
		assert_eq!(container[0], "CAFÉ NAÏVE");
		assert_eq!(container[1], "STRASSE"); // ß becomes SS in uppercase
		assert_eq!(container[2], "ÑOÑO");
	}

	#[tokio::test]
	async fn test_upper_empty_and_whitespace() {
		let function = TextUpper::new();

		let utf8_data = vec!["".to_string(), "   ".to_string(), "\t\n\r".to_string()];
		let bitvec = vec![true, true, true];
		let input_column = Column {
			name: Fragment::internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(utf8_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};
		let columns = Columns::new(vec![input_column]);
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
		assert_eq!(container.len(), 3);
		assert_eq!(container[0], "");
		assert_eq!(container[1], "   ");
		assert_eq!(container[2], "\t\n\r");
	}

	#[tokio::test]
	async fn test_upper_with_null_data() {
		let function = TextUpper::new();

		let utf8_data = vec!["hello".to_string(), "".to_string(), "world".to_string()];
		let bitvec = vec![true, false, true];
		let input_column = Column {
			name: Fragment::internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(utf8_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};
		let columns = Columns::new(vec![input_column]);
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
		assert_eq!(container.len(), 3);
		assert!(container.is_defined(0));
		assert!(!container.is_defined(1));
		assert!(container.is_defined(2));

		assert_eq!(container[0], "HELLO");
		assert_eq!(container[2], "WORLD");
	}

	#[tokio::test]
	async fn test_upper_multibyte_characters() {
		let function = TextUpper::new();

		let utf8_data = vec![
			"日本語".to_string(),  // Japanese (no case change)
			"中文".to_string(),    // Chinese (no case change)
			"한국어".to_string(),  // Korean (no case change)
			"العربية".to_string(), // Arabic (no case change)
		];
		let bitvec = vec![true, true, true, true];
		let input_column = Column {
			name: Fragment::internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(utf8_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};
		let columns = Columns::new(vec![input_column]);
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
		assert_eq!(container.len(), 4);
		// These languages don't have case distinctions, so they remain unchanged
		assert_eq!(container[0], "日本語");
		assert_eq!(container[1], "中文");
		assert_eq!(container[2], "한국어");
		assert_eq!(container[3], "العربية");
	}

	#[tokio::test]
	async fn test_upper_emoji_and_symbols() {
		let function = TextUpper::new();

		let utf8_data =
			vec!["hello 🌍 world".to_string(), "test 💻 code".to_string(), "data 📊 analysis".to_string()];
		let bitvec = vec![true, true, true];
		let input_column = Column {
			name: Fragment::internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(utf8_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};
		let columns = Columns::new(vec![input_column]);
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
		assert_eq!(container.len(), 3);
		assert_eq!(container[0], "HELLO 🌍 WORLD");
		assert_eq!(container[1], "TEST 💻 CODE");
		assert_eq!(container[2], "DATA 📊 ANALYSIS");
	}
}
