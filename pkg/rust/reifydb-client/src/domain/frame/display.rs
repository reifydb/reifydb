// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use std::fmt::{self, Display, Formatter};

use reifydb_type::{
	ROW_NUMBER_COLUMN_NAME, Value, util::unicode::UnicodeWidthStr,
};

use crate::domain::frame::{Frame, FrameColumn};

/// Calculate the display width of a string, handling newlines properly.
/// For strings with newlines, returns the width of the longest line.
/// For strings without newlines, returns the unicode display width.
fn display_width(s: &str) -> usize {
	if s.contains('\n') {
		s.lines().map(|line| line.width()).max().unwrap_or(0)
	} else {
		s.width()
	}
}

/// Escape newlines and tabs in a string for single-line display.
/// Replaces '\n' with "\\n" and '\t' with "\\t".
fn escape_control_chars(s: &str) -> String {
	s.replace('\n', "\\n").replace('\t', "\\t")
}

/// Create a column display order that puts RowNumber column first if it exists
fn get_column_display_order(frame: &Frame) -> Vec<usize> {
	let mut indices: Vec<usize> = (0..frame.len()).collect();

	// Find the RowNumber column and move it to the front
	if let Some(row_number_pos) =
		frame.iter().position(|col| col.name == ROW_NUMBER_COLUMN_NAME)
	{
		indices.remove(row_number_pos);
		indices.insert(0, row_number_pos);
	}

	indices
}

/// Extract string value from column at given row index, with proper escaping
fn extract_string_value(col: &FrameColumn, row_number: usize) -> String {
	let s =
		col.data.get(row_number)
			.unwrap_or(&Value::Undefined)
			.as_string();

	escape_control_chars(&s)
}

impl Display for Frame {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		let row_count = self.first().map_or(0, |c| c.data.len());
		let col_count = self.len();

		// Get the display order with RowNumber column first
		let column_order = get_column_display_order(self);

		let mut col_widths = vec![0; col_count];

		for (display_idx, &col_idx) in column_order.iter().enumerate() {
			let col = &self[col_idx];
			let display_name =
				escape_control_chars(&col.qualified_name());
			col_widths[display_idx] = display_width(&display_name);
		}

		for row_numberx in 0..row_count {
			for (display_idx, &col_idx) in
				column_order.iter().enumerate()
			{
				let col = &self[col_idx];
				let s = extract_string_value(col, row_numberx);
				col_widths[display_idx] = col_widths
					[display_idx]
					.max(display_width(&s));
			}
		}

		// Add padding
		for w in &mut col_widths {
			*w += 2;
		}

		let sep = format!(
			"+{}+",
			col_widths
				.iter()
				.map(|w| "-".repeat(*w + 2))
				.collect::<Vec<_>>()
				.join("+")
		);
		writeln!(f, "{}", sep)?;

		let header = column_order
			.iter()
			.enumerate()
			.map(|(display_idx, &col_idx)| {
				let col = &self[col_idx];
				let w = col_widths[display_idx];
				let name = escape_control_chars(
					&col.qualified_name(),
				);
				let pad = w - display_width(&name);
				let l = pad / 2;
				let r = pad - l;
				format!(
					" {:left$}{}{:right$} ",
					"",
					name,
					"",
					left = l,
					right = r
				)
			})
			.collect::<Vec<_>>();
		writeln!(f, "|{}|", header.join("|"))?;

		writeln!(f, "{}", sep)?;

		for row_numberx in 0..row_count {
			let row = column_order
				.iter()
				.enumerate()
				.map(|(display_idx, &col_idx)| {
					let col = &self[col_idx];
					let w = col_widths[display_idx];
					let s = extract_string_value(
						col,
						row_numberx,
					);
					let pad = w - display_width(&s);
					let l = pad / 2;
					let r = pad - l;
					format!(
						" {:left$}{}{:right$} ",
						"",
						s,
						"",
						left = l,
						right = r
					)
				})
				.collect::<Vec<_>>();

			writeln!(f, "|{}|", row.join("|"))?;
		}

		writeln!(f, "{}", sep)
	}
}

#[cfg(test)]
mod tests {
	use std::convert::TryFrom;

	use bitvec::vec::BitVec;
	use reifydb_type::{
		Blob, Date, DateTime, Interval, OrderedF32, OrderedF64,
		RowNumber, Time, Type, Uuid4, Uuid7, Value,
	};

	use super::*;

	// Helper functions to create test columns with defined/undefined values
	fn bool_column_with_bitvec(
		name: &str,
		data: impl IntoIterator<Item = bool>,
		bitvec: impl IntoIterator<Item = bool>,
	) -> FrameColumn {
		let values: Vec<bool> = data.into_iter().collect();
		let defined: Vec<bool> = bitvec.into_iter().collect();
		let mut result_data = Vec::new();

		for (i, &is_defined) in defined.iter().enumerate() {
			if is_defined {
				result_data.push(Value::Bool(values[i]));
			} else {
				result_data.push(Value::Undefined);
			}
		}

		FrameColumn {
			schema: None,
			store: None,
			name: name.to_string(),
			r#type: Type::Bool,
			data: result_data,
		}
	}

	fn float4_column_with_bitvec(
		name: &str,
		data: impl IntoIterator<Item = f32>,
		bitvec: impl IntoIterator<Item = bool>,
	) -> FrameColumn {
		let values: Vec<f32> = data.into_iter().collect();
		let defined: Vec<bool> = bitvec.into_iter().collect();
		let mut result_data = Vec::new();

		for (i, &is_defined) in defined.iter().enumerate() {
			if is_defined {
				result_data.push(Value::Float4(
					OrderedF32::try_from(values[i])
						.unwrap(),
				));
			} else {
				result_data.push(Value::Undefined);
			}
		}

		FrameColumn {
			schema: None,
			store: None,
			name: name.to_string(),
			r#type: Type::Float4,
			data: result_data,
		}
	}

	fn float8_column_with_bitvec(
		name: &str,
		data: impl IntoIterator<Item = f64>,
		bitvec: impl IntoIterator<Item = bool>,
	) -> FrameColumn {
		let values: Vec<f64> = data.into_iter().collect();
		let defined: Vec<bool> = bitvec.into_iter().collect();
		let mut result_data = Vec::new();

		for (i, &is_defined) in defined.iter().enumerate() {
			if is_defined {
				result_data.push(Value::Float8(
					OrderedF64::try_from(values[i])
						.unwrap(),
				));
			} else {
				result_data.push(Value::Undefined);
			}
		}

		FrameColumn {
			schema: None,
			store: None,
			name: name.to_string(),
			r#type: Type::Float8,
			data: result_data,
		}
	}

	fn int1_column_with_bitvec(
		name: &str,
		data: impl IntoIterator<Item = i8>,
		bitvec: impl IntoIterator<Item = bool>,
	) -> FrameColumn {
		let values: Vec<i8> = data.into_iter().collect();
		let defined: Vec<bool> = bitvec.into_iter().collect();
		let mut result_data = Vec::new();

		for (i, &is_defined) in defined.iter().enumerate() {
			if is_defined {
				result_data.push(Value::Int1(values[i]));
			} else {
				result_data.push(Value::Undefined);
			}
		}

		FrameColumn {
			schema: None,
			store: None,
			name: name.to_string(),
			r#type: Type::Int1,
			data: result_data,
		}
	}

	fn int2_column_with_bitvec(
		name: &str,
		data: impl IntoIterator<Item = i16>,
		bitvec: impl IntoIterator<Item = bool>,
	) -> FrameColumn {
		let values: Vec<i16> = data.into_iter().collect();
		let defined: Vec<bool> = bitvec.into_iter().collect();
		let mut result_data = Vec::new();

		for (i, &is_defined) in defined.iter().enumerate() {
			if is_defined {
				result_data.push(Value::Int2(values[i]));
			} else {
				result_data.push(Value::Undefined);
			}
		}

		FrameColumn {
			schema: None,
			store: None,
			name: name.to_string(),
			r#type: Type::Int2,
			data: result_data,
		}
	}

	fn int4_column_with_bitvec(
		name: &str,
		data: impl IntoIterator<Item = i32>,
		bitvec: impl IntoIterator<Item = bool>,
	) -> FrameColumn {
		let values: Vec<i32> = data.into_iter().collect();
		let defined: Vec<bool> = bitvec.into_iter().collect();
		let mut result_data = Vec::new();

		for (i, &is_defined) in defined.iter().enumerate() {
			if is_defined {
				result_data.push(Value::Int4(values[i]));
			} else {
				result_data.push(Value::Undefined);
			}
		}

		FrameColumn {
			schema: None,
			store: None,
			name: name.to_string(),
			r#type: Type::Int4,
			data: result_data,
		}
	}

	fn int8_column_with_bitvec(
		name: &str,
		data: impl IntoIterator<Item = i64>,
		bitvec: impl IntoIterator<Item = bool>,
	) -> FrameColumn {
		let values: Vec<i64> = data.into_iter().collect();
		let defined: Vec<bool> = bitvec.into_iter().collect();
		let mut result_data = Vec::new();

		for (i, &is_defined) in defined.iter().enumerate() {
			if is_defined {
				result_data.push(Value::Int8(values[i]));
			} else {
				result_data.push(Value::Undefined);
			}
		}

		FrameColumn {
			schema: None,
			store: None,
			name: name.to_string(),
			r#type: Type::Int8,
			data: result_data,
		}
	}

	fn int16_column_with_bitvec(
		name: &str,
		data: impl IntoIterator<Item = i128>,
		bitvec: impl IntoIterator<Item = bool>,
	) -> FrameColumn {
		let values: Vec<i128> = data.into_iter().collect();
		let defined: Vec<bool> = bitvec.into_iter().collect();
		let mut result_data = Vec::new();

		for (i, &is_defined) in defined.iter().enumerate() {
			if is_defined {
				result_data.push(Value::Int16(values[i]));
			} else {
				result_data.push(Value::Undefined);
			}
		}

		FrameColumn {
			schema: None,
			store: None,
			name: name.to_string(),
			r#type: Type::Int16,
			data: result_data,
		}
	}

	fn uint1_column_with_bitvec(
		name: &str,
		data: impl IntoIterator<Item = u8>,
		bitvec: impl IntoIterator<Item = bool>,
	) -> FrameColumn {
		let values: Vec<u8> = data.into_iter().collect();
		let defined: Vec<bool> = bitvec.into_iter().collect();
		let mut result_data = Vec::new();

		for (i, &is_defined) in defined.iter().enumerate() {
			if is_defined {
				result_data.push(Value::Uint1(values[i]));
			} else {
				result_data.push(Value::Undefined);
			}
		}

		FrameColumn {
			schema: None,
			store: None,
			name: name.to_string(),
			r#type: Type::Uint1,
			data: result_data,
		}
	}

	fn uint2_column_with_bitvec(
		name: &str,
		data: impl IntoIterator<Item = u16>,
		bitvec: impl IntoIterator<Item = bool>,
	) -> FrameColumn {
		let values: Vec<u16> = data.into_iter().collect();
		let defined: Vec<bool> = bitvec.into_iter().collect();
		let mut result_data = Vec::new();

		for (i, &is_defined) in defined.iter().enumerate() {
			if is_defined {
				result_data.push(Value::Uint2(values[i]));
			} else {
				result_data.push(Value::Undefined);
			}
		}

		FrameColumn {
			schema: None,
			store: None,
			name: name.to_string(),
			r#type: Type::Uint2,
			data: result_data,
		}
	}

	fn uint4_column_with_bitvec(
		name: &str,
		data: impl IntoIterator<Item = u32>,
		bitvec: impl IntoIterator<Item = bool>,
	) -> FrameColumn {
		let values: Vec<u32> = data.into_iter().collect();
		let defined: Vec<bool> = bitvec.into_iter().collect();
		let mut result_data = Vec::new();

		for (i, &is_defined) in defined.iter().enumerate() {
			if is_defined {
				result_data.push(Value::Uint4(values[i]));
			} else {
				result_data.push(Value::Undefined);
			}
		}

		FrameColumn {
			schema: None,
			store: None,
			name: name.to_string(),
			r#type: Type::Uint4,
			data: result_data,
		}
	}

	fn uint8_column_with_bitvec(
		name: &str,
		data: impl IntoIterator<Item = u64>,
		bitvec: impl IntoIterator<Item = bool>,
	) -> FrameColumn {
		let values: Vec<u64> = data.into_iter().collect();
		let defined: Vec<bool> = bitvec.into_iter().collect();
		let mut result_data = Vec::new();

		for (i, &is_defined) in defined.iter().enumerate() {
			if is_defined {
				result_data.push(Value::Uint8(values[i]));
			} else {
				result_data.push(Value::Undefined);
			}
		}

		FrameColumn {
			schema: None,
			store: None,
			name: name.to_string(),
			r#type: Type::Uint8,
			data: result_data,
		}
	}

	fn uint16_column_with_bitvec(
		name: &str,
		data: impl IntoIterator<Item = u128>,
		bitvec: impl IntoIterator<Item = bool>,
	) -> FrameColumn {
		let values: Vec<u128> = data.into_iter().collect();
		let defined: Vec<bool> = bitvec.into_iter().collect();
		let mut result_data = Vec::new();

		for (i, &is_defined) in defined.iter().enumerate() {
			if is_defined {
				result_data.push(Value::Uint16(values[i]));
			} else {
				result_data.push(Value::Undefined);
			}
		}

		FrameColumn {
			schema: None,
			store: None,
			name: name.to_string(),
			r#type: Type::Uint16,
			data: result_data,
		}
	}

	fn utf8_column_with_bitvec(
		name: &str,
		data: Vec<String>,
		bitvec: impl IntoIterator<Item = bool>,
	) -> FrameColumn {
		let defined: Vec<bool> = bitvec.into_iter().collect();
		let mut result_data = Vec::new();

		for (i, &is_defined) in defined.iter().enumerate() {
			if is_defined {
				result_data.push(Value::Utf8(data[i].clone()));
			} else {
				result_data.push(Value::Undefined);
			}
		}

		FrameColumn {
			schema: None,
			store: None,
			name: name.to_string(),
			r#type: Type::Utf8,
			data: result_data,
		}
	}

	fn undefined_column(name: &str, count: usize) -> FrameColumn {
		FrameColumn {
			schema: None,
			store: None,
			name: name.to_string(),
			r#type: Type::Undefined,
			data: vec![Value::Undefined; count],
		}
	}

	fn date_column_with_bitvec(
		name: &str,
		data: Vec<Date>,
		bitvec: BitVec,
	) -> FrameColumn {
		let mut result_data = Vec::new();

		for (i, is_defined) in bitvec.iter().enumerate() {
			if *is_defined {
				result_data.push(Value::Date(data[i]));
			} else {
				result_data.push(Value::Undefined);
			}
		}

		FrameColumn {
			schema: None,
			store: None,
			name: name.to_string(),
			r#type: Type::Date,
			data: result_data,
		}
	}

	fn datetime_column_with_bitvec(
		name: &str,
		data: Vec<DateTime>,
		bitvec: BitVec,
	) -> FrameColumn {
		let mut result_data = Vec::new();

		for (i, is_defined) in bitvec.iter().enumerate() {
			if *is_defined {
				result_data.push(Value::DateTime(data[i]));
			} else {
				result_data.push(Value::Undefined);
			}
		}

		FrameColumn {
			schema: None,
			store: None,
			name: name.to_string(),
			r#type: Type::DateTime,
			data: result_data,
		}
	}

	fn time_column_with_bitvec(
		name: &str,
		data: Vec<Time>,
		bitvec: BitVec,
	) -> FrameColumn {
		let mut result_data = Vec::new();

		for (i, is_defined) in bitvec.iter().enumerate() {
			if *is_defined {
				result_data.push(Value::Time(data[i]));
			} else {
				result_data.push(Value::Undefined);
			}
		}

		FrameColumn {
			schema: None,
			store: None,
			name: name.to_string(),
			r#type: Type::Time,
			data: result_data,
		}
	}

	fn interval_column_with_bitvec(
		name: &str,
		data: Vec<Interval>,
		bitvec: BitVec,
	) -> FrameColumn {
		let mut result_data = Vec::new();

		for (i, is_defined) in bitvec.iter().enumerate() {
			if *is_defined {
				result_data.push(Value::Interval(data[i]));
			} else {
				result_data.push(Value::Undefined);
			}
		}

		FrameColumn {
			schema: None,
			store: None,
			name: name.to_string(),
			r#type: Type::Interval,
			data: result_data,
		}
	}

	fn row_number_column(
		data: impl IntoIterator<Item = RowNumber>,
	) -> FrameColumn {
		let values: Vec<Value> =
			data.into_iter().map(Value::RowNumber).collect();

		FrameColumn {
			schema: None,
			store: None,
			name: ROW_NUMBER_COLUMN_NAME.to_string(),
			r#type: Type::RowNumber,
			data: values,
		}
	}

	fn row_number_column_with_bitvec(
		data: impl IntoIterator<Item = RowNumber>,
		bitvec: BitVec,
	) -> FrameColumn {
		let values: Vec<RowNumber> = data.into_iter().collect();
		let mut result_data = Vec::new();

		for (i, is_defined) in bitvec.iter().enumerate() {
			if *is_defined {
				result_data.push(Value::RowNumber(values[i]));
			} else {
				result_data.push(Value::Undefined);
			}
		}

		FrameColumn {
			schema: None,
			store: None,
			name: ROW_NUMBER_COLUMN_NAME.to_string(),
			r#type: Type::RowNumber,
			data: result_data,
		}
	}

	fn blob_column_with_bitvec(
		name: &str,
		data: Vec<Blob>,
		bitvec: impl IntoIterator<Item = bool>,
	) -> FrameColumn {
		let defined: Vec<bool> = bitvec.into_iter().collect();
		let mut result_data = Vec::new();

		for (i, &is_defined) in defined.iter().enumerate() {
			if is_defined {
				result_data.push(Value::Blob(data[i].clone()));
			} else {
				result_data.push(Value::Undefined);
			}
		}

		FrameColumn {
			schema: None,
			store: None,
			name: name.to_string(),
			r#type: Type::Blob,
			data: result_data,
		}
	}

	fn uuid4_column_with_bitvec(
		name: &str,
		data: Vec<Uuid4>,
		bitvec: impl IntoIterator<Item = bool>,
	) -> FrameColumn {
		let defined: Vec<bool> = bitvec.into_iter().collect();
		let mut result_data = Vec::new();

		for (i, &is_defined) in defined.iter().enumerate() {
			if is_defined {
				result_data.push(Value::Uuid4(data[i]));
			} else {
				result_data.push(Value::Undefined);
			}
		}

		FrameColumn {
			schema: None,
			store: None,
			name: name.to_string(),
			r#type: Type::Uuid4,
			data: result_data,
		}
	}

	fn uuid7_column_with_bitvec(
		name: &str,
		data: Vec<Uuid7>,
		bitvec: impl IntoIterator<Item = bool>,
	) -> FrameColumn {
		let defined: Vec<bool> = bitvec.into_iter().collect();
		let mut result_data = Vec::new();

		for (i, &is_defined) in defined.iter().enumerate() {
			if is_defined {
				result_data.push(Value::Uuid7(data[i]));
			} else {
				result_data.push(Value::Undefined);
			}
		}

		FrameColumn {
			schema: None,
			store: None,
			name: name.to_string(),
			r#type: Type::Uuid7,
			data: result_data,
		}
	}

	#[test]
	fn test_bool() {
		let frame = Frame::new(vec![bool_column_with_bitvec(
			"bool",
			[true, false],
			[true, false],
		)]);
		let output = format!("{}", frame);
		let expected = "\
+-------------+
|    bool     |
+-------------+
|    true     |
|  Undefined  |
+-------------+
";
		assert_eq!(output, expected);
	}

	#[test]
	fn test_float4() {
		let frame = Frame::new(vec![float4_column_with_bitvec(
			"float4",
			[1.2, 2.5],
			[true, false],
		)]);
		let output = format!("{}", frame);
		let expected = "\
+-------------+
|   float4    |
+-------------+
|     1.2     |
|  Undefined  |
+-------------+
";
		assert_eq!(output, expected);
	}

	#[test]
	#[allow(clippy::approx_constant)]
	fn test_float8() {
		let frame = Frame::new(vec![float8_column_with_bitvec(
			"float8",
			[3.14, 6.28],
			[true, false],
		)]);
		let output = format!("{}", frame);
		let expected = "\
+-------------+
|   float8    |
+-------------+
|    3.14     |
|  Undefined  |
+-------------+
";
		assert_eq!(output, expected);
	}

	#[test]
	fn test_int1() {
		let frame = Frame::new(vec![int1_column_with_bitvec(
			"int1",
			[1, -1],
			[true, false],
		)]);
		let output = format!("{}", frame);
		let expected = "\
+-------------+
|    int1     |
+-------------+
|      1      |
|  Undefined  |
+-------------+
";
		assert_eq!(output, expected);
	}

	#[test]
	fn test_int2() {
		let frame = Frame::new(vec![int2_column_with_bitvec(
			"int2",
			[100, 200],
			[true, false],
		)]);
		let output = format!("{}", frame);
		let expected = "\
+-------------+
|    int2     |
+-------------+
|     100     |
|  Undefined  |
+-------------+
";
		assert_eq!(output, expected);
	}

	#[test]
	fn test_int4() {
		let frame = Frame::new(vec![int4_column_with_bitvec(
			"int4",
			[1000, 2000],
			[true, false],
		)]);
		let output = format!("{}", frame);
		let expected = "\
+-------------+
|    int4     |
+-------------+
|    1000     |
|  Undefined  |
+-------------+
";
		assert_eq!(output, expected);
	}

	#[test]
	fn test_int8() {
		let frame = Frame::new(vec![int8_column_with_bitvec(
			"int8",
			[10000, 20000],
			[true, false],
		)]);
		let output = format!("{}", frame);
		let expected = "\
+-------------+
|    int8     |
+-------------+
|    10000    |
|  Undefined  |
+-------------+
";
		assert_eq!(output, expected);
	}

	#[test]
	fn test_int16() {
		let frame = Frame::new(vec![int16_column_with_bitvec(
			"int16",
			[100000, 200000],
			[true, false],
		)]);
		let output = format!("{}", frame);
		let expected = "\
+-------------+
|    int16    |
+-------------+
|   100000    |
|  Undefined  |
+-------------+
";
		assert_eq!(output, expected);
	}

	#[test]
	fn test_uint1() {
		let frame = Frame::new(vec![uint1_column_with_bitvec(
			"uint1",
			[1, 2],
			[true, false],
		)]);
		let output = format!("{}", frame);
		let expected = "\
+-------------+
|    uint1    |
+-------------+
|      1      |
|  Undefined  |
+-------------+
";
		assert_eq!(output, expected);
	}

	#[test]
	fn test_uint2() {
		let frame = Frame::new(vec![uint2_column_with_bitvec(
			"uint2",
			[100, 200],
			[true, false],
		)]);
		let output = format!("{}", frame);
		let expected = "\
+-------------+
|    uint2    |
+-------------+
|     100     |
|  Undefined  |
+-------------+
";
		assert_eq!(output, expected);
	}

	#[test]
	fn test_uint4() {
		let frame = Frame::new(vec![uint4_column_with_bitvec(
			"uint4",
			[1000, 2000],
			[true, false],
		)]);
		let output = format!("{}", frame);
		let expected = "\
+-------------+
|    uint4    |
+-------------+
|    1000     |
|  Undefined  |
+-------------+
";
		assert_eq!(output, expected);
	}

	#[test]
	fn test_uint8() {
		let frame = Frame::new(vec![uint8_column_with_bitvec(
			"uint8",
			[10000, 20000],
			[true, false],
		)]);
		let output = format!("{}", frame);
		let expected = "\
+-------------+
|    uint8    |
+-------------+
|    10000    |
|  Undefined  |
+-------------+
";
		assert_eq!(output, expected);
	}

	#[test]
	fn test_uint16() {
		let frame = Frame::new(vec![uint16_column_with_bitvec(
			"uint16",
			[100000, 200000],
			[true, false],
		)]);
		let output = format!("{}", frame);
		let expected = "\
+-------------+
|   uint16    |
+-------------+
|   100000    |
|  Undefined  |
+-------------+
";
		assert_eq!(output, expected);
	}

	#[test]
	fn test_string() {
		let frame = Frame::new(vec![utf8_column_with_bitvec(
			"string",
			vec!["foo".to_string(), "bar".to_string()],
			[true, false],
		)]);
		let output = format!("{}", frame);
		let expected = "\
+-------------+
|   string    |
+-------------+
|     foo     |
|  Undefined  |
+-------------+
";
		assert_eq!(output, expected);
	}

	#[test]
	fn test_undefined() {
		let frame = Frame::new(vec![undefined_column("undefined", 2)]);
		let output = format!("{}", frame);
		let expected = "\
+-------------+
|  undefined  |
+-------------+
|  Undefined  |
|  Undefined  |
+-------------+
";
		assert_eq!(output, expected);
	}

	#[test]
	fn test_date() {
		let dates = vec![
			Date::from_ymd(2025, 1, 15).unwrap(),
			Date::from_ymd(2025, 12, 25).unwrap(),
		];
		let frame = Frame::new(vec![date_column_with_bitvec(
			"date",
			dates,
			{
				let mut bv = BitVec::new();
				bv.push(true);
				bv.push(false);
				bv
			},
		)]);
		let output = format!("{}", frame);
		let expected = "\
+--------------+
|     date     |
+--------------+
|  2025-01-15  |
|  Undefined   |
+--------------+
";
		assert_eq!(output, expected);
	}

	#[test]
	fn test_datetime() {
		let datetimes = vec![
			DateTime::from_timestamp(1642694400).unwrap(),
			DateTime::from_timestamp(1735142400).unwrap(),
		];
		let frame = Frame::new(vec![datetime_column_with_bitvec(
			"datetime",
			datetimes,
			{
				let mut bv = BitVec::new();
				bv.push(true);
				bv.push(false);
				bv
			},
		)]);
		let output = format!("{}", frame);
		let expected = "\
+----------------------------------+
|             datetime             |
+----------------------------------+
|  2022-01-20T16:00:00.000000000Z  |
|            Undefined             |
+----------------------------------+
";
		assert_eq!(output, expected);
	}

	#[test]
	fn test_time() {
		let times = vec![
			Time::from_hms(14, 30, 45).unwrap(),
			Time::from_hms(9, 15, 30).unwrap(),
		];
		let frame = Frame::new(vec![time_column_with_bitvec(
			"time",
			times,
			{
				let mut bv = BitVec::new();
				bv.push(true);
				bv.push(false);
				bv
			},
		)]);
		let output = format!("{}", frame);
		let expected = "\
+----------------------+
|         time         |
+----------------------+
|  14:30:45.000000000  |
|      Undefined       |
+----------------------+
";
		assert_eq!(output, expected);
	}

	#[test]
	fn test_interval() {
		let intervals =
			vec![Interval::from_days(30), Interval::from_hours(24)];
		let frame = Frame::new(vec![interval_column_with_bitvec(
			"interval",
			intervals,
			{
				let mut bv = BitVec::new();
				bv.push(true);
				bv.push(false);
				bv
			},
		)]);
		let output = format!("{}", frame);

		let expected = "\
+-------------+
|  interval   |
+-------------+
|    P30D     |
|  Undefined  |
+-------------+
";
		assert_eq!(output, expected);
	}

	#[test]
	fn test_row_number() {
		let ids = vec![RowNumber(1234), RowNumber(5678)];
		let frame =
			Frame::new(vec![row_number_column_with_bitvec(ids, {
				let mut bv = BitVec::new();
				bv.push(true);
				bv.push(false);
				bv
			})]);
		let output = format!("{}", frame);
		let expected = "\
+-------------------+
|  __ROW__NUMBER__  |
+-------------------+
|       1234        |
|     Undefined     |
+-------------------+
";
		assert_eq!(output, expected);
	}

	#[test]
	fn test_row_number_column_ordering() {
		// Create a frame with regular columns and a RowNumber column
		let regular_column = utf8_column_with_bitvec(
			"name",
			vec!["Alice".to_string(), "Bob".to_string()],
			[true, true],
		);

		let age_column =
			int4_column_with_bitvec("age", [25, 30], [true, true]);

		let row_number_column = row_number_column([
			RowNumber::new(1),
			RowNumber::new(2),
		]);

		// Create frame with RowNumber column NOT first (it should be
		// reordered)
		let frame = Frame::new(vec![
			regular_column,
			age_column,
			row_number_column,
		]);
		let output = format!("{}", frame);

		// Verify that __ROW__NUMBER__ appears as the first column in
		// the output
		let lines: Vec<&str> = output.lines().collect();
		let header_line = lines[1]; // Second line contains the header

		assert!(header_line.contains("__ROW__NUMBER__"));

		// Check that the first data value in the first row is from the
		// RowNumber column
		let first_data_line = lines[3]; // Fourth line contains first data row
		assert!(first_data_line.contains("1")); // First RowNumber value
	}

	#[test]
	fn test_row_number_undefined_display() {
		// Create a RowNumber column with one undefined value
		let row_number_column = row_number_column_with_bitvec(
			[RowNumber::new(1), RowNumber::new(2)],
			{
				let mut bv = BitVec::new();
				bv.push(true);
				bv.push(false);
				bv
			}, /* Second value
			    * is undefined */
		);

		let frame = Frame::new(vec![row_number_column]);
		let output = format!("{}", frame);

		// Verify that undefined RowNumber displays as "Undefined"
		let lines: Vec<&str> = output.lines().collect();
		let first_data_line = lines[3]; // First data row
		let second_data_line = lines[4]; // Second data row

		assert!(first_data_line.contains("1")); // First RowNumber value
		assert!(second_data_line.contains("Undefined")); // Second value should be undefined
	}

	#[test]
	fn test_blob() {
		let blobs = vec![
			Blob::new(vec![0x01, 0x02, 0x03]),
			Blob::new(vec![0xFF, 0xEE, 0xDD]),
		];
		let frame = Frame::new(vec![blob_column_with_bitvec(
			"blob",
			blobs,
			[true, false],
		)]);
		let output = format!("{}", frame);
		let expected = "\
+-------------+
|    blob     |
+-------------+
|  0x010203   |
|  Undefined  |
+-------------+
";
		assert_eq!(output, expected);
	}

	#[test]
	fn test_uuid4() {
		let uuids = vec![
			Uuid4::from(
				::uuid::Uuid::parse_str(
					"550e8400-e29b-41d4-a716-446655440000",
				)
				.unwrap(),
			),
			Uuid4::from(
				::uuid::Uuid::parse_str(
					"550e8400-e29b-41d4-a716-446655440001",
				)
				.unwrap(),
			),
		];
		let frame = Frame::new(vec![uuid4_column_with_bitvec(
			"uuid4",
			uuids,
			[true, false],
		)]);
		let output = format!("{}", frame);
		let expected = "\
+----------------------------------------+
|                 uuid4                  |
+----------------------------------------+
|  550e8400-e29b-41d4-a716-446655440000  |
|               Undefined                |
+----------------------------------------+
";
		assert_eq!(output, expected);
	}

	#[test]
	fn test_uuid7() {
		let uuids = vec![
			Uuid7::from(
				::uuid::Uuid::parse_str(
					"01890a5d-ac96-774b-b9aa-789c0686aaa4",
				)
				.unwrap(),
			),
			Uuid7::from(
				::uuid::Uuid::parse_str(
					"01890a5d-ac96-774b-b9aa-789c0686aaa5",
				)
				.unwrap(),
			),
		];
		let frame = Frame::new(vec![uuid7_column_with_bitvec(
			"uuid7",
			uuids,
			[true, false],
		)]);
		let output = format!("{}", frame);
		let expected = "\
+----------------------------------------+
|                 uuid7                  |
+----------------------------------------+
|  01890a5d-ac96-774b-b9aa-789c0686aaa4  |
|               Undefined                |
+----------------------------------------+
";
		assert_eq!(output, expected);
	}
}
