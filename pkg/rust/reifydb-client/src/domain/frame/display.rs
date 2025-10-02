// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use std::fmt::{self, Display, Formatter};

use reifydb_type::{Value, util::unicode::UnicodeWidthStr};

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

/// Create a column display order (no special handling needed since encoded numbers are separate)
fn get_column_display_order(frame: &Frame) -> Vec<usize> {
	(0..frame.len()).collect()
}

/// Extract string value from column at given encoded index, with proper escaping
fn extract_string_value(col: &FrameColumn, row_number: usize) -> String {
	let s = col.data.get(row_number).unwrap_or(&Value::Undefined).as_string();

	escape_control_chars(&s)
}

impl Display for Frame {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		let row_count = self.first().map_or(0, |c| c.data.len());
		let has_row_numbers = !self.row_numbers.is_empty();
		let col_count = self.len()
			+ if has_row_numbers {
				1
			} else {
				0
			};

		// Get the display order for regular columns
		let column_order = get_column_display_order(self);

		let mut col_widths = vec![0; col_count];

		// If we have encoded numbers, calculate width for encoded number column
		let row_num_col_idx = if has_row_numbers {
			// Row number column is always first
			let row_num_header = "__ROW__NUMBER__";
			col_widths[0] = display_width(row_num_header);

			// Calculate max width needed for encoded numbers
			for row_num in &self.row_numbers {
				let s = row_num.to_string();
				col_widths[0] = col_widths[0].max(display_width(&s));
			}
			1 // Start regular columns at index 1
		} else {
			0 // Start regular columns at index 0
		};

		for (display_idx, &col_idx) in column_order.iter().enumerate() {
			let col = &self[col_idx];
			let display_name = escape_control_chars(&col.qualified_name());
			col_widths[row_num_col_idx + display_idx] = display_width(&display_name);
		}

		for row_numberx in 0..row_count {
			for (display_idx, &col_idx) in column_order.iter().enumerate() {
				let col = &self[col_idx];
				let s = extract_string_value(col, row_numberx);
				col_widths[row_num_col_idx + display_idx] =
					col_widths[row_num_col_idx + display_idx].max(display_width(&s));
			}
		}

		// Add padding
		for w in &mut col_widths {
			*w += 2;
		}

		let sep = format!("+{}+", col_widths.iter().map(|w| "-".repeat(*w + 2)).collect::<Vec<_>>().join("+"));
		writeln!(f, "{}", sep)?;

		let mut header = Vec::new();

		// Add encoded number header if present
		if has_row_numbers {
			let w = col_widths[0];
			let name = "__ROW__NUMBER__";
			let pad = w - display_width(name);
			let l = pad / 2;
			let r = pad - l;
			header.push(format!(" {:left$}{}{:right$} ", "", name, "", left = l, right = r));
		}

		// Add regular column headers
		for (display_idx, &col_idx) in column_order.iter().enumerate() {
			let col = &self[col_idx];
			let w = col_widths[row_num_col_idx + display_idx];
			let name = escape_control_chars(&col.qualified_name());
			let pad = w - display_width(&name);
			let l = pad / 2;
			let r = pad - l;
			header.push(format!(" {:left$}{}{:right$} ", "", name, "", left = l, right = r));
		}

		writeln!(f, "|{}|", header.join("|"))?;

		writeln!(f, "{}", sep)?;

		for row_numberx in 0..row_count {
			let mut row = Vec::new();

			// Add encoded number value if present
			if has_row_numbers {
				let w = col_widths[0];
				let s = if row_numberx < self.row_numbers.len() {
					self.row_numbers[row_numberx].to_string()
				} else {
					"Undefined".to_string()
				};
				let pad = w - display_width(&s);
				let l = pad / 2;
				let r = pad - l;
				row.push(format!(" {:left$}{}{:right$} ", "", s, "", left = l, right = r));
			}

			// Add regular column values
			for (display_idx, &col_idx) in column_order.iter().enumerate() {
				let col = &self[col_idx];
				let w = col_widths[row_num_col_idx + display_idx];
				let s = extract_string_value(col, row_numberx);
				let pad = w - display_width(&s);
				let l = pad / 2;
				let r = pad - l;
				row.push(format!(" {:left$}{}{:right$} ", "", s, "", left = l, right = r));
			}

			writeln!(f, "|{}|", row.join("|"))?;
		}

		writeln!(f, "{}", sep)
	}
}

#[cfg(test)]
mod tests {
	use std::convert::TryFrom;

	use reifydb_type::{
		Blob, Date, DateTime, Interval, OrderedF32, OrderedF64, RowNumber, Time, Type, Uuid4, Uuid7, Value,
		parse_uuid4, parse_uuid7,
	};

	use super::*;

	// Macro to create test columns with optional values (None = undefined)
	macro_rules! column_with_undefineds {
		($name:expr, Bool, $data:expr) => {{
			let result_data: Vec<Value> =
				$data.into_iter().map(|opt| opt.map_or(Value::Undefined, Value::Boolean)).collect();

			FrameColumn {
				namespace: None,
				store: None,
				name: $name.to_string(),
				r#type: Type::Boolean,
				data: result_data,
			}
		}};
		($name:expr, Float4, $data:expr) => {{
			let result_data: Vec<Value> = $data
				.into_iter()
				.map(|opt| {
					opt.map_or(Value::Undefined, |v| {
						Value::Float4(OrderedF32::try_from(v).unwrap())
					})
				})
				.collect();

			FrameColumn {
				namespace: None,
				store: None,
				name: $name.to_string(),
				r#type: Type::Float4,
				data: result_data,
			}
		}};
		($name:expr, Float8, $data:expr) => {{
			let result_data: Vec<Value> = $data
				.into_iter()
				.map(|opt| {
					opt.map_or(Value::Undefined, |v| {
						Value::Float8(OrderedF64::try_from(v).unwrap())
					})
				})
				.collect();

			FrameColumn {
				namespace: None,
				store: None,
				name: $name.to_string(),
				r#type: Type::Float8,
				data: result_data,
			}
		}};
		($name:expr, Utf8, $data:expr) => {{
			let result_data: Vec<Value> = $data
				.into_iter()
				.map(|opt| opt.map_or(Value::Undefined, |v| Value::Utf8(v.to_string())))
				.collect();

			FrameColumn {
				namespace: None,
				store: None,
				name: $name.to_string(),
				r#type: Type::Utf8,
				data: result_data,
			}
		}};
		($name:expr, RowNumber, $data:expr) => {{
			let result_data: Vec<Value> =
				$data.into_iter().map(|opt| opt.map_or(Value::Undefined, Value::RowNumber)).collect();

			FrameColumn {
				namespace: None,
				store: None,
				name: "__ROW__NUMBER__".to_string(),
				r#type: Type::RowNumber,
				data: result_data,
			}
		}};
		($name:expr, $type:ident, $data:expr) => {{
			let result_data: Vec<Value> =
				$data.into_iter().map(|opt| opt.map_or(Value::Undefined, Value::$type)).collect();

			FrameColumn {
				namespace: None,
				store: None,
				name: $name.to_string(),
				r#type: Type::$type,
				data: result_data,
			}
		}};
	}

	fn undefined_column(name: &str, count: usize) -> FrameColumn {
		FrameColumn {
			namespace: None,
			store: None,
			name: name.to_string(),
			r#type: Type::Undefined,
			data: vec![Value::Undefined; count],
		}
	}

	#[test]
	fn test_bool() {
		let frame = Frame::new(vec![], vec![column_with_undefineds!("bool", Bool, [Some(true), None])]);
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
		let frame = Frame::new(vec![], vec![column_with_undefineds!("float4", Float4, [Some(1.2_f32), None])]);
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
		let frame = Frame::new(vec![], vec![column_with_undefineds!("float8", Float8, [Some(3.14_f64), None])]);
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
		let frame = Frame::new(vec![], vec![column_with_undefineds!("int1", Int1, [Some(1_i8), None])]);
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
		let frame = Frame::new(vec![], vec![column_with_undefineds!("int2", Int2, [Some(100_i16), None])]);
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
		let frame = Frame::new(vec![], vec![column_with_undefineds!("int4", Int4, [Some(1000_i32), None])]);
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
		let frame = Frame::new(vec![], vec![column_with_undefineds!("int8", Int8, [Some(10000_i64), None])]);
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
		let frame =
			Frame::new(vec![], vec![column_with_undefineds!("int16", Int16, [Some(100000_i128), None])]);
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
		let frame = Frame::new(vec![], vec![column_with_undefineds!("uint1", Uint1, [Some(1_u8), None])]);
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
		let frame = Frame::new(vec![], vec![column_with_undefineds!("uint2", Uint2, [Some(100_u16), None])]);
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
		let frame = Frame::new(vec![], vec![column_with_undefineds!("uint4", Uint4, [Some(1000_u32), None])]);
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
		let frame = Frame::new(vec![], vec![column_with_undefineds!("uint8", Uint8, [Some(10000_u64), None])]);
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
		let frame =
			Frame::new(vec![], vec![column_with_undefineds!("uint16", Uint16, [Some(100000_u128), None])]);
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
		let frame = Frame::new(vec![], vec![column_with_undefineds!("string", Utf8, [Some("foo"), None])]);
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
		let frame = Frame::new(vec![], vec![undefined_column("undefined", 2)]);
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
		let frame = Frame::new(
			vec![],
			vec![column_with_undefineds!("date", Date, [Some(Date::from_ymd(2025, 1, 15).unwrap()), None])],
		);
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
		let frame = Frame::new(
			vec![],
			vec![column_with_undefineds!(
				"datetime",
				DateTime,
				[Some(DateTime::from_timestamp(1642694400).unwrap()), None]
			)],
		);
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
		let frame = Frame::new(
			vec![],
			vec![column_with_undefineds!("time", Time, [Some(Time::from_hms(14, 30, 45).unwrap()), None])],
		);
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
		let frame = Frame::new(
			vec![],
			vec![column_with_undefineds!("interval", Interval, [Some(Interval::from_days(30)), None])],
		);
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
		let frame = Frame::new(
			vec![],
			vec![column_with_undefineds!("__ROW__NUMBER__", RowNumber, [Some(RowNumber(1234)), None])],
		);
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
	fn test_row_number_display() {
		// Create a frame with regular columns and separate encoded numbers
		let regular_column = column_with_undefineds!("name", Utf8, [Some("Alice"), Some("Bob")]);

		let age_column = column_with_undefineds!("age", Int4, [Some(25_i32), Some(30_i32)]);

		// Create frame with encoded numbers as separate field
		let frame = Frame::new(vec![1, 2], vec![regular_column, age_column]);
		let output = format!("{}", frame);

		// Verify that __ROW__NUMBER__ appears as the first column in the output
		let lines: Vec<&str> = output.lines().collect();
		let header_line = lines[1]; // Second line contains the header

		assert!(header_line.starts_with("|  __ROW__NUMBER__"));

		// Check that the first data value in the first encoded is from encoded numbers
		let first_data_line = lines[3]; // Fourth line contains first data encoded
		assert!(first_data_line.contains("|         1         |")); // First RowNumber value
	}

	#[test]
	fn test_row_number_undefined_display() {
		// Create a RowNumber column with one undefined value
		let row_number_column = column_with_undefineds!(
			"__ROW__NUMBER__",
			RowNumber,
			[Some(RowNumber::new(1)), None] /* Second value is
			                                 * undefined */
		);

		let frame = Frame::new(vec![], vec![row_number_column]);
		let output = format!("{}", frame);

		// Verify that undefined RowNumber displays as "Undefined"
		let lines: Vec<&str> = output.lines().collect();
		let first_data_line = lines[3]; // First data encoded
		let second_data_line = lines[4]; // Second data encoded

		assert!(first_data_line.contains("1")); // First RowNumber value
		assert!(second_data_line.contains("Undefined")); // Second value should be
		// undefined
	}

	#[test]
	fn test_blob() {
		let frame = Frame::new(
			vec![],
			vec![column_with_undefineds!("blob", Blob, [Some(Blob::new(vec![0x01, 0x02, 0x03])), None])],
		);
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
		let frame = Frame::new(
			vec![],
			vec![column_with_undefineds!(
				"uuid4",
				Uuid4,
				[Some(Uuid4::from(parse_uuid4("550e8400-e29b-41d4-a716-446655440000").unwrap())), None]
			)],
		);
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
		let frame = Frame::new(
			vec![],
			vec![column_with_undefineds!(
				"uuid7",
				Uuid7,
				[Some(Uuid7::from(parse_uuid7("01890a5d-ac96-774b-b9aa-789c0686aaa4").unwrap())), None]
			)],
		);
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
