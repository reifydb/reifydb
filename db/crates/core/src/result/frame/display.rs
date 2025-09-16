// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::fmt::{self, Display, Formatter};

use reifydb_type::{ROW_NUMBER_COLUMN_NAME, util::unicode::UnicodeWidthStr};

use crate::result::frame::{Frame, FrameColumn};

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
fn extract_string_value(col: &FrameColumn, row_numberx: usize) -> String {
	let s = col.data.as_string(row_numberx);
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
	use reifydb_type::{
		Blob, Date, DateTime, Interval, RowNumber, Time, Uuid4, Uuid7,
		parse_uuid4, parse_uuid7,
	};

	use super::*;
	use crate::{
		BitVec, FrameColumnData,
		value::container::{
			BlobContainer, BoolContainer, NumberContainer,
			RowNumberContainer, TemporalContainer,
			UndefinedContainer, Utf8Container, UuidContainer,
		},
	};

	// Macro to create test columns with optional values (None = undefined)
	macro_rules! column_with_undefineds {
		($name:expr, Bool, $data:expr) => {{
			let (values, bitvec): (Vec<_>, Vec<_>) = $data
				.into_iter()
				.map(|opt| match opt {
					Some(v) => (v, true),
					None => (false, false), // dummy value, will be marked as undefined
				})
				.unzip();

			FrameColumn {
				namespace: None,
				store: None,
				name: $name.to_string(),
				data: FrameColumnData::Bool(BoolContainer::new(
					values,
					BitVec::from_slice(&bitvec),
				)),
			}
		}};
		($name:expr, Float4, $data:expr) => {{
			let (values, bitvec): (Vec<_>, Vec<_>) = $data
				.into_iter()
				.map(|opt| match opt {
					Some(v) => (v, true),
					None => (0.0_f32, false), // dummy value, will be marked as undefined
				})
				.unzip();

			FrameColumn {
				namespace: None,
				store: None,
				name: $name.to_string(),
				data: FrameColumnData::Float4(NumberContainer::new(
					values,
					BitVec::from_slice(&bitvec),
				)),
			}
		}};
		($name:expr, Float8, $data:expr) => {{
			let (values, bitvec): (Vec<_>, Vec<_>) = $data
				.into_iter()
				.map(|opt| match opt {
					Some(v) => (v, true),
					None => (0.0_f64, false), // dummy value, will be marked as undefined
				})
				.unzip();

			FrameColumn {
				namespace: None,
				store: None,
				name: $name.to_string(),
				data: FrameColumnData::Float8(NumberContainer::new(
					values,
					BitVec::from_slice(&bitvec),
				)),
			}
		}};
		($name:expr, Int1, $data:expr) => {{
			let (values, bitvec): (Vec<_>, Vec<_>) = $data
				.into_iter()
				.map(|opt| match opt {
					Some(v) => (v, true),
					None => (0_i8, false), // dummy value, will be marked as undefined
				})
				.unzip();

			FrameColumn {
				namespace: None,
				store: None,
				name: $name.to_string(),
				data: FrameColumnData::Int1(NumberContainer::new(
					values,
					BitVec::from_slice(&bitvec),
				)),
			}
		}};
		($name:expr, Int2, $data:expr) => {{
			let (values, bitvec): (Vec<_>, Vec<_>) = $data
				.into_iter()
				.map(|opt| match opt {
					Some(v) => (v, true),
					None => (0_i16, false), // dummy value, will be marked as undefined
				})
				.unzip();

			FrameColumn {
				namespace: None,
				store: None,
				name: $name.to_string(),
				data: FrameColumnData::Int2(NumberContainer::new(
					values,
					BitVec::from_slice(&bitvec),
				)),
			}
		}};
		($name:expr, Int4, $data:expr) => {{
			let (values, bitvec): (Vec<_>, Vec<_>) = $data
				.into_iter()
				.map(|opt| match opt {
					Some(v) => (v, true),
					None => (0_i32, false), // dummy value, will be marked as undefined
				})
				.unzip();

			FrameColumn {
				namespace: None,
				store: None,
				name: $name.to_string(),
				data: FrameColumnData::Int4(NumberContainer::new(
					values,
					BitVec::from_slice(&bitvec),
				)),
			}
		}};
		($name:expr, Int8, $data:expr) => {{
			let (values, bitvec): (Vec<_>, Vec<_>) = $data
				.into_iter()
				.map(|opt| match opt {
					Some(v) => (v, true),
					None => (0_i64, false), // dummy value, will be marked as undefined
				})
				.unzip();

			FrameColumn {
				namespace: None,
				store: None,
				name: $name.to_string(),
				data: FrameColumnData::Int8(NumberContainer::new(
					values,
					BitVec::from_slice(&bitvec),
				)),
			}
		}};
		($name:expr, Int16, $data:expr) => {{
			let (values, bitvec): (Vec<_>, Vec<_>) = $data
				.into_iter()
				.map(|opt| match opt {
					Some(v) => (v, true),
					None => (0_i128, false), // dummy value, will be marked as undefined
				})
				.unzip();

			FrameColumn {
				namespace: None,
				store: None,
				name: $name.to_string(),
				data: FrameColumnData::Int16(NumberContainer::new(
					values,
					BitVec::from_slice(&bitvec),
				)),
			}
		}};
		($name:expr, Uint1, $data:expr) => {{
			let (values, bitvec): (Vec<_>, Vec<_>) = $data
				.into_iter()
				.map(|opt| match opt {
					Some(v) => (v, true),
					None => (0_u8, false), // dummy value, will be marked as undefined
				})
				.unzip();

			FrameColumn {
				namespace: None,
				store: None,
				name: $name.to_string(),
				data: FrameColumnData::Uint1(NumberContainer::new(
					values,
					BitVec::from_slice(&bitvec),
				)),
			}
		}};
		($name:expr, Uint2, $data:expr) => {{
			let (values, bitvec): (Vec<_>, Vec<_>) = $data
				.into_iter()
				.map(|opt| match opt {
					Some(v) => (v, true),
					None => (0_u16, false), // dummy value, will be marked as undefined
				})
				.unzip();

			FrameColumn {
				namespace: None,
				store: None,
				name: $name.to_string(),
				data: FrameColumnData::Uint2(NumberContainer::new(
					values,
					BitVec::from_slice(&bitvec),
				)),
			}
		}};
		($name:expr, Uint4, $data:expr) => {{
			let (values, bitvec): (Vec<_>, Vec<_>) = $data
				.into_iter()
				.map(|opt| match opt {
					Some(v) => (v, true),
					None => (0_u32, false), // dummy value, will be marked as undefined
				})
				.unzip();

			FrameColumn {
				namespace: None,
				store: None,
				name: $name.to_string(),
				data: FrameColumnData::Uint4(NumberContainer::new(
					values,
					BitVec::from_slice(&bitvec),
				)),
			}
		}};
		($name:expr, Uint8, $data:expr) => {{
			let (values, bitvec): (Vec<_>, Vec<_>) = $data
				.into_iter()
				.map(|opt| match opt {
					Some(v) => (v, true),
					None => (0_u64, false), // dummy value, will be marked as undefined
				})
				.unzip();

			FrameColumn {
				namespace: None,
				store: None,
				name: $name.to_string(),
				data: FrameColumnData::Uint8(NumberContainer::new(
					values,
					BitVec::from_slice(&bitvec),
				)),
			}
		}};
		($name:expr, Uint16, $data:expr) => {{
			let (values, bitvec): (Vec<_>, Vec<_>) = $data
				.into_iter()
				.map(|opt| match opt {
					Some(v) => (v, true),
					None => (0_u128, false), // dummy value, will be marked as undefined
				})
				.unzip();

			FrameColumn {
				namespace: None,
				store: None,
				name: $name.to_string(),
				data: FrameColumnData::Uint16(NumberContainer::new(
					values,
					BitVec::from_slice(&bitvec),
				)),
			}
		}};
		($name:expr, Utf8, $data:expr) => {{
			let (values, bitvec): (Vec<_>, Vec<_>) = $data
				.into_iter()
				.map(|opt| match opt {
					Some(v) => (v.to_string(), true),
					None => (String::new(), false), // dummy value, will be marked as undefined
				})
				.unzip();

			FrameColumn {
				namespace: None,
				store: None,
				name: $name.to_string(),
				data: FrameColumnData::Utf8(Utf8Container::new(
					values,
					BitVec::from_slice(&bitvec),
				)),
			}
		}};
		($name:expr, Date, $data:expr) => {{
			let (values, bitvec): (Vec<_>, Vec<_>) = $data
				.into_iter()
				.map(|opt| match opt {
					Some(v) => (v, true),
					None => (Date::from_ymd(1970, 1, 1).unwrap(), false), // dummy value
				})
				.unzip();

			FrameColumn {
				namespace: None,
				store: None,
				name: $name.to_string(),
				data: FrameColumnData::Date(TemporalContainer::new(
					values,
					BitVec::from_slice(&bitvec),
				)),
			}
		}};
		($name:expr, DateTime, $data:expr) => {{
			let (values, bitvec): (Vec<_>, Vec<_>) = $data
				.into_iter()
				.map(|opt| match opt {
					Some(v) => (v, true),
					None => (DateTime::from_timestamp(0).unwrap(), false), // dummy value
				})
				.unzip();

			FrameColumn {
				namespace: None,
				store: None,
				name: $name.to_string(),
				data: FrameColumnData::DateTime(TemporalContainer::new(
					values,
					BitVec::from_slice(&bitvec),
				)),
			}
		}};
		($name:expr, Time, $data:expr) => {{
			let (values, bitvec): (Vec<_>, Vec<_>) = $data
				.into_iter()
				.map(|opt| match opt {
					Some(v) => (v, true),
					None => (Time::from_hms(0, 0, 0).unwrap(), false), // dummy value
				})
				.unzip();

			FrameColumn {
				namespace: None,
				store: None,
				name: $name.to_string(),
				data: FrameColumnData::Time(TemporalContainer::new(
					values,
					BitVec::from_slice(&bitvec),
				)),
			}
		}};
		($name:expr, Interval, $data:expr) => {{
			let (values, bitvec): (Vec<_>, Vec<_>) = $data
				.into_iter()
				.map(|opt| match opt {
					Some(v) => (v, true),
					None => (Interval::from_days(0), false), // dummy value
				})
				.unzip();

			FrameColumn {
				namespace: None,
				store: None,
				name: $name.to_string(),
				data: FrameColumnData::Interval(TemporalContainer::new(
					values,
					BitVec::from_slice(&bitvec),
				)),
			}
		}};
		($name:expr, Blob, $data:expr) => {{
			let (values, bitvec): (Vec<_>, Vec<_>) = $data
				.into_iter()
				.map(|opt| match opt {
					Some(v) => (v, true),
					None => (Blob::new(vec![]), false), // dummy value
				})
				.unzip();

			FrameColumn {
				namespace: None,
				store: None,
				name: $name.to_string(),
				data: FrameColumnData::Blob(BlobContainer::new(
					values,
					BitVec::from_slice(&bitvec),
				)),
			}
		}};
		($name:expr, Uuid4, $data:expr) => {{
			let (values, bitvec): (Vec<_>, Vec<_>) = $data
				.into_iter()
				.map(|opt| match opt {
					Some(v) => (v, true),
					None => (Uuid4::from(parse_uuid4("550e8400-e29b-41d4-a716-446655440000").unwrap()), false), // dummy value
				})
				.unzip();

			FrameColumn {
				namespace: None,
				store: None,
				name: $name.to_string(),
				data: FrameColumnData::Uuid4(UuidContainer::new(
					values,
					BitVec::from_slice(&bitvec),
				)),
			}
		}};
		($name:expr, Uuid7, $data:expr) => {{
			let (values, bitvec): (Vec<_>, Vec<_>) = $data
				.into_iter()
				.map(|opt| match opt {
					Some(v) => (v, true),
					None => (Uuid7::from(parse_uuid7("00000000-0000-7000-8000-000000000000").unwrap()), false), // dummy value
				})
				.unzip();

			FrameColumn {
				namespace: None,
				store: None,
				name: $name.to_string(),
				data: FrameColumnData::Uuid7(UuidContainer::new(
					values,
					BitVec::from_slice(&bitvec),
				)),
			}
		}};
		($name:expr, RowNumber, $data:expr) => {{
			let (values, bitvec): (Vec<_>, Vec<_>) = $data
				.into_iter()
				.map(|opt| match opt {
					Some(v) => (v, true),
					None => (RowNumber(0), false), // dummy value
				})
				.unzip();

			FrameColumn {
				namespace: None,
				store: None,
				name: ROW_NUMBER_COLUMN_NAME.to_string(),
				data: FrameColumnData::RowNumber(RowNumberContainer::new(
					values,
					BitVec::from_slice(&bitvec),
				)),
			}
		}};
	}

	fn undefined_column(name: &str, count: usize) -> FrameColumn {
		FrameColumn {
			namespace: None,
			store: None,
			name: name.to_string(),
			data: FrameColumnData::Undefined(
				UndefinedContainer::new(count),
			),
		}
	}

	fn row_number_column(
		data: impl IntoIterator<Item = RowNumber>,
	) -> FrameColumn {
		let data_vec: Vec<RowNumber> = data.into_iter().collect();
		let bitvec = BitVec::repeat(data_vec.len(), true);
		FrameColumn {
			namespace: None,
			store: None,
			name: ROW_NUMBER_COLUMN_NAME.to_string(),
			data: FrameColumnData::RowNumber(
				RowNumberContainer::new(data_vec, bitvec),
			),
		}
	}

	#[test]
	fn test_bool() {
		let frame = Frame::new(vec![column_with_undefineds!(
			"bool",
			Bool,
			[Some(true), None]
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
		let frame = Frame::new(vec![column_with_undefineds!(
			"float4",
			Float4,
			[Some(1.2_f32), None]
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
		let frame = Frame::new(vec![column_with_undefineds!(
			"float8",
			Float8,
			[Some(3.14_f64), None]
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
		let frame = Frame::new(vec![column_with_undefineds!(
			"int1",
			Int1,
			[Some(1_i8), None]
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
		let frame = Frame::new(vec![column_with_undefineds!(
			"int2",
			Int2,
			[Some(100_i16), None]
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
		let frame = Frame::new(vec![column_with_undefineds!(
			"int4",
			Int4,
			[Some(1000_i32), None]
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
		let frame = Frame::new(vec![column_with_undefineds!(
			"int8",
			Int8,
			[Some(10000_i64), None]
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
		let frame = Frame::new(vec![column_with_undefineds!(
			"int16",
			Int16,
			[Some(100000_i128), None]
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
		let frame = Frame::new(vec![column_with_undefineds!(
			"uint1",
			Uint1,
			[Some(1_u8), None]
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
		let frame = Frame::new(vec![column_with_undefineds!(
			"uint2",
			Uint2,
			[Some(100_u16), None]
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
		let frame = Frame::new(vec![column_with_undefineds!(
			"uint4",
			Uint4,
			[Some(1000_u32), None]
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
		let frame = Frame::new(vec![column_with_undefineds!(
			"uint8",
			Uint8,
			[Some(10000_u64), None]
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
		let frame = Frame::new(vec![column_with_undefineds!(
			"uint16",
			Uint16,
			[Some(100000_u128), None]
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
		let frame = Frame::new(vec![column_with_undefineds!(
			"string",
			Utf8,
			[Some("foo"), None]
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
		let frame = Frame::new(vec![column_with_undefineds!(
			"date",
			Date,
			[Some(Date::from_ymd(2025, 1, 15).unwrap()), None]
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
		let frame = Frame::new(vec![column_with_undefineds!(
			"datetime",
			DateTime,
			[
				Some(DateTime::from_timestamp(1642694400)
					.unwrap()),
				None
			]
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
		let frame = Frame::new(vec![column_with_undefineds!(
			"time",
			Time,
			[Some(Time::from_hms(14, 30, 45).unwrap()), None]
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
		let frame = Frame::new(vec![column_with_undefineds!(
			"interval",
			Interval,
			[Some(Interval::from_days(30)), None]
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
		let frame = Frame::new(vec![column_with_undefineds!(
			"__ROW__NUMBER__",
			RowNumber,
			[Some(RowNumber(1234)), None]
		)]);
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
		let regular_column = column_with_undefineds!(
			"name",
			Utf8,
			[Some("Alice"), Some("Bob")]
		);

		let age_column = column_with_undefineds!(
			"age",
			Int4,
			[Some(25_i32), Some(30_i32)]
		);

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

		// Verify that __ROW__ID__ appears as the first column in the
		// output
		let lines: Vec<&str> = output.lines().collect();
		let header_line = lines[1]; // Second line contains the header

		// The header should start with __ROW__ID__ column
		assert!(header_line.contains("__ROW__NUMBER__"));

		// Check that the first data value in the first row is from the
		// RowNumber column
		let first_data_line = lines[3]; // Fourth line contains first data row
		assert!(first_data_line.contains("1")); // First RowNumber value
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
		let frame = Frame::new(vec![column_with_undefineds!(
			"blob",
			Blob,
			[Some(Blob::new(vec![0x01, 0x02, 0x03])), None]
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
		let frame = Frame::new(vec![column_with_undefineds!(
			"uuid4",
			Uuid4,
			[
				Some(Uuid4::from(
					parse_uuid4(
						"550e8400-e29b-41d4-a716-446655440000"
					)
					.unwrap()
				)),
				None
			]
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
		let frame = Frame::new(vec![column_with_undefineds!(
			"uuid7",
			Uuid7,
			[
				Some(Uuid7::from(
					parse_uuid7(
						"01890a5d-ac96-774b-b9aa-789c0686aaa4"
					)
					.unwrap()
				)),
				None
			]
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
