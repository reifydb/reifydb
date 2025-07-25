// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::{ColumnValues, Frame, FrameColumn};
use crate::value::row_id::ROW_ID_COLUMN_NAME;
use std::fmt::{self, Display, Formatter};
use unicode_width::UnicodeWidthStr;

/// Calculate the display width of a string, handling newlines properly.
/// For strings with newlines, returns the width of the longest line.
/// For strings without newlines, returns the unicode display width.
fn display_width(s: &str) -> usize {
    if s.contains('\n') { s.lines().map(|line| line.width()).max().unwrap_or(0) } else { s.width() }
}

/// Escape newlines and tabs in a string for single-line display.
/// Replaces '\n' with "\\n" and '\t' with "\\t".
fn escape_control_chars(s: &str) -> String {
    s.replace('\n', "\\n").replace('\t', "\\t")
}

/// Create a column display order that puts RowId column first if it exists
fn get_column_display_order(frame: &Frame) -> Vec<usize> {
    let mut indices: Vec<usize> = (0..frame.columns.len()).collect();

    // Find the RowId column and move it to the front
    if let Some(row_id_pos) = frame.columns.iter().position(|col| col.name() == ROW_ID_COLUMN_NAME)
    {
        indices.remove(row_id_pos);
        indices.insert(0, row_id_pos);
    }

    indices
}

/// Determine if the frame contains columns from multiple source frames
/// Only shows qualified names when there are actually multiple table sources
fn has_multiple_frames(frame: &Frame) -> bool {
    if frame.columns.len() < 2 {
        return false;
    }

    // Collect unique table source frames (only those that have a source frame)
    let table_sources: std::collections::HashSet<&str> =
        frame.columns.iter().filter_map(|col| col.frame()).collect();

    // Only show qualified names if there are 2 or more actual table sources
    table_sources.len() > 1
}

/// Get the appropriate display name for a column based on whether multiple frames are present
fn get_column_display_name(col: &FrameColumn, use_qualified: bool) -> String {
    if use_qualified { col.qualified_name() } else { col.name().to_string() }
}

/// Extract string value from column at given row index, with proper escaping
fn extract_string_value(col: &FrameColumn, row_idx: usize) -> String {
    let s = match &col.values() {
        ColumnValues::Bool(v, bitvec) => {
            if bitvec.get(row_idx) {
                v[row_idx].to_string()
            } else {
                "Undefined".into()
            }
        }
        ColumnValues::Float4(v, bitvec) => {
            if bitvec.get(row_idx) {
                v[row_idx].to_string()
            } else {
                "Undefined".into()
            }
        }
        ColumnValues::Float8(v, bitvec) => {
            if bitvec.get(row_idx) {
                v[row_idx].to_string()
            } else {
                "Undefined".into()
            }
        }
        ColumnValues::Int1(v, bitvec) => {
            if bitvec.get(row_idx) {
                v[row_idx].to_string()
            } else {
                "Undefined".into()
            }
        }
        ColumnValues::Int2(v, bitvec) => {
            if bitvec.get(row_idx) {
                v[row_idx].to_string()
            } else {
                "Undefined".into()
            }
        }
        ColumnValues::Int4(v, bitvec) => {
            if bitvec.get(row_idx) {
                v[row_idx].to_string()
            } else {
                "Undefined".into()
            }
        }
        ColumnValues::Int8(v, bitvec) => {
            if bitvec.get(row_idx) {
                v[row_idx].to_string()
            } else {
                "Undefined".into()
            }
        }
        ColumnValues::Int16(v, bitvec) => {
            if bitvec.get(row_idx) {
                v[row_idx].to_string()
            } else {
                "Undefined".into()
            }
        }
        ColumnValues::Uint1(v, bitvec) => {
            if bitvec.get(row_idx) {
                v[row_idx].to_string()
            } else {
                "Undefined".into()
            }
        }
        ColumnValues::Uint2(v, bitvec) => {
            if bitvec.get(row_idx) {
                v[row_idx].to_string()
            } else {
                "Undefined".into()
            }
        }
        ColumnValues::Uint4(v, bitvec) => {
            if bitvec.get(row_idx) {
                v[row_idx].to_string()
            } else {
                "Undefined".into()
            }
        }
        ColumnValues::Uint8(v, bitvec) => {
            if bitvec.get(row_idx) {
                v[row_idx].to_string()
            } else {
                "Undefined".into()
            }
        }
        ColumnValues::Uint16(v, bitvec) => {
            if bitvec.get(row_idx) {
                v[row_idx].to_string()
            } else {
                "Undefined".into()
            }
        }
        ColumnValues::Utf8(v, bitvec) => {
            if bitvec.get(row_idx) {
                v[row_idx].clone()
            } else {
                "Undefined".into()
            }
        }
        ColumnValues::Date(v, bitvec) => {
            if bitvec.get(row_idx) {
                v[row_idx].to_string()
            } else {
                "Undefined".into()
            }
        }
        ColumnValues::DateTime(v, bitvec) => {
            if bitvec.get(row_idx) {
                v[row_idx].to_string()
            } else {
                "Undefined".into()
            }
        }
        ColumnValues::Time(v, bitvec) => {
            if bitvec.get(row_idx) {
                v[row_idx].to_string()
            } else {
                "Undefined".into()
            }
        }
        ColumnValues::Interval(v, bitvec) => {
            if bitvec.get(row_idx) {
                v[row_idx].to_string()
            } else {
                "Undefined".into()
            }
        }
        ColumnValues::RowId(v, b) => {
            if b.get(row_idx) {
                v[row_idx].to_string()
            } else {
                "Undefined".into()
            }
        }
        ColumnValues::Uuid4(v, b) => {
            if b.get(row_idx) {
                v[row_idx].to_string()
            } else {
                "Undefined".into()
            }
        }
        ColumnValues::Uuid7(v, b) => {
            if b.get(row_idx) {
                v[row_idx].to_string()
            } else {
                "Undefined".into()
            }
        }
        ColumnValues::Undefined(_) => "Undefined".into(),
    };
    escape_control_chars(&s)
}

impl Display for Frame {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let row_count = self.columns.first().map_or(0, |c| c.values().len());
        let col_count = self.columns.len();

        // Get the display order with RowId column first
        let column_order = get_column_display_order(self);

        // Determine if we should show qualified names (when multiple source frames are present)
        let use_qualified_names = has_multiple_frames(self);

        let mut col_widths = vec![0; col_count];

        for (display_idx, &col_idx) in column_order.iter().enumerate() {
            let col = &self.columns[col_idx];
            let display_name = get_column_display_name(col, use_qualified_names);
            col_widths[display_idx] = display_width(&display_name);
        }

        for row_idx in 0..row_count {
            for (display_idx, &col_idx) in column_order.iter().enumerate() {
                let col = &self.columns[col_idx];
                let s = extract_string_value(col, row_idx);
                col_widths[display_idx] = col_widths[display_idx].max(display_width(&s));
            }
        }

        // Add padding
        for w in &mut col_widths {
            *w += 2;
        }

        let sep = format!(
            "+{}+",
            col_widths.iter().map(|w| "-".repeat(*w + 2)).collect::<Vec<_>>().join("+")
        );
        writeln!(f, "{}", sep)?;

        let header = column_order
            .iter()
            .enumerate()
            .map(|(display_idx, &col_idx)| {
                let col = &self.columns[col_idx];
                let w = col_widths[display_idx];
                let name = get_column_display_name(col, use_qualified_names);
                let pad = w - display_width(&name);
                let l = pad / 2;
                let r = pad - l;
                format!(" {:left$}{}{:right$} ", "", name, "", left = l, right = r)
            })
            .collect::<Vec<_>>();
        writeln!(f, "|{}|", header.join("|"))?;

        writeln!(f, "{}", sep)?;

        for row_idx in 0..row_count {
            let row = column_order
                .iter()
                .enumerate()
                .map(|(display_idx, &col_idx)| {
                    let col = &self.columns[col_idx];
                    let w = col_widths[display_idx];
                    let s = extract_string_value(col, row_idx);
                    let pad = w - display_width(&s);
                    let l = pad / 2;
                    let r = pad - l;
                    format!(" {:left$}{}{:right$} ", "", s, "", left = l, right = r)
                })
                .collect::<Vec<_>>();

            writeln!(f, "|{}|", row.join("|"))?;
        }

        writeln!(f, "{}", sep)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::column::TableQualified;
    use crate::{BitVec, RowId};

    #[test]
    fn test_bool() {
        let frame = Frame::new(vec![TableQualified::bool_with_bitvec(
            "test_frame",
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
        let frame = Frame::new(vec![TableQualified::float4_with_bitvec(
            "test_frame",
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
    fn test_float8() {
        let frame = Frame::new(vec![TableQualified::float8_with_bitvec(
            "test_frame",
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
        let frame = Frame::new(vec![TableQualified::int1_with_bitvec(
            "test_frame",
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
        let frame = Frame::new(vec![TableQualified::int2_with_bitvec(
            "test_frame",
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
        let frame = Frame::new(vec![TableQualified::int4_with_bitvec(
            "test_frame",
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
        let frame = Frame::new(vec![TableQualified::int8_with_bitvec(
            "test_frame",
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
        let frame = Frame::new(vec![TableQualified::int16_with_bitvec(
            "test_frame",
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
        let frame = Frame::new(vec![TableQualified::uint1_with_bitvec(
            "test_frame",
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
        let frame = Frame::new(vec![TableQualified::uint2_with_bitvec(
            "test_frame",
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
        let frame = Frame::new(vec![TableQualified::uint4_with_bitvec(
            "test_frame",
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
        let frame = Frame::new(vec![TableQualified::uint8_with_bitvec(
            "test_frame",
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
        let frame = Frame::new(vec![TableQualified::uint16_with_bitvec(
            "test_frame",
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
        let frame = Frame::new(vec![TableQualified::utf8_with_bitvec(
            "test_frame",
            "string",
            ["foo", "bar"],
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
        let frame = Frame::new(vec![TableQualified::undefined("test_frame", "undefined", 2)]);
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
        use crate::Date;
        let dates =
            vec![Date::from_ymd(2025, 1, 15).unwrap(), Date::from_ymd(2025, 12, 25).unwrap()];
        let frame = Frame::new(vec![TableQualified::date_with_bitvec(
            "test",
            "date",
            dates,
            BitVec::from_slice(&[true, false]),
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
        use crate::DateTime;
        let datetimes = vec![
            DateTime::from_timestamp(1642694400).unwrap(),
            DateTime::from_timestamp(1735142400).unwrap(),
        ];
        let frame = Frame::new(vec![TableQualified::datetime_with_bitvec(
            "test",
            "datetime",
            datetimes,
            BitVec::from_slice(&[true, false]),
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
        use crate::Time;
        let times = vec![Time::from_hms(14, 30, 45).unwrap(), Time::from_hms(9, 15, 30).unwrap()];
        let frame = Frame::new(vec![TableQualified::time_with_bitvec(
            "test",
            "time",
            times,
            BitVec::from_slice(&[true, false]),
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
        use crate::Interval;
        let intervals = vec![Interval::from_days(30), Interval::from_hours(24)];
        let frame = Frame::new(vec![TableQualified::interval_with_bitvec(
            "test",
            "interval",
            intervals,
            BitVec::from_slice(&[true, false]),
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
    fn test_row_id() {
        let ids = vec![RowId(1234), RowId(5678)];
        let frame = Frame::new(vec![TableQualified::row_id_with_bitvec(
            "test",
            ids,
            BitVec::from_slice(&[true, false]),
        )]);
        let output = format!("{}", frame);
        let expected = "\
+---------------+
|  __ROW__ID__  |
+---------------+
|     1234      |
|   Undefined   |
+---------------+
";
        assert_eq!(output, expected);
    }

    #[test]
    fn test_row_id_column_ordering() {
        use crate::RowId;

        // Create a frame with regular columns and a RowId column
        let regular_column = TableQualified::utf8("test", "name", ["Alice", "Bob"]);

        let age_column = TableQualified::int4("test", "age", [25, 30]);

        let row_id_column = TableQualified::row_id("test", [RowId::new(1), RowId::new(2)]);

        // Create frame with RowId column NOT first (it should be reordered)
        let frame = Frame::new(vec![regular_column, age_column, row_id_column]);
        let output = format!("{}", frame);

        // Verify that __ROW__ID__ appears as the first column in the output
        let lines: Vec<&str> = output.lines().collect();
        let header_line = lines[1]; // Second line contains the header

        // The header should start with __ROW__ID__ column
        assert!(header_line.contains("__ROW__ID__"));

        // Check that the first data value in the first row is from the RowId column
        let first_data_line = lines[3]; // Fourth line contains first data row
        assert!(first_data_line.contains("1")); // First RowId value
    }

    #[test]
    fn test_row_id_undefined_display() {
        use crate::{BitVec, RowId};

        // Create a RowId column with one undefined value
        let row_id_column = TableQualified::row_id_with_bitvec(
            "test",
            [RowId::new(1), RowId::new(2)],
            BitVec::from_slice(&[true, false]), // Second value is undefined
        );

        let frame = Frame::new(vec![row_id_column]);
        let output = format!("{}", frame);

        // Verify that undefined RowId displays as "Undefined"
        let lines: Vec<&str> = output.lines().collect();
        let first_data_line = lines[3]; // First data row
        let second_data_line = lines[4]; // Second data row

        assert!(first_data_line.contains("1")); // First RowId value
        assert!(second_data_line.contains("Undefined")); // Second value should be undefined
    }
}
