// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::{ColumnValues, Frame};
use reifydb_core::value::row_id::ROW_ID_COLUMN_NAME;
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
    if let Some(row_id_pos) = frame.columns.iter().position(|col| col.name == ROW_ID_COLUMN_NAME) {
        indices.remove(row_id_pos);
        indices.insert(0, row_id_pos);
    }
    
    indices
}

/// Extract string value from column at given row index, with proper escaping
fn extract_string_value(col: &crate::frame::FrameColumn, row_idx: usize) -> String {
    let s = match &col.values {
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
        ColumnValues::RowId(v) => v[row_idx].to_string(),
        ColumnValues::Undefined(_) => "Undefined".into(),
    };
    escape_control_chars(&s)
}

impl Display for Frame {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let row_count = self.columns.first().map_or(0, |c| c.values.len());
        let col_count = self.columns.len();
        
        // Get the display order with RowId column first
        let column_order = get_column_display_order(self);

        let mut col_widths = vec![0; col_count];

        for (display_idx, &col_idx) in column_order.iter().enumerate() {
            let col = &self.columns[col_idx];
            col_widths[display_idx] = display_width(&col.name);
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
                let name = &col.name;
                let pad = w - display_width(name);
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
    use crate::frame::FrameColumn;
    use reifydb_core::BitVec;

    #[test]
    fn test_bool() {
        let frame =
            Frame::new(vec![FrameColumn::bool_with_bitvec("bool", [true, false], [true, false])]);
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
        let frame = Frame::new(vec![FrameColumn::float4_with_bitvec(
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
        let frame = Frame::new(vec![FrameColumn::float8_with_bitvec(
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
        let frame =
            Frame::new(vec![FrameColumn::int1_with_bitvec("int1", [1, -1], [true, false])]);
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
        let frame =
            Frame::new(vec![FrameColumn::int2_with_bitvec("int2", [100, 200], [true, false])]);
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
        let frame =
            Frame::new(vec![FrameColumn::int4_with_bitvec("int4", [1000, 2000], [true, false])]);
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
        let frame = Frame::new(vec![FrameColumn::int8_with_bitvec(
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
        let frame = Frame::new(vec![FrameColumn::int16_with_bitvec(
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
        let frame =
            Frame::new(vec![FrameColumn::uint1_with_bitvec("uint1", [1, 2], [true, false])]);
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
        let frame =
            Frame::new(vec![FrameColumn::uint2_with_bitvec("uint2", [100, 200], [true, false])]);
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
        let frame = Frame::new(vec![FrameColumn::uint4_with_bitvec(
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
        let frame = Frame::new(vec![FrameColumn::uint8_with_bitvec(
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
        let frame = Frame::new(vec![FrameColumn::uint16_with_bitvec(
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
        let frame = Frame::new(vec![FrameColumn::utf8_with_bitvec(
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
        let frame = Frame::new(vec![FrameColumn::undefined("undefined", 2)]);
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
        use reifydb_core::{CowVec, Date};
        let dates =
            vec![Date::from_ymd(2025, 1, 15).unwrap(), Date::from_ymd(2025, 12, 25).unwrap()];
        let frame = Frame::new(vec![FrameColumn {
            name: "date".to_string(),
            values: ColumnValues::Date(CowVec::new(dates), BitVec::from_slice(&[true, false])),
        }]);
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
        use reifydb_core::{CowVec, DateTime};
        let datetimes = vec![
            DateTime::from_timestamp(1642694400).unwrap(),
            DateTime::from_timestamp(1735142400).unwrap(),
        ];
        let frame = Frame::new(vec![FrameColumn {
            name: "datetime".to_string(),
            values: ColumnValues::DateTime(
                CowVec::new(datetimes),
                BitVec::from_slice(&[true, false]),
            ),
        }]);
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
        use reifydb_core::{CowVec, Time};
        let times = vec![Time::from_hms(14, 30, 45).unwrap(), Time::from_hms(9, 15, 30).unwrap()];
        let frame = Frame::new(vec![FrameColumn {
            name: "time".to_string(),
            values: ColumnValues::Time(CowVec::new(times), BitVec::from_slice(&[true, false])),
        }]);
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
        use reifydb_core::{CowVec, Interval};
        let intervals = vec![Interval::from_days(30), Interval::from_hours(24)];
        let frame = Frame::new(vec![FrameColumn {
            name: "interval".to_string(),
            values: ColumnValues::Interval(
                CowVec::new(intervals),
                BitVec::from_slice(&[true, false]),
            ),
        }]);
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
    fn test_row_id_column_ordering() {
        use reifydb_core::{CowVec, RowId};
        
        // Create a frame with regular columns and a RowId column
        let regular_column = FrameColumn {
            name: "name".to_string(),
            values: ColumnValues::utf8(vec!["Alice".to_string(), "Bob".to_string()]),
        };
        
        let age_column = FrameColumn {
            name: "age".to_string(),
            values: ColumnValues::int4(vec![25, 30]),
        };
        
        let row_id_column = FrameColumn {
            name: ROW_ID_COLUMN_NAME.to_string(),
            values: ColumnValues::RowId(CowVec::new(vec![RowId::new(1), RowId::new(2)])),
        };
        
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
}
