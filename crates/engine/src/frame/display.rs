// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::{ColumnValues, Frame};
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
        ColumnValues::Undefined(_) => "Undefined".into(),
    };
    escape_control_chars(&s)
}

impl Display for Frame {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let row_count = self.columns.first().map_or(0, |c| c.values.len());
        let col_count = self.columns.len();

        let mut col_widths = vec![0; col_count];

        for (i, col) in self.columns.iter().enumerate() {
            col_widths[i] = display_width(&col.name);
        }

        for row_idx in 0..row_count {
            for (i, col) in self.columns.iter().enumerate() {
                let s = extract_string_value(col, row_idx);
                col_widths[i] = col_widths[i].max(display_width(&s));
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

        let header = self
            .columns
            .iter()
            .enumerate()
            .map(|(i, col)| {
                let w = col_widths[i];
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
            let row = self
                .columns
                .iter()
                .enumerate()
                .map(|(i, col)| {
                    let w = col_widths[i];
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
}
