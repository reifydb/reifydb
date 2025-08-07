// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::result::frame::{Frame, FrameColumn};
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
    let mut indices: Vec<usize> = (0..frame.len()).collect();

    // Find the RowId column and move it to the front
    if let Some(row_id_pos) = frame.iter().position(|col| col.name == ROW_ID_COLUMN_NAME) {
        indices.remove(row_id_pos);
        indices.insert(0, row_id_pos);
    }

    indices
}

/// Extract string value from column at given row index, with proper escaping
fn extract_string_value(col: &FrameColumn, row_idx: usize) -> String {
    let s = col.data.as_string(row_idx);
    escape_control_chars(&s)
}

impl Display for Frame {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let row_count = self.first().map_or(0, |c| c.data.len());
        let col_count = self.len();

        // Get the display order with RowId column first
        let column_order = get_column_display_order(self);

        let mut col_widths = vec![0; col_count];

        for (display_idx, &col_idx) in column_order.iter().enumerate() {
            let col = &self[col_idx];
            let display_name = col.qualified_name();
            col_widths[display_idx] = display_width(&display_name);
        }

        for row_idx in 0..row_count {
            for (display_idx, &col_idx) in column_order.iter().enumerate() {
                let col = &self[col_idx];
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
                let col = &self[col_idx];
                let w = col_widths[display_idx];
                let name = col.qualified_name();
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
                    let col = &self[col_idx];
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
    use crate::value::Blob;
    use crate::value::container::{
        BlobContainer, BoolContainer, NumberContainer, RowIdContainer, StringContainer,
        TemporalContainer, UndefinedContainer, UuidContainer,
    };
    use crate::value::uuid::{Uuid4, Uuid7};
    use crate::{BitVec, Date, DateTime, FrameColumnData, Interval, RowId, Time};

    fn bool_column_with_bitvec(
        name: &str,
        data: impl IntoIterator<Item = bool>,
        bitvec: impl IntoIterator<Item = bool>,
    ) -> FrameColumn {
        FrameColumn {
            schema: None,
            table: None,
            name: name.to_string(),
            data: FrameColumnData::Bool(BoolContainer::new(
                data.into_iter().collect(),
                BitVec::from_slice(&bitvec.into_iter().collect::<Vec<_>>()),
            )),
        }
    }

    fn float4_column_with_bitvec(
        name: &str,
        data: impl IntoIterator<Item = f32>,
        bitvec: impl IntoIterator<Item = bool>,
    ) -> FrameColumn {
        FrameColumn {
            schema: None,
            table: None,
            name: name.to_string(),
            data: FrameColumnData::Float4(NumberContainer::new(
                data.into_iter().collect(),
                BitVec::from_slice(&bitvec.into_iter().collect::<Vec<_>>()),
            )),
        }
    }

    fn float8_column_with_bitvec(
        name: &str,
        data: impl IntoIterator<Item = f64>,
        bitvec: impl IntoIterator<Item = bool>,
    ) -> FrameColumn {
        FrameColumn {
            schema: None,
            table: None,
            name: name.to_string(),
            data: FrameColumnData::Float8(NumberContainer::new(
                data.into_iter().collect(),
                BitVec::from_slice(&bitvec.into_iter().collect::<Vec<_>>()),
            )),
        }
    }

    fn int1_column_with_bitvec(
        name: &str,
        data: impl IntoIterator<Item = i8>,
        bitvec: impl IntoIterator<Item = bool>,
    ) -> FrameColumn {
        FrameColumn {
            schema: None,
            table: None,
            name: name.to_string(),
            data: FrameColumnData::Int1(NumberContainer::new(
                data.into_iter().collect(),
                BitVec::from_slice(&bitvec.into_iter().collect::<Vec<_>>()),
            )),
        }
    }

    fn int2_column_with_bitvec(
        name: &str,
        data: impl IntoIterator<Item = i16>,
        bitvec: impl IntoIterator<Item = bool>,
    ) -> FrameColumn {
        FrameColumn {
            schema: None,
            table: None,
            name: name.to_string(),
            data: FrameColumnData::Int2(NumberContainer::new(
                data.into_iter().collect(),
                BitVec::from_slice(&bitvec.into_iter().collect::<Vec<_>>()),
            )),
        }
    }

    fn int4_column_with_bitvec(
        name: &str,
        data: impl IntoIterator<Item = i32>,
        bitvec: impl IntoIterator<Item = bool>,
    ) -> FrameColumn {
        FrameColumn {
            schema: None,
            table: None,
            name: name.to_string(),
            data: FrameColumnData::Int4(NumberContainer::new(
                data.into_iter().collect(),
                BitVec::from_slice(&bitvec.into_iter().collect::<Vec<_>>()),
            )),
        }
    }

    fn int8_column_with_bitvec(
        name: &str,
        data: impl IntoIterator<Item = i64>,
        bitvec: impl IntoIterator<Item = bool>,
    ) -> FrameColumn {
        FrameColumn {
            schema: None,
            table: None,
            name: name.to_string(),
            data: FrameColumnData::Int8(NumberContainer::new(
                data.into_iter().collect(),
                BitVec::from_slice(&bitvec.into_iter().collect::<Vec<_>>()),
            )),
        }
    }

    fn int16_column_with_bitvec(
        name: &str,
        data: impl IntoIterator<Item = i128>,
        bitvec: impl IntoIterator<Item = bool>,
    ) -> FrameColumn {
        FrameColumn {
            schema: None,
            table: None,
            name: name.to_string(),
            data: FrameColumnData::Int16(NumberContainer::new(
                data.into_iter().collect(),
                BitVec::from_slice(&bitvec.into_iter().collect::<Vec<_>>()),
            )),
        }
    }

    fn uint1_column_with_bitvec(
        name: &str,
        data: impl IntoIterator<Item = u8>,
        bitvec: impl IntoIterator<Item = bool>,
    ) -> FrameColumn {
        FrameColumn {
            schema: None,
            table: None,
            name: name.to_string(),
            data: FrameColumnData::Uint1(NumberContainer::new(
                data.into_iter().collect(),
                BitVec::from_slice(&bitvec.into_iter().collect::<Vec<_>>()),
            )),
        }
    }

    fn uint2_column_with_bitvec(
        name: &str,
        data: impl IntoIterator<Item = u16>,
        bitvec: impl IntoIterator<Item = bool>,
    ) -> FrameColumn {
        FrameColumn {
            schema: None,
            table: None,
            name: name.to_string(),
            data: FrameColumnData::Uint2(NumberContainer::new(
                data.into_iter().collect(),
                BitVec::from_slice(&bitvec.into_iter().collect::<Vec<_>>()),
            )),
        }
    }

    fn uint4_column_with_bitvec(
        name: &str,
        data: impl IntoIterator<Item = u32>,
        bitvec: impl IntoIterator<Item = bool>,
    ) -> FrameColumn {
        FrameColumn {
            schema: None,
            table: None,
            name: name.to_string(),
            data: FrameColumnData::Uint4(NumberContainer::new(
                data.into_iter().collect(),
                BitVec::from_slice(&bitvec.into_iter().collect::<Vec<_>>()),
            )),
        }
    }

    fn uint8_column_with_bitvec(
        name: &str,
        data: impl IntoIterator<Item = u64>,
        bitvec: impl IntoIterator<Item = bool>,
    ) -> FrameColumn {
        FrameColumn {
            schema: None,
            table: None,
            name: name.to_string(),
            data: FrameColumnData::Uint8(NumberContainer::new(
                data.into_iter().collect(),
                BitVec::from_slice(&bitvec.into_iter().collect::<Vec<_>>()),
            )),
        }
    }

    fn uint16_column_with_bitvec(
        name: &str,
        data: impl IntoIterator<Item = u128>,
        bitvec: impl IntoIterator<Item = bool>,
    ) -> FrameColumn {
        FrameColumn {
            schema: None,
            table: None,
            name: name.to_string(),
            data: FrameColumnData::Uint16(NumberContainer::new(
                data.into_iter().collect(),
                BitVec::from_slice(&bitvec.into_iter().collect::<Vec<_>>()),
            )),
        }
    }

    fn utf8_column_with_bitvec(
        name: &str,
        data: Vec<String>,
        bitvec: impl IntoIterator<Item = bool>,
    ) -> FrameColumn {
        FrameColumn {
            schema: None,
            table: None,
            name: name.to_string(),
            data: FrameColumnData::Utf8(StringContainer::new(
                data,
                BitVec::from_slice(&bitvec.into_iter().collect::<Vec<_>>()),
            )),
        }
    }

    fn undefined_column(name: &str, len: usize) -> FrameColumn {
        FrameColumn {
            schema: None,
            table: None,
            name: name.to_string(),
            data: FrameColumnData::Undefined(UndefinedContainer::new(len)),
        }
    }

    fn date_column_with_bitvec(
        name: &str,
        data: impl IntoIterator<Item = Date>,
        bitvec: BitVec,
    ) -> FrameColumn {
        FrameColumn {
            schema: None,
            table: None,
            name: name.to_string(),
            data: FrameColumnData::Date(TemporalContainer::new(data.into_iter().collect(), bitvec)),
        }
    }

    fn datetime_column_with_bitvec(
        name: &str,
        data: impl IntoIterator<Item = DateTime>,
        bitvec: BitVec,
    ) -> FrameColumn {
        FrameColumn {
            schema: None,
            table: None,
            name: name.to_string(),
            data: FrameColumnData::DateTime(TemporalContainer::new(
                data.into_iter().collect(),
                bitvec,
            )),
        }
    }

    fn time_column_with_bitvec(
        name: &str,
        data: impl IntoIterator<Item = Time>,
        bitvec: BitVec,
    ) -> FrameColumn {
        FrameColumn {
            schema: None,
            table: None,
            name: name.to_string(),
            data: FrameColumnData::Time(TemporalContainer::new(data.into_iter().collect(), bitvec)),
        }
    }

    fn interval_column_with_bitvec(
        name: &str,
        data: impl IntoIterator<Item = Interval>,
        bitvec: BitVec,
    ) -> FrameColumn {
        FrameColumn {
            schema: None,
            table: None,
            name: name.to_string(),
            data: FrameColumnData::Interval(TemporalContainer::new(
                data.into_iter().collect(),
                bitvec,
            )),
        }
    }

    fn row_id_column_with_bitvec(
        data: impl IntoIterator<Item = RowId>,
        bitvec: BitVec,
    ) -> FrameColumn {
        FrameColumn {
            schema: None,
            table: None,
            name: "__ROW__ID__".to_string(),
            data: FrameColumnData::RowId(RowIdContainer::new(data.into_iter().collect(), bitvec)),
        }
    }

    fn row_id_column(data: impl IntoIterator<Item = RowId>) -> FrameColumn {
        let data_vec: Vec<RowId> = data.into_iter().collect();
        let bitvec = BitVec::repeat(data_vec.len(), true);
        FrameColumn {
            schema: None,
            table: None,
            name: "__ROW__ID__".to_string(),
            data: FrameColumnData::RowId(RowIdContainer::new(data_vec, bitvec)),
        }
    }

    fn blob_column_with_bitvec(
        name: &str,
        data: impl IntoIterator<Item = Blob>,
        bitvec: impl IntoIterator<Item = bool>,
    ) -> FrameColumn {
        FrameColumn {
            schema: None,
            table: None,
            name: name.to_string(),
            data: FrameColumnData::Blob(BlobContainer::new(
                data.into_iter().collect(),
                BitVec::from_slice(&bitvec.into_iter().collect::<Vec<_>>()),
            )),
        }
    }

    fn uuid4_column_with_bitvec(
        name: &str,
        data: impl IntoIterator<Item = Uuid4>,
        bitvec: impl IntoIterator<Item = bool>,
    ) -> FrameColumn {
        FrameColumn {
            schema: None,
            table: None,
            name: name.to_string(),
            data: FrameColumnData::Uuid4(UuidContainer::new(
                data.into_iter().collect(),
                BitVec::from_slice(&bitvec.into_iter().collect::<Vec<_>>()),
            )),
        }
    }

    fn uuid7_column_with_bitvec(
        name: &str,
        data: impl IntoIterator<Item = Uuid7>,
        bitvec: impl IntoIterator<Item = bool>,
    ) -> FrameColumn {
        FrameColumn {
            schema: None,
            table: None,
            name: name.to_string(),
            data: FrameColumnData::Uuid7(UuidContainer::new(
                data.into_iter().collect(),
                BitVec::from_slice(&bitvec.into_iter().collect::<Vec<_>>()),
            )),
        }
    }

    #[test]
    fn test_bool() {
        let frame = Frame::new(vec![bool_column_with_bitvec("bool", [true, false], [true, false])]);
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
        let frame =
            Frame::new(vec![float4_column_with_bitvec("float4", [1.2, 2.5], [true, false])]);
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
        let frame =
            Frame::new(vec![float8_column_with_bitvec("float8", [3.14, 6.28], [true, false])]);
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
        let frame = Frame::new(vec![int1_column_with_bitvec("int1", [1, -1], [true, false])]);
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
        let frame = Frame::new(vec![int2_column_with_bitvec("int2", [100, 200], [true, false])]);
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
        let frame = Frame::new(vec![int4_column_with_bitvec("int4", [1000, 2000], [true, false])]);
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
        let frame =
            Frame::new(vec![int8_column_with_bitvec("int8", [10000, 20000], [true, false])]);
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
            Frame::new(vec![int16_column_with_bitvec("int16", [100000, 200000], [true, false])]);
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
        let frame = Frame::new(vec![uint1_column_with_bitvec("uint1", [1, 2], [true, false])]);
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
        let frame = Frame::new(vec![uint2_column_with_bitvec("uint2", [100, 200], [true, false])]);
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
        let frame =
            Frame::new(vec![uint4_column_with_bitvec("uint4", [1000, 2000], [true, false])]);
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
        let frame =
            Frame::new(vec![uint8_column_with_bitvec("uint8", [10000, 20000], [true, false])]);
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
            Frame::new(vec![uint16_column_with_bitvec("uint16", [100000, 200000], [true, false])]);
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
        let dates =
            vec![Date::from_ymd(2025, 1, 15).unwrap(), Date::from_ymd(2025, 12, 25).unwrap()];
        let frame = Frame::new(vec![date_column_with_bitvec(
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
        let datetimes = vec![
            DateTime::from_timestamp(1642694400).unwrap(),
            DateTime::from_timestamp(1735142400).unwrap(),
        ];
        let frame = Frame::new(vec![datetime_column_with_bitvec(
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
        let times = vec![Time::from_hms(14, 30, 45).unwrap(), Time::from_hms(9, 15, 30).unwrap()];
        let frame = Frame::new(vec![time_column_with_bitvec(
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
        let intervals = vec![Interval::from_days(30), Interval::from_hours(24)];
        let frame = Frame::new(vec![interval_column_with_bitvec(
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
        let frame =
            Frame::new(vec![row_id_column_with_bitvec(ids, BitVec::from_slice(&[true, false]))]);
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
        // Create a frame with regular columns and a RowId column
        let regular_column = utf8_column_with_bitvec(
            "name",
            vec!["Alice".to_string(), "Bob".to_string()],
            [true, true],
        );

        let age_column = int4_column_with_bitvec("age", [25, 30], [true, true]);

        let row_id_column = row_id_column([RowId::new(1), RowId::new(2)]);

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
        // Create a RowId column with one undefined value
        let row_id_column = row_id_column_with_bitvec(
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

    #[test]
    fn test_blob() {
        let blobs = vec![Blob::new(vec![0x01, 0x02, 0x03]), Blob::new(vec![0xFF, 0xEE, 0xDD])];
        let frame = Frame::new(vec![blob_column_with_bitvec("blob", blobs, [true, false])]);
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
            Uuid4::from(::uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()),
            Uuid4::from(::uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440001").unwrap()),
        ];
        let frame = Frame::new(vec![uuid4_column_with_bitvec("uuid4", uuids, [true, false])]);
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
            Uuid7::from(::uuid::Uuid::parse_str("01890a5d-ac96-774b-b9aa-789c0686aaa4").unwrap()),
            Uuid7::from(::uuid::Uuid::parse_str("01890a5d-ac96-774b-b9aa-789c0686aaa5").unwrap()),
        ];
        let frame = Frame::new(vec![uuid7_column_with_bitvec("uuid7", uuids, [true, false])]);
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
