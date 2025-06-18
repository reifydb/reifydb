// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::frame::{ColumnValues, Frame};
use std::fmt::{self, Display, Formatter};

impl Display for Frame {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let row_count = self.columns.first().map_or(0, |c| c.data.len());
        let col_count = self.columns.len();

        let mut col_widths = vec![0; col_count];

        for (i, col) in self.columns.iter().enumerate() {
            col_widths[i] = col.name.len();
        }

        for row_idx in 0..row_count {
            for (i, col) in self.columns.iter().enumerate() {
                let s = match &col.data {
                    ColumnValues::Bool(v, valid) => {
                        if valid[row_idx] {
                            v[row_idx].to_string()
                        } else {
                            "Undefined".into()
                        }
                    }
                    ColumnValues::Float4(v, valid) => {
                        if valid[row_idx] {
                            v[row_idx].to_string()
                        } else {
                            "Undefined".into()
                        }
                    }
                    ColumnValues::Float8(v, valid) => {
                        if valid[row_idx] {
                            v[row_idx].to_string()
                        } else {
                            "Undefined".into()
                        }
                    }
                    ColumnValues::Int1(v, valid) => {
                        if valid[row_idx] {
                            v[row_idx].to_string()
                        } else {
                            "Undefined".into()
                        }
                    }
                    ColumnValues::Int2(v, valid) => {
                        if valid[row_idx] {
                            v[row_idx].to_string()
                        } else {
                            "Undefined".into()
                        }
                    }
                    ColumnValues::Int4(v, valid) => {
                        if valid[row_idx] {
                            v[row_idx].to_string()
                        } else {
                            "Undefined".into()
                        }
                    }
                    ColumnValues::Int8(v, valid) => {
                        if valid[row_idx] {
                            v[row_idx].to_string()
                        } else {
                            "Undefined".into()
                        }
                    }
                    ColumnValues::Int16(v, valid) => {
                        if valid[row_idx] {
                            v[row_idx].to_string()
                        } else {
                            "Undefined".into()
                        }
                    }
                    ColumnValues::Uint1(v, valid) => {
                        if valid[row_idx] {
                            v[row_idx].to_string()
                        } else {
                            "Undefined".into()
                        }
                    }
                    ColumnValues::Uint2(v, valid) => {
                        if valid[row_idx] {
                            v[row_idx].to_string()
                        } else {
                            "Undefined".into()
                        }
                    }
                    ColumnValues::Uint4(v, valid) => {
                        if valid[row_idx] {
                            v[row_idx].to_string()
                        } else {
                            "Undefined".into()
                        }
                    }
                    ColumnValues::Uint8(v, valid) => {
                        if valid[row_idx] {
                            v[row_idx].to_string()
                        } else {
                            "Undefined".into()
                        }
                    }
                    ColumnValues::Uint16(v, valid) => {
                        if valid[row_idx] {
                            v[row_idx].to_string()
                        } else {
                            "Undefined".into()
                        }
                    }
                    ColumnValues::String(v, valid) => {
                        if valid[row_idx] {
                            v[row_idx].clone()
                        } else {
                            "Undefined".into()
                        }
                    }
                    ColumnValues::Undefined(_) => "Undefined".into(),
                };
                col_widths[i] = col_widths[i].max(s.len());
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
                let pad = w - name.len();
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
                    let s = match &col.data {
                        ColumnValues::Bool(v, valid) => {
                            if valid[row_idx] {
                                v[row_idx].to_string()
                            } else {
                                "Undefined".into()
                            }
                        }
                        ColumnValues::Float4(v, valid) => {
                            if valid[row_idx] {
                                v[row_idx].to_string()
                            } else {
                                "Undefined".into()
                            }
                        }
                        ColumnValues::Float8(v, valid) => {
                            if valid[row_idx] {
                                v[row_idx].to_string()
                            } else {
                                "Undefined".into()
                            }
                        }
                        ColumnValues::Int1(v, valid) => {
                            if valid[row_idx] {
                                v[row_idx].to_string()
                            } else {
                                "Undefined".into()
                            }
                        }
                        ColumnValues::Int2(v, valid) => {
                            if valid[row_idx] {
                                v[row_idx].to_string()
                            } else {
                                "Undefined".into()
                            }
                        }
                        ColumnValues::Int4(v, valid) => {
                            if valid[row_idx] {
                                v[row_idx].to_string()
                            } else {
                                "Undefined".into()
                            }
                        }
                        ColumnValues::Int8(v, valid) => {
                            if valid[row_idx] {
                                v[row_idx].to_string()
                            } else {
                                "Undefined".into()
                            }
                        }
                        ColumnValues::Int16(v, valid) => {
                            if valid[row_idx] {
                                v[row_idx].to_string()
                            } else {
                                "Undefined".into()
                            }
                        }
                        ColumnValues::Uint1(v, valid) => {
                            if valid[row_idx] {
                                v[row_idx].to_string()
                            } else {
                                "Undefined".into()
                            }
                        }
                        ColumnValues::Uint2(v, valid) => {
                            if valid[row_idx] {
                                v[row_idx].to_string()
                            } else {
                                "Undefined".into()
                            }
                        }
                        ColumnValues::Uint4(v, valid) => {
                            if valid[row_idx] {
                                v[row_idx].to_string()
                            } else {
                                "Undefined".into()
                            }
                        }
                        ColumnValues::Uint8(v, valid) => {
                            if valid[row_idx] {
                                v[row_idx].to_string()
                            } else {
                                "Undefined".into()
                            }
                        }
                        ColumnValues::Uint16(v, valid) => {
                            if valid[row_idx] {
                                v[row_idx].to_string()
                            } else {
                                "Undefined".into()
                            }
                        }
                        ColumnValues::String(v, valid) => {
                            if valid[row_idx] {
                                v[row_idx].clone()
                            } else {
                                "Undefined".into()
                            }
                        }
                        ColumnValues::Undefined(_) => "Undefined".into(),
                    };
                    let pad = w - s.len();
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
    use crate::frame::Column;

    #[test]
    fn test_bool() {
        let frame =
            Frame::new(vec![Column::bool_with_validity("bool", [true, false], [true, false])]);
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
            Frame::new(vec![Column::float4_with_validity("float4", [1.2, 2.5], [true, false])]);
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
        let frame =
            Frame::new(vec![Column::float8_with_validity("float8", [3.14, 6.28], [true, false])]);
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
        let frame = Frame::new(vec![Column::int1_with_validity("int1", [1, -1], [true, false])]);
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
        let frame = Frame::new(vec![Column::int2_with_validity("int2", [100, 200], [true, false])]);
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
            Frame::new(vec![Column::int4_with_validity("int4", [1000, 2000], [true, false])]);
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
            Frame::new(vec![Column::int8_with_validity("int8", [10000, 20000], [true, false])]);
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
            Frame::new(vec![Column::int16_with_validity("int16", [100000, 200000], [true, false])]);
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
        let frame = Frame::new(vec![Column::uint1_with_validity("uint1", [1, 2], [true, false])]);
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
            Frame::new(vec![Column::uint2_with_validity("uint2", [100, 200], [true, false])]);
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
            Frame::new(vec![Column::uint4_with_validity("uint4", [1000, 2000], [true, false])]);
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
            Frame::new(vec![Column::uint8_with_validity("uint8", [10000, 20000], [true, false])]);
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
        let frame = Frame::new(vec![Column::uint16_with_validity(
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
        let frame =
            Frame::new(vec![Column::string_with_validity("string", ["foo", "bar"], [true, false])]);
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
        let frame = Frame::new(vec![Column::undefined("undefined", 2)]);
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
}
