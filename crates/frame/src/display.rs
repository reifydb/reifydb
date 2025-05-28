// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{ColumnValues, Frame};
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
                    ColumnValues::Float8(v, valid) => {
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
                    ColumnValues::Text(v, valid) => {
                        if valid[row_idx] {
                            v[row_idx].clone()
                        } else {
                            "Undefined".into()
                        }
                    }
                    ColumnValues::Bool(v, valid) => {
                        if valid[row_idx] {
                            v[row_idx].to_string()
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
                        ColumnValues::Float8(v, valid) => {
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
                        ColumnValues::Text(v, valid) => {
                            if valid[row_idx] {
                                v[row_idx].clone()
                            } else {
                                "Undefined".into()
                            }
                        }
                        ColumnValues::Bool(v, valid) => {
                            if valid[row_idx] {
                                v[row_idx].to_string()
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
    #[test]
    #[ignore]
    fn implement() {
        todo!()
    }
}
