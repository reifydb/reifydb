// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::ExecutionResult;
use base::{Label, Row};
use std::fmt::{Display, Formatter};

impl Display for ExecutionResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutionResult::CreateSchema { schema } => {
                write!(f, "schema {schema} created")
            }
            ExecutionResult::CreateTable { schema, table, .. } => {
                write!(f, "table {table} created in schema {schema}")
            }
            ExecutionResult::InsertIntoTable { schema, table, inserted } => {
                if *inserted != 1 {
                    write!(f, "inserted {inserted} rows into table {table} in schema {schema}")
                } else {
                    write!(f, "inserted 1 row into table {table} created in schema {schema}")
                }
            }
            ExecutionResult::Query { labels, rows } => print_query(labels, rows, f),
        }
    }
}

fn print_query(labels: &Vec<Label>, rows: &Vec<Row>, f: &mut Formatter<'_>) -> std::fmt::Result {
    let num_cols = labels.len();
    let mut col_widths = vec![0; num_cols];

    // Measure label widths
    for (i, label) in labels.iter().enumerate() {
        col_widths[i] = label.to_string().len();
    }

    // Measure row value widths
    for row in rows {
        for (i, val) in row.iter().enumerate() {
            col_widths[i] = col_widths[i].max(val.to_string().len());
        }
    }

    // Add fixed padding to each column
    for width in &mut col_widths {
        *width += 2;
    }

    let separator =
        format!("+{}+", col_widths.iter().map(|w| "-".repeat(*w)).collect::<Vec<_>>().join("+"));

    let print_header_row = |row: &[String]| {
        let cells = row.iter().enumerate().map(|(i, cell)| {
            let w = col_widths[i] - 2;
            let padding = w.saturating_sub(cell.len());
            let left = padding / 2;
            let right = padding - left;
            format!(" {:left$}{}{:right$} ", "", cell, "", left = left, right = right)
        });
        format!("|{}|", cells.collect::<Vec<_>>().join("|"))
    };

    let print_row = |row: &[String]| {
        format!(
            "|{}|",
            row.iter()
                .enumerate()
                .map(|(i, cell)| format!(" {:<width$} ", cell, width = col_widths[i] - 2))
                .collect::<Vec<_>>()
                .join("|")
        )
    };

    writeln!(f, "{}", separator)?;
    writeln!(f, "{}", print_header_row(&labels.iter().map(|l| l.to_string()).collect::<Vec<_>>()))?;
    writeln!(f, "{}", separator)?;
    for row in rows {
        writeln!(f, "{}", print_row(&row.iter().map(|v| v.to_string()).collect::<Vec<_>>()))?;
    }
    write!(f, "{}", separator)
}
