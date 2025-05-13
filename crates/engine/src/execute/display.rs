// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::ExecutionResult;
use base::{Label, Row};
use std::fmt::{Display, Formatter};

impl Display for ExecutionResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutionResult::CreateSchema { schema } => {
                write!(f, "schema created: {schema}")
            }
            ExecutionResult::CreateTable { .. } => todo!(),
            ExecutionResult::InsertIntoTable { .. } => todo!(),
            ExecutionResult::Query { labels, rows } => print_query(labels, rows, f),
        }
    }
}

fn print_query(labels: &Vec<Label>, rows: &Vec<Row>, f: &mut Formatter<'_>) -> std::fmt::Result {
    let mut out = String::new();
    
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
    
    // uses string instead of writeln! because otherwise it causes some weird formatting in the tests
    
    out += separator.as_str();
    out += "\n";
    out += print_header_row(&labels.iter().map(|l| l.to_string()).collect::<Vec<_>>()).as_str();
    out += "\n";
    out += separator.as_str();
    out += "\n";
    
    for row in rows {
        out += print_row(&row.iter().map(|v| v.to_string()).collect::<Vec<_>>()).as_str();
        out += "\n";
    }
    
    out += separator.as_str();
    Display::fmt(&out, f)
}
