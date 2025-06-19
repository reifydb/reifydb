// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::{CreateSchemaResult, CreateTableResult};
use crate::{Column, ExecutionResult};
use reifydb_core::Value;
use std::fmt::{Display, Formatter};

impl Display for ExecutionResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutionResult::CreateDeferredView { schema, view, .. } => {
                write!(f, "deferred view {view} created in schema {schema}")
            }
            ExecutionResult::CreateSchema(CreateSchemaResult { schema, created, .. }) => {
                if *created {
                    write!(f, "schema {schema} created")
                } else {
                    write!(f, "schema {schema} already exists")
                }
            }
            ExecutionResult::CreateSeries { schema, series, .. } => {
                write!(f, "series {series} created in schema {schema}")
            }
            ExecutionResult::CreateTable(CreateTableResult { schema, table, .. }) => {
                write!(f, "table {table} created in schema {schema}")
            }
            ExecutionResult::InsertIntoSeries { schema, series, inserted } => {
                if *inserted != 1 {
                    write!(f, "inserted {inserted} rows into series {series} in schema {schema}")
                } else {
                    write!(f, "inserted 1 row into series {series} created in schema {schema}")
                }
            }
            ExecutionResult::InsertIntoTable { schema, table, inserted } => {
                if *inserted != 1 {
                    write!(f, "inserted {inserted} rows into table {table} in schema {schema}")
                } else {
                    write!(f, "inserted 1 row into table {table} created in schema {schema}")
                }
            }
            ExecutionResult::Query { columns, rows } => print_query(columns, rows, f),
            ExecutionResult::DescribeQuery { columns } => print_describe_query(columns, f),
        }
    }
}

pub fn print_describe_query(columns: &Vec<Column>, f: &mut Formatter<'_>) -> std::fmt::Result {
    let headers = ["name", "kind"];
    let mut col_widths = vec![headers[0].len(), headers[1].len()];

    // Measure max widths for name and kind
    for column in columns {
        col_widths[0] = col_widths[0].max(column.name.len());
        col_widths[1] = col_widths[1].max(format!("{:?}", column.kind).len());
    }

    // Add padding
    for width in &mut col_widths {
        *width += 2;
    }

    let separator =
        format!("+{}+", col_widths.iter().map(|w| "-".repeat(*w)).collect::<Vec<_>>().join("+"));

    let print_row = |name: &str, kind: &str| {
        let cells = [name, kind]
            .iter()
            .enumerate()
            .map(|(i, cell)| {
                let w = col_widths[i] - 2;
                let padding = w.saturating_sub(cell.len());
                let left = padding / 2;
                let right = padding - left;
                format!(" {:left$}{}{:right$} ", "", cell, "", left = left, right = right)
            })
            .collect::<Vec<_>>();
        format!("|{}|", cells.join("|"))
    };

    writeln!(f, "{}", separator)?;
    writeln!(f, "{}", print_row(headers[0], headers[1]))?;
    writeln!(f, "{}", separator)?;
    for column in columns {
        writeln!(f, "{}", print_row(&column.name, &format!("{:?}", column.kind)))?;
    }
    write!(f, "{}", separator)
}

fn print_query(
    labels: &Vec<Column>,
    rows: &Vec<Vec<Value>>,
    f: &mut Formatter<'_>,
) -> std::fmt::Result {
    let num_cols = labels.len();
    let mut col_widths = vec![0; num_cols];

    // Measure column widths
    for (i, column) in labels.iter().enumerate() {
        col_widths[i] = column.name.len();
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

    let print_header_row = |row: &[&str]| {
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
        let cells = row.iter().enumerate().map(|(i, cell)| {
            let w = col_widths[i] - 2;
            let padding = w.saturating_sub(cell.len());
            let left = padding / 2;
            let right = padding - left;
            format!(" {:left$}{}{:right$} ", "", cell, "", left = left, right = right)
        });
        format!("|{}|", cells.collect::<Vec<_>>().join("|"))
    };

    writeln!(f, "{}", separator)?;
    writeln!(
        f,
        "{}",
        print_header_row(&labels.iter().map(|column| column.name.as_str()).collect::<Vec<_>>())
    )?;
    writeln!(f, "{}", separator)?;
    for row in rows {
        writeln!(f, "{}", print_row(&row.iter().map(|v| v.to_string()).collect::<Vec<_>>()))?;
    }
    write!(f, "{}", separator)
}
