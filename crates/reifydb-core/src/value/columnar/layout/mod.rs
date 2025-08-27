// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod apply;
mod extend;

use crate::value::columnar::{Column, Columns};

#[derive(Debug, Clone)]
pub struct ColumnsLayout {
	pub columns: Vec<ColumnLayout>,
}

impl ColumnsLayout {
	pub fn from_columns(columns: &Columns) -> Self {
		Self {
			columns: columns
				.iter()
				.map(|c| ColumnLayout::from_column(c))
				.collect(),
		}
	}
}

#[derive(Debug, Clone)]
pub struct ColumnLayout {
	pub schema: Option<String>,
	pub source: Option<String>,
	pub name: String,
}

impl ColumnLayout {
	pub fn from_column(column: &Column) -> Self {
		Self {
			schema: column.schema().map(|s| s.to_string()),
			source: column.source().map(|s| s.to_string()),
			name: column.name().to_string(),
		}
	}
}
