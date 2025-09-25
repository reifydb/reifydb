// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod apply;
mod extend;

use reifydb_type::Fragment;

use crate::value::column::{Column, Columns};

#[derive(Debug, Clone)]
pub struct ColumnsLayout<'a> {
	pub columns: Vec<ColumnLayout<'a>>,
}

impl<'a> ColumnsLayout<'a> {
	pub fn from_columns(columns: &Columns<'a>) -> Self {
		Self {
			columns: columns.iter().map(|c| ColumnLayout::from_column(c)).collect(),
		}
	}
}

#[derive(Debug, Clone)]
pub struct ColumnLayout<'a> {
	pub namespace: Option<Fragment<'a>>,
	pub source: Option<Fragment<'a>>,
	pub name: Fragment<'a>,
}

impl<'a> ColumnLayout<'a> {
	pub fn from_column(column: &Column<'a>) -> Self {
		Self {
			namespace: None,
			source: None,
			name: column.name().clone(),
		}
	}
}
