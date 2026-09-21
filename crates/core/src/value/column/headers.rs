// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::fragment::Fragment;

use crate::value::column::columns::Columns;

#[derive(Debug, Clone)]
pub struct ColumnHeaders {
	pub columns: Vec<Fragment>,
	pub row_numbers: bool,
}

impl ColumnHeaders {
	pub fn from_columns(columns: &Columns) -> Self {
		Self {
			columns: columns.iter().map(|c| c.name().clone()).collect(),
			row_numbers: columns.system.has_row_numbers(),
		}
	}

	pub fn empty() -> Self {
		Self {
			columns: Vec::new(),
			row_numbers: false,
		}
	}
}
