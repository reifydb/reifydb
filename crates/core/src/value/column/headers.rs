// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_value::fragment::Fragment;

use crate::value::column::columns::Columns;

#[derive(Debug, Clone)]
pub struct ColumnHeaders {
	pub columns: Vec<Fragment>,
}

impl ColumnHeaders {
	pub fn from_columns(columns: &Columns) -> Self {
		Self {
			columns: columns.iter().map(|c| c.name().clone()).collect(),
		}
	}

	pub fn empty() -> Self {
		Self {
			columns: Vec::new(),
		}
	}
}
