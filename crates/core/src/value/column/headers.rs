// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::Fragment;

use crate::value::column::Columns;

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
