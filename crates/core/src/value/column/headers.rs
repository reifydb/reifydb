// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::Fragment;

use crate::value::column::Columns;

#[derive(Debug, Clone)]
pub struct ColumnHeaders<'a> {
	pub columns: Vec<Fragment<'a>>,
}

impl<'a> ColumnHeaders<'a> {
	pub fn from_columns(columns: &Columns<'a>) -> Self {
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
