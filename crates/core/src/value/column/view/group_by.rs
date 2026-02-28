// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use indexmap::IndexMap;
use reifydb_type::{Result, error::Error, value::Value};

use crate::{
	error::CoreError,
	value::column::{ColumnData, columns::Columns},
};

pub type GroupKey = Vec<Value>;
pub type GroupByView = IndexMap<GroupKey, Vec<usize>>;

impl Columns {
	pub fn group_by_view(&self, keys: &[&str]) -> Result<GroupByView> {
		let row_count = self.first().map_or(0, |c| c.data().len());

		let mut key_columns: Vec<&ColumnData> = Vec::with_capacity(keys.len());

		for &key in keys {
			let column = self.iter().find(|c| c.name() == key).ok_or_else(|| {
				let err: Error = CoreError::FrameError {
					message: format!("Column '{}' not found", key),
				}
				.into();
				err
			})?;
			key_columns.push(&column.data());
		}

		let mut result = GroupByView::new();

		for row_numberx in 0..row_count {
			let mut values = Vec::with_capacity(keys.len());

			for col in &key_columns {
				let value = col.get_value(row_numberx);
				values.push(value);
			}

			result.entry(values).or_default().push(row_numberx);
		}

		Ok(result)
	}
}
