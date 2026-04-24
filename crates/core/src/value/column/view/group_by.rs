// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use indexmap::IndexMap;
use reifydb_type::{Result, error::Error, value::Value};

use crate::{
	error::CoreError,
	value::column::{ColumnBuffer, columns::Columns},
};

pub type GroupKey = Vec<Value>;
pub type GroupByView = IndexMap<GroupKey, Vec<usize>>;

impl Columns {
	pub fn group_by_view(&self, keys: &[&str]) -> Result<GroupByView> {
		let row_count = self.columns.first().map_or(0, |c| c.len());

		let mut key_columns: Vec<&ColumnBuffer> = Vec::with_capacity(keys.len());

		for &key in keys {
			let pos = self.names.iter().position(|n| n.text() == key).ok_or_else(|| {
				let err: Error = CoreError::FrameError {
					message: format!("Column '{}' not found", key),
				}
				.into();
				err
			})?;
			key_columns.push(&self.columns[pos]);
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
