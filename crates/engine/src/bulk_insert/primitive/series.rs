// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::params::Params;

use crate::bulk_insert::builder::{BulkInsertBuilder, ValidationMode};

/// Buffered series insert operation
#[derive(Debug, Clone)]
pub struct PendingSeriesInsert {
	pub namespace: String,
	pub series: String,
	pub rows: Vec<Params>,
}

impl PendingSeriesInsert {
	pub fn new(namespace: String, series: String) -> Self {
		Self {
			namespace,
			series,
			rows: Vec::new(),
		}
	}

	pub fn add_row(&mut self, params: Params) {
		self.rows.push(params);
	}

	pub fn add_rows<I: IntoIterator<Item = Params>>(&mut self, iter: I) {
		self.rows.extend(iter);
	}
}

/// Builder for inserting rows into a specific series.
///
/// Created by calling `series()` on a `BulkInsertBuilder`.
/// Call `done()` to finish and return to the main builder.
pub struct SeriesInsertBuilder<'a, 'e, V: ValidationMode> {
	parent: &'a mut BulkInsertBuilder<'e, V>,
	pending: PendingSeriesInsert,
}

impl<'a, 'e, V: ValidationMode> SeriesInsertBuilder<'a, 'e, V> {
	/// Create a new series insert builder.
	pub(crate) fn new(parent: &'a mut BulkInsertBuilder<'e, V>, namespace: String, series: String) -> Self {
		Self {
			parent,
			pending: PendingSeriesInsert::new(namespace, series),
		}
	}

	/// Add a single row from named params.
	///
	/// # Example
	///
	/// ```ignore
	/// builder.row(params!{ timestamp: 12345, value: 0.42 })
	/// ```
	pub fn row(mut self, params: Params) -> Self {
		self.pending.add_row(params);
		self
	}

	/// Add multiple rows from an iterator.
	///
	/// # Example
	///
	/// ```ignore
	/// let rows = vec![
	///     params!{ timestamp: 12345, value: 0.42 },
	///     params!{ timestamp: 12346, value: 0.51 },
	/// ];
	/// builder.rows(rows)
	/// ```
	pub fn rows<I>(mut self, iter: I) -> Self
	where
		I: IntoIterator<Item = Params>,
	{
		self.pending.add_rows(iter);
		self
	}

	/// Finish this series insert and return to the main builder.
	///
	/// This allows chaining to insert into additional targets.
	pub fn done(self) -> &'a mut BulkInsertBuilder<'e, V> {
		self.parent.add_series_insert(self.pending);
		self.parent
	}
}
