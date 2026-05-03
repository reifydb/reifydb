// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::params::Params;

use crate::bulk_insert::builder::{BulkInsertBuilder, ValidationMode};

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

pub struct SeriesInsertBuilder<'a, 'e, V: ValidationMode> {
	parent: &'a mut BulkInsertBuilder<'e, V>,
	pending: PendingSeriesInsert,
}

impl<'a, 'e, V: ValidationMode> SeriesInsertBuilder<'a, 'e, V> {
	pub(crate) fn new(parent: &'a mut BulkInsertBuilder<'e, V>, namespace: String, series: String) -> Self {
		Self {
			parent,
			pending: PendingSeriesInsert::new(namespace, series),
		}
	}

	pub fn row(mut self, params: Params) -> Self {
		self.pending.add_row(params);
		self
	}

	pub fn rows<I>(mut self, iter: I) -> Self
	where
		I: IntoIterator<Item = Params>,
	{
		self.pending.add_rows(iter);
		self
	}

	pub fn done(self) -> &'a mut BulkInsertBuilder<'e, V> {
		self.parent.add_series_insert(self.pending);
		self.parent
	}
}
