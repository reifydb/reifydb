// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{iter::once, sync::Arc};

use arrow_schema::{Schema, SchemaRef};
use reifydb_core::{
	interface::{
		catalog::{flow::OperatorId, series::Series},
		change::{Change, Diff},
		flow::OperatorCapability,
	},
	value::column::builder::ColumnBuilder,
};
use reifydb_value::Result;

use crate::operator::{HostOperator, host::HostContext, sink::decode_dictionary_columns};

pub struct SourceSeriesOperator {
	operator: OperatorId,
	schema: SchemaRef,
}

impl SourceSeriesOperator {
	pub fn new(operator: OperatorId) -> Self {
		Self {
			operator,
			schema: Arc::new(Schema::empty()),
		}
	}

	pub fn with_series(mut self, series: &Series) -> Self {
		let key = series.key_column_data(vec![]).0;
		let data = series
			.data_columns()
			.map(|col| ColumnBuilder::with_capacity(col.constraint.get_type(), 0).finish(&col.name).0);
		self.schema = Arc::new(Schema::new(once(key).chain(data).collect::<Vec<_>>()));
		self
	}
}

impl HostOperator for SourceSeriesOperator {
	fn id(&self) -> OperatorId {
		self.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn apply(&mut self, host: &mut dyn HostContext, change: Change) -> Result<Change> {
		let mut decoded_diffs = Vec::with_capacity(change.diffs.len());
		for diff in change.diffs {
			decoded_diffs.push(match diff {
				Diff::Insert {
					post,
					..
				} => {
					let mut decoded = post;
					decode_dictionary_columns(&mut decoded, host)?;
					Diff::insert(decoded)
				}
				Diff::Update {
					pre,
					post,
					..
				} => {
					let mut decoded_pre = pre;
					let mut decoded_post = post;
					decode_dictionary_columns(&mut decoded_pre, host)?;
					decode_dictionary_columns(&mut decoded_post, host)?;
					Diff::update(decoded_pre, decoded_post)
				}
				Diff::Remove {
					pre,
					..
				} => {
					let mut decoded = pre;
					decode_dictionary_columns(&mut decoded, host)?;
					Diff::remove(decoded)
				}
			});
		}
		Ok(Change::from_flow(self.operator, change.version, decoded_diffs, change.changed_at))
	}

	fn output_schema(&self) -> Option<SchemaRef> {
		Some(self.schema.clone())
	}
}
