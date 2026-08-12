// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::{
		catalog::{flow::OperatorId, table::Table},
		change::{Change, Diff},
		flow::OperatorCapability,
	},
	value::column::columns::Columns,
};
use reifydb_value::Result;

use crate::operator::{Operator, bridge::Bridge, sink::decode_dictionary_columns};

pub struct SourceTableOperator {
	operator: OperatorId,
	table: Table,
}

impl SourceTableOperator {
	pub fn new(operator: OperatorId, table: Table) -> Self {
		Self {
			operator,
			table,
		}
	}
}

impl Operator for SourceTableOperator {
	fn id(&self) -> OperatorId {
		self.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn apply(&mut self, bridge: &mut dyn Bridge, change: Change) -> Result<Change> {
		let mut decoded_diffs = Vec::with_capacity(change.diffs.len());
		for diff in change.diffs {
			decoded_diffs.push(match diff {
				Diff::Insert {
					post,
					..
				} => {
					let mut decoded = post;
					decode_dictionary_columns(&mut decoded, bridge)?;
					Diff::insert(decoded)
				}
				Diff::Update {
					pre,
					post,
					..
				} => {
					let mut decoded_pre = pre;
					let mut decoded_post = post;
					decode_dictionary_columns(&mut decoded_pre, bridge)?;
					decode_dictionary_columns(&mut decoded_post, bridge)?;
					Diff::update(decoded_pre, decoded_post)
				}
				Diff::Remove {
					pre,
					..
				} => {
					let mut decoded = pre;
					decode_dictionary_columns(&mut decoded, bridge)?;
					Diff::remove(decoded)
				}
			});
		}
		Ok(Change::from_flow(self.operator, change.version, decoded_diffs, change.changed_at))
	}

	fn output_schema(&self) -> Option<Columns> {
		Some(self.output_schema())
	}
}

impl SourceTableOperator {
	pub fn output_schema(&self) -> Columns {
		Columns::from_catalog_columns(&self.table.columns)
	}
}
