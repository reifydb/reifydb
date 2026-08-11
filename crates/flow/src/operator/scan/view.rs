// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::{
		catalog::{flow::OperatorId, view::View},
		change::{Change, Diff},
		flow::OperatorCapability,
	},
	value::column::columns::Columns,
};
use reifydb_value::Result;

use crate::{
	operator::{Operator, sink::decode_dictionary_columns},
	transaction::FlowTransaction,
};

pub struct SourceViewOperator {
	operator: OperatorId,
	view: View,
}

impl SourceViewOperator {
	pub fn new(operator: OperatorId, view: View) -> Self {
		Self {
			operator,
			view,
		}
	}
}

impl<T: FlowTransaction> Operator<T> for SourceViewOperator {
	fn id(&self) -> OperatorId {
		self.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn apply(&self, txn: &mut T, change: Change) -> Result<Change> {
		let mut decoded_diffs = Vec::with_capacity(change.diffs.len());
		for diff in change.diffs {
			decoded_diffs.push(match diff {
				Diff::Insert {
					post,
					..
				} => {
					let mut decoded = post;
					decode_dictionary_columns(&mut decoded, txn)?;
					Diff::insert(decoded)
				}
				Diff::Update {
					pre,
					post,
					..
				} => {
					let mut decoded_pre = pre;
					let mut decoded_post = post;
					decode_dictionary_columns(&mut decoded_pre, txn)?;
					decode_dictionary_columns(&mut decoded_post, txn)?;
					Diff::update(decoded_pre, decoded_post)
				}
				Diff::Remove {
					pre,
					..
				} => {
					let mut decoded = pre;
					decode_dictionary_columns(&mut decoded, txn)?;
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

impl SourceViewOperator {
	pub fn output_schema(&self) -> Columns {
		Columns::from_catalog_columns(self.view.columns())
	}
}
