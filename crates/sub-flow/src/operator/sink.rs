// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::{
	Row,
	interface::{EncodableKey, FlowNodeId, ResolvedView, RowKey, SourceId},
};
use reifydb_engine::StandardRowEvaluator;
use reifydb_flow_operator_sdk::{FlowChange, FlowDiff};
use reifydb_type::RowNumber;

use crate::{Operator, operator::Operators, transaction::FlowTransaction};

pub struct SinkViewOperator {
	parent: Arc<Operators>,
	node: FlowNodeId,
	view: ResolvedView<'static>,
}

impl SinkViewOperator {
	pub fn new(parent: Arc<Operators>, node: FlowNodeId, view: ResolvedView<'static>) -> Self {
		Self {
			parent,
			node,
			view,
		}
	}
}

impl Operator for SinkViewOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(
		&self,
		txn: &mut FlowTransaction,
		change: FlowChange,
		evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		// Transform rows to match the view's schema before writing
		let target_columns = self.view.columns();

		for diff in change.diffs.iter() {
			match diff {
				FlowDiff::Insert {
					post,
					..
				} => {
					let transformed_row = evaluator.coerce(post, target_columns)?;

					let row_id = post.number;
					let row = transformed_row.encoded;

					let key = RowKey {
						source: SourceId::view(self.view.def().id),
						row: row_id,
					}
					.encode();

					txn.set(&key, row)?;
				}
				FlowDiff::Update {
					pre,
					post,
					..
				} => {
					// Transform the encoded to match the view schema
					let transformed_row = evaluator.coerce(post, target_columns)?;

					let key = RowKey {
						source: SourceId::view(self.view.def().id),
						row: post.number,
					}
					.encode();

					txn.remove(&RowKey {
						source: SourceId::view(self.view.def().id),
						row: pre.number,
					}
					.encode())?;

					txn.set(&key, transformed_row.encoded)?;
				}
				FlowDiff::Remove {
					pre,
					..
				} => {
					let key = RowKey {
						source: SourceId::view(self.view.def().id),
						row: pre.number,
					}
					.encode();

					txn.remove(&key)?;
				}
			}
		}

		Ok(FlowChange::internal(self.node, change.version, Vec::new()))
	}

	fn get_rows(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> crate::Result<Vec<Option<Row>>> {
		unreachable!()
	}
}
