// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use reifydb_core::{
	Row,
	interface::{
		ColumnDef, ColumnPolicyKind, ColumnSaturationPolicy, EncodableKey, FlowNodeId, PrimitiveId,
		ResolvedView, RowKey,
	},
	value::column::{Column, Columns},
};
use reifydb_engine::{ColumnEvaluationContext, StandardColumnEvaluator, TargetColumn, cast_column_data, stack::Stack};
use reifydb_sdk::{FlowChange, FlowDiff};
use reifydb_type::{Fragment, Params, RowNumber};

use crate::{Operator, operator::Operators, transaction::FlowTransaction};

static EMPTY_PARAMS: Params = Params::None;
static EMPTY_STACK: LazyLock<Stack> = LazyLock::new(Stack::new);

/// Coerce columns to match target schema types
fn coerce_columns(columns: &Columns, target_columns: &[ColumnDef]) -> crate::Result<Columns> {
	let row_count = columns.row_count();
	if row_count == 0 {
		return Ok(Columns::empty());
	}

	let mut result_columns = Vec::with_capacity(target_columns.len());

	for target_col in target_columns {
		let target_type = target_col.constraint.get_type();

		// Create context with Undefined saturation policy for this column
		// This ensures overflow during cast produces undefined instead of errors
		// FIXME how to handle failing views ?!
		let ctx = ColumnEvaluationContext {
			target: Some(TargetColumn::Partial {
				source_name: None,
				column_name: Some(target_col.name.clone()),
				column_type: target_type,
				policies: vec![ColumnPolicyKind::Saturation(ColumnSaturationPolicy::Undefined)],
			}),
			columns: columns.clone(),
			row_count,
			take: None,
			params: &EMPTY_PARAMS,
			stack: &EMPTY_STACK,
			is_aggregate_context: false,
		};

		if let Some(source_col) = columns.column(&target_col.name) {
			// Cast to target type
			let casted = cast_column_data(
				&ctx,
				source_col.data(),
				target_type,
				Fragment::internal(&target_col.name),
			)?;
			result_columns.push(Column {
				name: Fragment::internal(&target_col.name),
				data: casted,
			});
		} else {
			result_columns.push(Column::undefined_typed(
				Fragment::internal(&target_col.name),
				target_type,
				row_count,
			))
		}
	}

	// Preserve row numbers
	let row_numbers = columns.row_numbers.iter().cloned().collect();
	Ok(Columns::with_row_numbers(result_columns, row_numbers))
}

fn columns_to_row(columns: &Columns, row_idx: usize) -> Row {
	columns.extract_row(row_idx).to_single_row()
}

pub struct SinkViewOperator {
	parent: Arc<Operators>,
	node: FlowNodeId,
	view: ResolvedView,
}

impl SinkViewOperator {
	pub fn new(parent: Arc<Operators>, node: FlowNodeId, view: ResolvedView) -> Self {
		Self {
			parent,
			node,
			view,
		}
	}
}

#[async_trait]
impl Operator for SinkViewOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	async fn apply(
		&self,
		txn: &mut FlowTransaction,
		change: FlowChange,
		_evaluator: &StandardColumnEvaluator,
	) -> crate::Result<FlowChange> {
		// Write rows to the view storage
		let view_def = self.view.def().clone();

		for diff in change.diffs.iter() {
			match diff {
				FlowDiff::Insert {
					post,
				} => {
					// Coerce columns to match view schema types
					let coerced = coerce_columns(post, &view_def.columns)?;
					let row_count = coerced.row_count();
					for row_idx in 0..row_count {
						let post_row = columns_to_row(&coerced, row_idx);

						let key = RowKey {
							primitive: PrimitiveId::view(view_def.id),
							row: post_row.number,
						}
						.encode();

						txn.set(&key, post_row.encoded)?;
					}
				}
				FlowDiff::Update {
					pre,
					post,
				} => {
					// Coerce columns to match view schema types
					let coerced_pre = coerce_columns(pre, &view_def.columns)?;
					let coerced_post = coerce_columns(post, &view_def.columns)?;
					let row_count = coerced_post.row_count();
					for row_idx in 0..row_count {
						let pre_row = columns_to_row(&coerced_pre, row_idx);
						let post_row = columns_to_row(&coerced_post, row_idx);

						let old_key = RowKey {
							primitive: PrimitiveId::view(view_def.id),
							row: pre_row.number,
						}
						.encode();

						let new_key = RowKey {
							primitive: PrimitiveId::view(view_def.id),
							row: post_row.number,
						}
						.encode();

						txn.remove(&old_key)?;
						txn.set(&new_key, post_row.encoded)?;
					}
				}
				FlowDiff::Remove {
					pre,
				} => {
					// Coerce columns to match view schema types
					let coerced = coerce_columns(pre, &view_def.columns)?;
					let row_count = coerced.row_count();
					for row_idx in 0..row_count {
						let pre_row = columns_to_row(&coerced, row_idx);

						let key = RowKey {
							primitive: PrimitiveId::view(view_def.id),
							row: pre_row.number,
						}
						.encode();

						txn.remove(&key)?;
					}
				}
			}
		}

		Ok(FlowChange::internal(self.node, change.version, Vec::new()))
	}

	async fn pull(&self, _txn: &mut FlowTransaction, _rows: &[RowNumber]) -> crate::Result<Columns> {
		unreachable!()
	}
}
