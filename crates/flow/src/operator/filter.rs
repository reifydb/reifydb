// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use reifydb_core::{
	expression::Expression,
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff},
	},
	internal_err,
	value::batch::{empty_batch, take_rows},
};
use reifydb_evaluate::{
	expression::{
		compile::compile_expression,
		context::{CompileContext, EvalContext},
	},
	lower::LoweredExpr,
};
use reifydb_routine_abi::registry::Routines;
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	Result,
	value::{Value, column_view::ColumnView, value_type::ValueType},
};
use tracing::instrument;

use crate::{context::FlowContext, operator::forward_system_columns};

pub struct FilterOperator {
	parent_schema: Option<SchemaRef>,
	operator: OperatorId,
	lowered_conditions: Vec<LoweredExpr>,
	routines: Routines,
	runtime_context: RuntimeContext,
	ctx: Arc<FlowContext>,
}

impl FilterOperator {
	pub fn new(
		parent_schema: Option<SchemaRef>,
		operator: OperatorId,
		conditions: Vec<Expression>,
		routines: Routines,
		runtime_context: RuntimeContext,
		ctx: Arc<FlowContext>,
	) -> Result<Self> {
		let compile_ctx = CompileContext {
			symbols: &ctx.symbols,
		};
		for condition in &conditions {
			compile_expression(&compile_ctx, condition)?;
		}
		let lowered_conditions: Vec<LoweredExpr> =
			conditions.into_iter().map(|condition| LoweredExpr::new(condition, "filter")).collect();

		Ok(Self {
			parent_schema,
			operator,
			lowered_conditions,
			routines,
			runtime_context,
			ctx,
		})
	}

	#[instrument(name = "flow::operator::filter::evaluate", level = "trace", skip_all, fields(rows = columns.num_rows()))]
	fn evaluate(&self, columns: &RecordBatch) -> Result<Vec<bool>> {
		let row_count = columns.num_rows();
		if row_count == 0 {
			return Ok(Vec::new());
		}

		let session = EvalContext {
			params: &self.ctx.params,
			symbols: &self.ctx.symbols,
			routines: &self.routines,
			runtime_context: &self.runtime_context,
			identity: self.ctx.identity,
			is_aggregate_context: false,
			batch: empty_batch(),
			row_count: 1,
			target: None,
			take: None,
		};
		let exec_ctx = session.with_eval(columns.clone(), row_count);

		let mut mask = vec![true; row_count];

		for lowered_condition in &self.lowered_conditions {
			let result = lowered_condition.evaluate(&exec_ctx)?;
			let result_col = ColumnView::try_from(&result)?;

			for (row_idx, mask_val) in mask.iter_mut().enumerate() {
				if *mask_val {
					match result_col.get_value(row_idx) {
						Value::Boolean(true) => {}
						Value::Boolean(false) => *mask_val = false,
						Value::None {
							inner: ValueType::Boolean,
						} => *mask_val = false,
						result => {
							return internal_err!(
								"Filter condition did not evaluate to boolean, got: {:?}",
								result
							);
						}
					}
				}
			}
		}

		Ok(mask)
	}

	#[instrument(name = "flow::operator::filter::passing", level = "trace", skip_all, fields(rows = columns.num_rows()))]
	fn filter_passing(&self, columns: &RecordBatch, mask: &[bool]) -> Result<RecordBatch> {
		let passing_indices: Vec<usize> =
			mask.iter().enumerate().filter(|&(_, pass)| *pass).map(|(idx, _)| idx).collect();

		if passing_indices.is_empty() {
			Ok(empty_batch())
		} else {
			extract(columns, &passing_indices)
		}
	}
}

impl FilterOperator {
	pub fn id(&self) -> OperatorId {
		self.operator
	}

	pub fn apply(&mut self, change: Change) -> Result<Change> {
		let mut result = Vec::new();

		for diff in change.diffs {
			match diff {
				Diff::Insert {
					post,
					..
				} => self.apply_filter_insert(&post, &mut result)?,
				Diff::Update {
					pre,
					post,
					..
				} => self.apply_filter_update(&pre, &post, &mut result)?,
				Diff::Remove {
					pre,
					..
				} => self.apply_filter_remove(&pre, &mut result)?,
			}
		}

		Ok(Change::from_flow(self.operator, change.version, result, change.changed_at))
	}
}

impl FilterOperator {
	#[inline]
	pub fn output_schema(&self) -> Option<SchemaRef> {
		self.parent_schema.clone()
	}

	#[instrument(name = "flow::operator::filter::insert", level = "trace", skip_all, fields(rows = post.num_rows()))]
	fn apply_filter_insert(&self, post: &RecordBatch, result: &mut Vec<Diff>) -> Result<()> {
		let mask = self.evaluate(post)?;
		let passing = self.filter_passing(post, &mask)?;
		if passing.num_columns() > 0 {
			result.push(Diff::insert(passing));
		}
		Ok(())
	}

	#[inline]
	#[instrument(name = "flow::operator::filter::remove", level = "trace", skip_all, fields(rows = pre.num_rows()))]
	fn apply_filter_remove(&self, pre: &RecordBatch, result: &mut Vec<Diff>) -> Result<()> {
		let mask = self.evaluate(pre)?;
		let passing = self.filter_passing(pre, &mask)?;
		if passing.num_columns() > 0 {
			result.push(Diff::remove(passing));
		}
		Ok(())
	}

	#[inline]
	#[instrument(name = "flow::operator::filter::update", level = "trace", skip_all, fields(rows = post.num_rows()))]
	fn apply_filter_update(&self, pre: &RecordBatch, post: &RecordBatch, result: &mut Vec<Diff>) -> Result<()> {
		let pre_mask = self.evaluate(pre)?;
		let post_mask = self.evaluate(post)?;

		let mut updated_idx = Vec::new();
		let mut inserted_idx = Vec::new();
		let mut removed_idx = Vec::new();

		let row_count = pre_mask.len().min(post_mask.len());
		for i in 0..row_count {
			match (pre_mask[i], post_mask[i]) {
				(true, true) => updated_idx.push(i),
				(false, true) => inserted_idx.push(i),
				(true, false) => removed_idx.push(i),
				(false, false) => {}
			}
		}

		if !updated_idx.is_empty() {
			result.push(Diff::update(extract(pre, &updated_idx)?, extract(post, &updated_idx)?));
		}
		if !inserted_idx.is_empty() {
			result.push(Diff::insert(extract(post, &inserted_idx)?));
		}
		if !removed_idx.is_empty() {
			result.push(Diff::remove(extract(pre, &removed_idx)?));
		}
		Ok(())
	}
}

fn extract(columns: &RecordBatch, indices: &[usize]) -> Result<RecordBatch> {
	forward_system_columns(&take_rows(columns, indices)?)
}
