// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff},
		flow::OperatorCapability,
	},
	internal_err,
	value::column::columns::Columns,
};
use reifydb_evaluate::expression::{
	compile::{CompiledExpr, compile_expression},
	context::{CompileContext, EvalContext},
};
use reifydb_routine_abi::registry::Routines;
use reifydb_rql::expression::Expression;
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	Result,
	value::{Value, value_type::ValueType},
};
use tracing::instrument;

use crate::{context::FlowContext, operator::Operator, transaction::FlowTransaction};

pub struct FilterOperator {
	parent_schema: Option<Columns>,
	operator: OperatorId,
	compiled_conditions: Vec<CompiledExpr>,
	routines: Routines,
	runtime_context: RuntimeContext,
	ctx: Arc<FlowContext>,
}

impl FilterOperator {
	pub fn new(
		parent_schema: Option<Columns>,
		operator: OperatorId,
		conditions: Vec<Expression>,
		routines: Routines,
		runtime_context: RuntimeContext,
		ctx: Arc<FlowContext>,
	) -> Self {
		let compile_ctx = CompileContext {
			symbols: &ctx.symbols,
		};
		let compiled_conditions: Vec<CompiledExpr> = conditions
			.iter()
			.map(|e| compile_expression(&compile_ctx, e).expect("Failed to compile filter condition"))
			.collect();

		Self {
			parent_schema,
			operator,
			compiled_conditions,
			routines,
			runtime_context,
			ctx,
		}
	}

	#[instrument(name = "flow::operator::filter::evaluate", level = "trace", skip_all, fields(rows = columns.row_count()))]
	fn evaluate(&self, columns: &Columns) -> Result<Vec<bool>> {
		let row_count = columns.row_count();
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
			columns: Columns::empty(),
			row_count: 1,
			target: None,
			take: None,
		};
		let exec_ctx = session.with_eval(columns.clone(), row_count);

		let mut mask = vec![true; row_count];

		for compiled_condition in &self.compiled_conditions {
			let result_col = compiled_condition.execute(&exec_ctx)?;

			for (row_idx, mask_val) in mask.iter_mut().enumerate() {
				if *mask_val {
					match result_col.data().get_value(row_idx) {
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

	#[instrument(name = "flow::operator::filter::passing", level = "trace", skip_all, fields(rows = columns.row_count()))]
	fn filter_passing(&self, columns: &Columns, mask: &[bool]) -> Columns {
		let passing_indices: Vec<usize> =
			mask.iter().enumerate().filter(|&(_, pass)| *pass).map(|(idx, _)| idx).collect();

		if passing_indices.is_empty() {
			Columns::empty()
		} else {
			columns.extract_by_indices(&passing_indices)
		}
	}
}

impl<T: FlowTransaction> Operator<T> for FilterOperator {
	fn id(&self) -> OperatorId {
		self.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn apply(&self, _txn: &mut T, change: Change) -> Result<Change> {
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

	fn output_schema(&self) -> Option<Columns> {
		self.output_schema()
	}
}

impl FilterOperator {
	#[inline]
	pub(crate) fn output_schema(&self) -> Option<Columns> {
		self.parent_schema.clone()
	}

	#[instrument(name = "flow::operator::filter::insert", level = "trace", skip_all, fields(rows = post.row_count()))]
	fn apply_filter_insert(&self, post: &Columns, result: &mut Vec<Diff>) -> Result<()> {
		let mask = self.evaluate(post)?;
		let passing = self.filter_passing(post, &mask);
		if !passing.is_empty() {
			result.push(Diff::insert(passing));
		}
		Ok(())
	}

	#[inline]
	#[instrument(name = "flow::operator::filter::remove", level = "trace", skip_all, fields(rows = pre.row_count()))]
	fn apply_filter_remove(&self, pre: &Columns, result: &mut Vec<Diff>) -> Result<()> {
		let mask = self.evaluate(pre)?;
		let passing = self.filter_passing(pre, &mask);
		if !passing.is_empty() {
			result.push(Diff::remove(passing));
		}
		Ok(())
	}

	#[inline]
	#[instrument(name = "flow::operator::filter::update", level = "trace", skip_all, fields(rows = post.row_count()))]
	fn apply_filter_update(&self, pre: &Columns, post: &Columns, result: &mut Vec<Diff>) -> Result<()> {
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
			result.push(Diff::update(
				pre.extract_by_indices(&updated_idx),
				post.extract_by_indices(&updated_idx),
			));
		}
		if !inserted_idx.is_empty() {
			result.push(Diff::insert(post.extract_by_indices(&inserted_idx)));
		}
		if !removed_idx.is_empty() {
			result.push(Diff::remove(pre.extract_by_indices(&removed_idx)));
		}
		Ok(())
	}
}
