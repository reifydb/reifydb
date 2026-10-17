// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{FieldRef, Schema, SchemaRef};
use reifydb_core::{
	expression::{Expression, name::display_label},
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff},
	},
	value::{batch::empty_batch, column::factory::rename},
};
use reifydb_evaluate::expression::{
	compile::compile_expression,
	context::{CompileContext, EvalContext},
};
use reifydb_routine_abi::registry::Routines;
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{Result, value::system_columns::user_columns};
use tracing::instrument;

use crate::{
	context::FlowContext,
	operator::{
		dictionary::{BoundExpressions, BoundLiterals},
		forward_system_columns,
		map::schema_column,
		with_system_columns_of,
	},
	txn::Find,
};

pub struct ExtendOperator {
	parent_schema: Option<SchemaRef>,
	operator: OperatorId,
	expressions: BoundExpressions,
	labels: Vec<String>,
	routines: Routines,
	runtime_context: RuntimeContext,
	ctx: Arc<FlowContext>,
}

impl ExtendOperator {
	pub fn new(
		parent_schema: Option<SchemaRef>,
		operator: OperatorId,
		expressions: Vec<Expression>,
		routines: Routines,
		runtime_context: RuntimeContext,
		ctx: Arc<FlowContext>,
	) -> Result<Self> {
		let compile_ctx = CompileContext {
			symbols: &ctx.symbols,
		};
		for expression in &expressions {
			compile_expression(&compile_ctx, expression)?;
		}
		let labels = expressions.iter().map(|expr| display_label(expr).text().to_string()).collect();

		Ok(Self {
			parent_schema,
			operator,
			expressions: BoundExpressions::new(expressions, true, "extend"),
			labels,
			routines,
			runtime_context,
			ctx,
		})
	}

	pub fn output_schema(&self) -> Option<SchemaRef> {
		let parent = self.parent_schema.as_ref()?;
		let mut fields: Vec<FieldRef> = parent.fields().iter().cloned().collect();
		fields.extend(self.expressions.expressions().iter().map(|expr| schema_column(Some(parent), expr)));
		Some(Arc::new(Schema::new(fields)))
	}

	#[instrument(name = "flow::operator::extend::extend", level = "trace", skip_all, fields(rows = columns.num_rows()))]
	fn extend<T: Find + ?Sized>(
		&mut self,
		txn: &mut T,
		literals: &mut Option<BoundLiterals>,
		columns: &RecordBatch,
	) -> Result<RecordBatch> {
		let row_count = columns.num_rows();
		if row_count == 0 {
			return Ok(empty_batch());
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
		let (bound, lowered_expressions) = self.expressions.bind(txn, literals, columns)?;
		let exec_ctx = session.with_eval(bound, row_count);

		let mut result_columns: Vec<(FieldRef, ArrayRef)> =
			user_columns(columns).map(|(field, array)| (field.clone(), array.clone())).collect();

		for (lowered_expr, label) in lowered_expressions.iter().zip(&self.labels) {
			let evaluated_col = lowered_expr.evaluate(&exec_ctx)?;
			result_columns.push(rename(evaluated_col, label));
		}

		forward_system_columns(&with_system_columns_of(result_columns, columns)?)
	}
}

impl ExtendOperator {
	pub fn id(&self) -> OperatorId {
		self.operator
	}

	pub fn apply<T: Find + ?Sized>(&mut self, txn: &mut T, change: Change) -> Result<Change> {
		let mut result = Vec::new();
		let mut literals = None;

		for diff in change.diffs.into_iter() {
			match diff {
				Diff::Insert {
					post,
					..
				} => {
					let extended = self.extend(txn, &mut literals, &post)?;

					if extended.num_columns() > 0 {
						result.push(Diff::insert(extended));
					}
				}
				Diff::Update {
					pre,
					post,
					..
				} => {
					let extended_post = self.extend(txn, &mut literals, &post)?;
					let extended_pre = self.extend(txn, &mut literals, &pre)?;

					if extended_post.num_columns() > 0 {
						result.push(Diff::update(extended_pre, extended_post));
					}
				}
				Diff::Remove {
					pre,
					..
				} => {
					let extended_pre = self.extend(txn, &mut literals, &pre)?;
					if extended_pre.num_columns() > 0 {
						result.push(Diff::remove(extended_pre));
					}
				}
			}
		}

		Ok(Change::from_flow(self.operator, change.version, result, change.changed_at))
	}
}
