// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{FieldRef, Schema, SchemaRef};
use reifydb_core::{
	expression::{Expression, name::display_label},
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff},
	},
	value::{batch::empty_batch, column::factory::rename},
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
	value::value_type::{
		ValueType,
		field::{FieldType, to_field},
	},
};
use tracing::instrument;

use crate::{
	context::FlowContext,
	operator::{forward_system_columns, with_system_columns_of},
};

pub struct MapOperator {
	parent_schema: Option<SchemaRef>,
	operator: OperatorId,
	expressions: Vec<Expression>,
	labels: Vec<String>,
	lowered_expressions: Vec<LoweredExpr>,
	routines: Routines,
	runtime_context: RuntimeContext,
	ctx: Arc<FlowContext>,
}

impl MapOperator {
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
		let lowered_expressions: Vec<LoweredExpr> =
			expressions.iter().map(|expression| LoweredExpr::new(expression.clone(), "map")).collect();
		let labels = expressions.iter().map(|expr| display_label(expr).text().to_string()).collect();

		Ok(Self {
			parent_schema,
			operator,
			expressions,
			labels,
			lowered_expressions,
			routines,
			runtime_context,
			ctx,
		})
	}

	pub fn output_schema(&self) -> Option<SchemaRef> {
		Some(Arc::new(Schema::new(
			self.expressions
				.iter()
				.map(|expr| schema_column(self.parent_schema.as_ref(), expr))
				.collect::<Vec<FieldRef>>(),
		)))
	}

	#[instrument(name = "flow::operator::map::project", level = "trace", skip_all, fields(rows = columns.num_rows()))]
	fn project(&self, columns: &RecordBatch) -> Result<RecordBatch> {
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
		let exec_ctx = session.with_eval(columns.clone(), row_count);

		let mut result_columns = Vec::with_capacity(self.expressions.len());

		for (lowered_expr, label) in self.lowered_expressions.iter().zip(&self.labels) {
			let evaluated_col = lowered_expr.evaluate(&exec_ctx)?;
			result_columns.push(rename(evaluated_col, label));
		}

		forward_system_columns(&with_system_columns_of(result_columns, columns)?)
	}
}

pub fn schema_column(parent: Option<&SchemaRef>, expression: &Expression) -> FieldRef {
	let source = match expression {
		Expression::Alias(alias) => alias.expression.as_ref(),
		other => other,
	};
	let label = display_label(expression);
	let parent_field = match (parent, source) {
		(Some(parent), Expression::Column(column)) => parent.field_with_name(column.0.name.text()).ok(),
		_ => None,
	};
	match parent_field {
		Some(field) => Arc::new(field.clone().with_name(label.text())),
		None => Arc::new(to_field(label.text(), &FieldType::from(ValueType::Any))),
	}
}

impl MapOperator {
	pub fn id(&self) -> OperatorId {
		self.operator
	}

	pub fn apply(&mut self, change: Change) -> Result<Change> {
		let mut result = Vec::new();

		for diff in change.diffs.into_iter() {
			match diff {
				Diff::Insert {
					post,
					..
				} => {
					let projected = self.project(&post)?;

					if projected.num_columns() > 0 {
						result.push(Diff::insert(projected));
					}
				}
				Diff::Update {
					pre,
					post,
					..
				} => {
					let projected_post = self.project(&post)?;
					let projected_pre = self.project(&pre)?;

					if projected_post.num_columns() > 0 {
						result.push(Diff::update(projected_pre, projected_post));
					}
				}
				Diff::Remove {
					pre,
					..
				} => {
					let projected_pre = self.project(&pre)?;
					if projected_pre.num_columns() > 0 {
						result.push(Diff::remove(projected_pre));
					}
				}
			}
		}

		Ok(Change::from_flow(self.operator, change.version, result, change.changed_at))
	}
}
