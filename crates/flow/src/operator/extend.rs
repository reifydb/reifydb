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
	compile::{CompiledExpr, compile_expression},
	context::{CompileContext, EvalContext},
};
use reifydb_routine_abi::registry::Routines;
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{Result, value::system_columns::user_columns};
use tracing::instrument;

use crate::{
	context::FlowContext,
	operator::{forward_system_columns, map::schema_column, with_system_columns_of},
};

pub struct ExtendOperator {
	parent_schema: Option<SchemaRef>,
	operator: OperatorId,
	expressions: Vec<Expression>,
	compiled_expressions: Vec<CompiledExpr>,
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
		let compiled_expressions: Vec<CompiledExpr> =
			expressions.iter().map(|e| compile_expression(&compile_ctx, e)).collect::<Result<Vec<_>>>()?;

		Ok(Self {
			parent_schema,
			operator,
			expressions,
			compiled_expressions,
			routines,
			runtime_context,
			ctx,
		})
	}

	pub fn output_schema(&self) -> Option<SchemaRef> {
		let parent = self.parent_schema.as_ref()?;
		let mut fields: Vec<FieldRef> = parent.fields().iter().cloned().collect();
		fields.extend(self.expressions.iter().map(|expr| schema_column(Some(parent), expr)));
		Some(Arc::new(Schema::new(fields)))
	}

	#[instrument(name = "flow::operator::extend::extend", level = "trace", skip_all, fields(rows = columns.num_rows()))]
	fn extend(&self, columns: &RecordBatch) -> Result<RecordBatch> {
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

		let mut result_columns: Vec<(FieldRef, ArrayRef)> =
			user_columns(columns).map(|(field, array)| (field.clone(), array.clone())).collect();

		for (i, compiled_expr) in self.compiled_expressions.iter().enumerate() {
			let evaluated_col = compiled_expr.execute(&exec_ctx)?;

			let expr = &self.expressions[i];
			let field_name = display_label(expr).text().to_string();

			result_columns.push(rename(evaluated_col, &field_name));
		}

		forward_system_columns(&with_system_columns_of(result_columns, columns)?)
	}
}

impl ExtendOperator {
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
					let extended = self.extend(&post)?;

					if extended.num_columns() > 0 {
						result.push(Diff::insert(extended));
					}
				}
				Diff::Update {
					pre,
					post,
					..
				} => {
					let extended_post = self.extend(&post)?;
					let extended_pre = self.extend(&pre)?;

					if extended_post.num_columns() > 0 {
						result.push(Diff::update(extended_pre, extended_post));
					}
				}
				Diff::Remove {
					pre,
					..
				} => {
					let extended_pre = self.extend(&pre)?;
					if extended_pre.num_columns() > 0 {
						result.push(Diff::remove(extended_pre));
					}
				}
			}
		}

		Ok(Change::from_flow(self.operator, change.version, result, change.changed_at))
	}
}
