// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff},
	},
	value::column::{ColumnWithName, builder::ColumnBuilder, columns::Columns},
};
use reifydb_evaluate::expression::{
	compile::{CompiledExpr, compile_expression},
	context::{CompileContext, EvalContext},
};
use reifydb_routine_abi::registry::Routines;
use reifydb_rql::expression::{Expression, name::display_label};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	Result,
	fragment::Fragment,
	value::{system_columns::SystemColumns, value_type::ValueType},
};
use tracing::instrument;

use crate::context::FlowContext;

pub struct MapOperator {
	parent_schema: Option<Columns>,
	operator: OperatorId,
	expressions: Vec<Expression>,
	compiled_expressions: Vec<CompiledExpr>,
	routines: Routines,
	runtime_context: RuntimeContext,
	ctx: Arc<FlowContext>,
}

impl MapOperator {
	pub fn new(
		parent_schema: Option<Columns>,
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

	pub fn output_schema(&self) -> Option<Columns> {
		Some(Columns::new(
			self.expressions.iter().map(|expr| schema_column(self.parent_schema.as_ref(), expr)).collect(),
		))
	}

	#[instrument(name = "flow::operator::map::project", level = "trace", skip_all, fields(rows = columns.row_count()))]
	fn project(&self, columns: &Columns) -> Result<Columns> {
		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok(Columns::empty());
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

		let mut result_columns = Vec::with_capacity(self.expressions.len());

		for (i, compiled_expr) in self.compiled_expressions.iter().enumerate() {
			let evaluated_col = compiled_expr.execute(&exec_ctx)?;

			let expr = &self.expressions[i];
			let field_name = display_label(expr).text().to_string();

			let named_column =
				ColumnWithName::new(Fragment::internal(field_name), evaluated_col.data().clone());

			result_columns.push(named_column);
		}

		let row_numbers = if columns.row_numbers().is_empty() {
			Vec::new()
		} else {
			columns.row_numbers().to_vec()
		};

		Ok(Columns::with_system(
			result_columns,
			SystemColumns::new(
				row_numbers,
				Vec::new(),
				columns.created_at().to_vec(),
				columns.updated_at().to_vec(),
				columns.time().to_vec(),
				Vec::new(),
			),
		))
	}
}

pub fn schema_column(parent: Option<&Columns>, expression: &Expression) -> ColumnWithName {
	let source = match expression {
		Expression::Alias(alias) => alias.expression.as_ref(),
		other => other,
	};
	let ty = match (parent, source) {
		(Some(parent), Expression::Column(column)) => {
			parent.column(column.0.name.text()).map(|col| col.data().get_type())
		}
		_ => None,
	};
	ColumnWithName::new(
		Fragment::internal(display_label(expression).text()),
		ColumnBuilder::with_capacity(ty.unwrap_or(ValueType::Any), 0).finish(),
	)
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

					if !projected.is_empty() {
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

					if !projected_post.is_empty() {
						result.push(Diff::update(projected_pre, projected_post));
					}
				}
				Diff::Remove {
					pre,
					..
				} => {
					let projected_pre = self.project(&pre)?;
					if !projected_pre.is_empty() {
						result.push(Diff::remove(projected_pre));
					}
				}
			}
		}

		Ok(Change::from_flow(self.operator, change.version, result, change.changed_at))
	}
}
