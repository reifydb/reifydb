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
	value::{
		batch::empty_batch,
		column::factory::{column_reference, rename_keeping_tag, tag_label},
	},
};
use reifydb_evaluate::{expression::context::EvalContext, lower::LoweredExpr};
use reifydb_routine_abi::registry::Routines;
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	Result,
	value::value_type::{
		ValueType,
		field::{FieldType, is_sumtype_tag, to_field},
	},
};
use tracing::instrument;

use crate::{
	context::FlowContext,
	operator::{
		dictionary::{BoundExpressions, BoundLiterals},
		forward_system_columns, schema_context, with_system_columns_of,
	},
	txn::Find,
};

pub struct MapOperator {
	parent_schema: Option<SchemaRef>,
	decoded_schema: Option<SchemaRef>,
	operator: OperatorId,
	expressions: BoundExpressions,
	labels: Vec<String>,
	references: Vec<Option<String>>,
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
		let labels = expressions.iter().map(|expr| display_label(expr).text().to_string()).collect();
		let references = expressions.iter().map(column_reference).collect();

		Ok(Self {
			parent_schema,
			decoded_schema: None,
			operator,
			expressions: BoundExpressions::new(expressions, true, "map"),
			labels,
			references,
			routines,
			runtime_context,
			ctx,
		})
	}

	pub fn with_decoded_schema(mut self, decoded_schema: Option<SchemaRef>) -> Self {
		self.decoded_schema = decoded_schema;
		self
	}

	pub fn output_schema(&self) -> Option<SchemaRef> {
		let parent = self.parent_schema.as_ref();
		let typing = self.decoded_schema.as_ref().or(parent);
		let ctx = schema_context(typing, &self.routines, &self.runtime_context, &self.ctx);
		Some(Arc::new(Schema::new(
			self.expressions
				.expressions()
				.iter()
				.map(|expr| schema_column(parent, expr, &LoweredExpr::new(expr.clone(), "map"), &ctx))
				.collect::<Vec<FieldRef>>(),
		)))
	}

	#[instrument(name = "flow::operator::map::project", level = "trace", skip_all, fields(rows = columns.num_rows()))]
	fn project<T: Find + ?Sized>(
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
			batch: empty_batch(),
			row_count: 1,
			target: None,
			take: None,
		};
		let (bound, lowered_expressions) = self.expressions.bind(txn, literals, columns)?;
		let exec_ctx = session.with_eval(bound, row_count);

		let mut result_columns = Vec::with_capacity(self.labels.len());

		for ((lowered_expr, label), reference) in
			lowered_expressions.iter().zip(&self.labels).zip(&self.references)
		{
			let evaluated_col = lowered_expr.evaluate(&exec_ctx)?;
			result_columns.push(rename_keeping_tag(
				evaluated_col,
				label,
				reference
					.as_deref()
					.is_some_and(|name| columns.schema_ref().field_with_name(name).is_err()),
			));
		}

		forward_system_columns(&with_system_columns_of(result_columns, columns)?)
	}
}

pub fn schema_column(
	parent: Option<&SchemaRef>,
	expression: &Expression,
	lowered: &LoweredExpr,
	ctx: &EvalContext,
) -> FieldRef {
	let source = match expression {
		Expression::Alias(alias) => alias.expression.as_ref(),
		other => other,
	};
	let label = display_label(expression);
	let (parent_field, resolved) = match (parent, source) {
		(Some(parent), Expression::Column(column)) => {
			let name = column.0.name.text();
			match parent.field_with_name(name) {
				Ok(field) => (Some(field), false),
				Err(_) => (
					parent.field_with_name(&format!("{name}_tag"))
						.ok()
						.filter(|field| is_sumtype_tag(field)),
					true,
				),
			}
		}
		_ => (None, false),
	};
	match parent_field.cloned().or_else(|| lowered.output_field(ctx).map(|field| field.as_ref().clone())) {
		Some(field) => {
			let name = match resolved {
				true => tag_label(&field, label.text()),
				false => label.text().to_string(),
			};
			Arc::new(field.with_name(name))
		}
		None => Arc::new(to_field(label.text(), &FieldType::from(ValueType::Any))),
	}
}

impl MapOperator {
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
					let projected = self.project(txn, &mut literals, &post)?;

					if projected.num_columns() > 0 {
						result.push(Diff::insert(projected));
					}
				}
				Diff::Update {
					pre,
					post,
					..
				} => {
					let projected_post = self.project(txn, &mut literals, &post)?;
					let projected_pre = self.project(txn, &mut literals, &pre)?;

					if projected_post.num_columns() > 0 {
						result.push(Diff::update(projected_pre, projected_post));
					}
				}
				Diff::Remove {
					pre,
					..
				} => {
					let projected_pre = self.project(txn, &mut literals, &pre)?;
					if projected_pre.num_columns() > 0 {
						result.push(Diff::remove(projected_pre));
					}
				}
			}
		}

		Ok(Change::from_flow(self.operator, change.version, result, change.changed_at))
	}
}
