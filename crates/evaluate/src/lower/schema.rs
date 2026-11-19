// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_schema::{Field, Schema};
use datafusion_common::{Column, DFSchema};
use datafusion_expr::Expr;
use reifydb_core::{
	error::diagnostic::query::column_not_found,
	expression::{AccessObjectExpression, ColumnExpression},
	value::batch::is_scalar,
};
use reifydb_value::{
	error,
	error::ValueError,
	value::system_columns::{is_system_field, resolve_column, user_columns},
};

use crate::{
	expression::{access::access_index, context::EvalContext},
	lower::{Node, error::from_datafusion, literal::row_zero},
	stack::Variable,
};

pub(super) fn ordinal_schema(operator: &str, schema: &Schema) -> Result<DFSchema, ValueError> {
	let fields: Vec<Field> = schema
		.fields()
		.iter()
		.enumerate()
		.map(|(index, field)| field.as_ref().clone().with_name(ordinal_name(index)))
		.collect();
	DFSchema::try_from(Schema::new(fields)).map_err(|err| from_datafusion(operator, err))
}

pub(super) fn column(ctx: &EvalContext, operator: &str, column: &ColumnExpression) -> Result<Node, ValueError> {
	let name = column.0.name.text();

	if let Some(index) = resolve_column(&ctx.batch, name) {
		let field = &ctx.batch.schema_ref().fields()[index];
		let field = match is_system_field(field) {
			true => Arc::new(field.as_ref().clone().with_name(name)),
			false => field.clone(),
		};
		return Ok(Node {
			expr: ordinal(index),
			field,
		});
	}

	if let Some(Variable::Columns {
		batch: scalar_batch,
	}) = ctx.symbols.get(name)
		&& is_scalar(scalar_batch)
		&& let Some((field, array)) = user_columns(scalar_batch).next()
	{
		return row_zero(operator, field, array);
	}

	Err(error!(column_not_found(column.0.name.clone())))
}

pub(super) fn access(ctx: &EvalContext, access: &AccessObjectExpression) -> Result<Node, ValueError> {
	let index = access_index(&ctx.batch, access)?;
	Ok(Node {
		expr: ordinal(index),
		field: ctx.batch.schema_ref().fields()[index].clone(),
	})
}

pub(super) fn ordinal(index: usize) -> Expr {
	Expr::Column(Column::from_name(ordinal_name(index)))
}

fn ordinal_name(index: usize) -> String {
	format!("#{index}")
}
