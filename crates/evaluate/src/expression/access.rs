// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::FieldRef;
use reifydb_core::{
	error::diagnostic::query::column_not_found, expression::AccessObjectExpression,
	interface::identifier::ColumnObject,
};
use reifydb_value::{error, fragment::Fragment, value::system_columns::is_system_field};

use crate::{Result, expression::context::EvalContext};

pub(crate) fn access_lookup(ctx: &EvalContext, expr: &AccessObjectExpression) -> Result<(FieldRef, ArrayRef)> {
	let index = access_position(&ctx.batch, expr)?;
	Ok((ctx.batch.schema_ref().fields()[index].clone(), ctx.batch.column(index).clone()))
}

pub(crate) fn access_position(batch: &RecordBatch, expr: &AccessObjectExpression) -> Result<usize> {
	let source = match &expr.column.object {
		ColumnObject::Qualified {
			name,
			..
		} => name,
		ColumnObject::Alias(alias) => alias,
	};
	let column = expr.column.name.text().to_string();

	let qualified_name = format!("{}.{}", source.text(), &column);

	let matching_col =
		batch.schema_ref().fields().iter().enumerate().filter(|(_, field)| !is_system_field(field)).find(
			|(_, field)| {
				if field.name() == &qualified_name {
					return true;
				}

				if matches!(&expr.column.object, ColumnObject::Qualified { .. })
					&& field.name() == &column
				{
					return !field.name().contains('.');
				}

				false
			},
		);

	if let Some((index, _)) = matching_col {
		Ok(index)
	} else {
		Err(error!(column_not_found(Fragment::Statement {
			column: expr.column.name.column(),
			line: expr.column.name.line(),
			text: Arc::from(format!("{}.{}", source.text(), &column)),
		})))
	}
}
