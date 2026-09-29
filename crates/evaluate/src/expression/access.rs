// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::{
	error::diagnostic::query::column_not_found, expression::AccessObjectExpression,
	interface::identifier::ColumnObject,
};
use reifydb_value::{error, fragment::Fragment, value::system_columns::user_columns};

use crate::{Result, expression::context::EvalContext};

pub(crate) fn access_lookup(ctx: &EvalContext, expr: &AccessObjectExpression) -> Result<(FieldRef, ArrayRef)> {
	let source = match &expr.column.object {
		ColumnObject::Qualified {
			name,
			..
		} => name,
		ColumnObject::Alias(alias) => alias,
	};
	let column = expr.column.name.text().to_string();

	let qualified_name = format!("{}.{}", source.text(), &column);

	let matching_col = user_columns(&ctx.batch).find(|(field, _)| {
		if field.name() == &qualified_name {
			return true;
		}

		if matches!(&expr.column.object, ColumnObject::Qualified { .. }) && field.name() == &column {
			return !field.name().contains('.');
		}

		false
	});

	if let Some((field, array)) = matching_col {
		Ok((field.clone(), array.clone()))
	} else {
		Err(error!(column_not_found(Fragment::Statement {
			column: expr.column.name.column(),
			line: expr.column.name.line(),
			text: Arc::from(format!("{}.{}", source.text(), &column)),
		})))
	}
}
