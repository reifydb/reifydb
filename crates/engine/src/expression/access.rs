// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	error::diagnostic::query::column_not_found, interface::identifier::ColumnShape, value::column::ColumnWithName,
};
use reifydb_rql::expression::AccessShapeExpression;
use reifydb_type::{error, fragment::Fragment};

use crate::{Result, expression::context::EvalContext};

pub(crate) fn access_lookup(ctx: &EvalContext, expr: &AccessShapeExpression) -> Result<ColumnWithName> {
	let source = match &expr.column.shape {
		ColumnShape::Qualified {
			name,
			..
		} => name,
		ColumnShape::Alias(alias) => alias,
	};
	let column = expr.column.name.text().to_string();

	let qualified_name = format!("{}.{}", source.text(), &column);

	let matching_col = ctx.columns.iter().find(|col| {
		if col.name().text() == qualified_name {
			return true;
		}

		if matches!(&expr.column.shape, ColumnShape::Qualified { .. }) && col.name().text() == column {
			return !col.name().text().contains('.');
		}

		false
	});

	if let Some(col) = matching_col {
		Ok(ColumnWithName::new(col.name().clone(), col.data().clone()))
	} else {
		Err(error!(column_not_found(Fragment::Statement {
			column: expr.column.name.column(),
			line: expr.column.name.line(),
			text: Arc::from(format!("{}.{}", source.text(), &column)),
		})))
	}
}
