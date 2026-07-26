// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	error::diagnostic::query::column_not_found, interface::identifier::ColumnObject, value::column::ColumnWithName,
};
use reifydb_rql::expression::AccessObjectExpression;
use reifydb_value::{error, fragment::Fragment};

use crate::{Result, expression::context::EvalContext};

pub(crate) fn access_lookup(ctx: &EvalContext, expr: &AccessObjectExpression) -> Result<ColumnWithName> {
	let source = match &expr.column.object {
		ColumnObject::Qualified {
			name,
			..
		} => name,
		ColumnObject::Alias(alias) => alias,
	};
	let column = expr.column.name.text().to_string();

	let qualified_name = format!("{}.{}", source.text(), &column);

	let matching_col = ctx.columns.iter().find(|col| {
		if col.name().text() == qualified_name {
			return true;
		}

		if matches!(&expr.column.object, ColumnObject::Qualified { .. }) && col.name().text() == column {
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
