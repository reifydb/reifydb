// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{error::diagnostic::query::column_not_found, value::column::Column};
use reifydb_rql::expression::AccessPrimitiveExpression;
use reifydb_type::{error, fragment::Fragment};

use crate::evaluate::ColumnEvaluationContext;

pub(crate) fn access_lookup(ctx: &ColumnEvaluationContext, expr: &AccessPrimitiveExpression) -> crate::Result<Column> {
	use reifydb_core::interface::identifier::ColumnPrimitive;

	// Extract primitive name based on the ColumnPrimitive type
	let source = match &expr.column.primitive {
		ColumnPrimitive::Primitive {
			primitive,
			..
		} => primitive,
		ColumnPrimitive::Alias(alias) => alias,
	};
	let column = expr.column.name.text().to_string();

	// Build the full qualified name for aliased columns
	let qualified_name = format!("{}.{}", source.text(), &column);

	// Find columns - try both qualified name and unqualified name
	let matching_col = ctx.columns.iter().find(|col| {
		// First try to match the fully qualified name (for aliased columns)
		if col.name().text() == qualified_name {
			return true;
		}

		// For non-aliased columns, just match on the column name
		// (but only if this isn't an aliased access)
		if matches!(&expr.column.primitive, ColumnPrimitive::Primitive { .. }) {
			if col.name().text() == column {
				// Make sure this column doesn't belong to a different source
				// by checking if it has a dot in the name (qualified)
				return !col.name().text().contains('.');
			}
		}

		false
	});

	if let Some(col) = matching_col {
		// Extract the column data and preserve it
		Ok(col.with_new_data(col.data().clone()))
	} else {
		// If not found, return an error with proper diagnostic
		Err(error!(column_not_found(Fragment::Statement {
			column: expr.column.name.column(),
			line: expr.column.name.line(),
			text: Arc::from(format!("{}.{}", source.text(), &column)),
		})))
	}
}
