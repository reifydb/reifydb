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
	// Extract primitive name based on the ColumnShape type
	let source = match &expr.column.shape {
		ColumnShape::Qualified {
			name,
			..
		} => name,
		ColumnShape::Alias(alias) => alias,
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
		if matches!(&expr.column.shape, ColumnShape::Qualified { .. }) && col.name().text() == column {
			// Make sure this column doesn't belong to a different source
			// by checking if it has a dot in the name (qualified)
			return !col.name().text().contains('.');
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
