// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::value::column::Column;
use reifydb_rql::expression::AccessSourceExpression;
use reifydb_type::{Fragment, OwnedFragment, diagnostic::query::column_not_found, error};

use crate::evaluate::{ColumnEvaluationContext, column::StandardColumnEvaluator};

impl StandardColumnEvaluator {
	pub(crate) fn access<'a>(
		&self,
		ctx: &ColumnEvaluationContext<'a>,
		expr: &AccessSourceExpression<'a>,
	) -> crate::Result<Column<'a>> {
		use reifydb_core::interface::identifier::ColumnSource;

		// Extract source name based on the ColumnSource type
		let source = match &expr.column.source {
			ColumnSource::Source {
				source,
				..
			} => source,
			ColumnSource::Alias(alias) => alias,
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
			if matches!(&expr.column.source, ColumnSource::Source { .. }) {
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
			Err(error!(column_not_found(Fragment::Owned(OwnedFragment::Statement {
				column: expr.column.name.column(),
				line: expr.column.name.line(),
				text: format!("{}.{}", source.text(), &column),
			}))))
		}
	}
}
