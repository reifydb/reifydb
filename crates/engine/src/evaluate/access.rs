// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{
		EvaluationContext, evaluate::expression::AccessSourceExpression,
	},
	value::columnar::Column,
};
use reifydb_type::{
	Fragment, OwnedFragment, diagnostic::query::column_not_found, error,
};

use crate::evaluate::StandardEvaluator;

impl StandardEvaluator {
	pub(crate) fn access(
		&self,
		ctx: &EvaluationContext,
		expr: &AccessSourceExpression,
	) -> crate::Result<Column> {
		let source = expr.source.fragment().to_string();
		let column = expr.column.fragment().to_string();

		// Find columns where source matches and name matches
		let matching_col = ctx.columns.iter().find(|col| {
			// Check if column name matches
			if col.name() != column {
				return false;
			}

			// Check if source matches
			match col {
				Column::FullyQualified(fq) => {
					// For fully qualified, the source might
					// be "schema.table" We need to
					// match against either just table name
					// or schema.table
					let full_source = format!(
						"{}.{}",
						fq.schema, fq.source
					);
					fq.source == source
						|| full_source == source
				}
				Column::SourceQualified(sq) => {
					sq.source == source
				}
				_ => false,
			}
		});

		if let Some(col) = matching_col {
			// Extract the column data and preserve it
			Ok(col.with_new_data(col.data().clone()))
		} else {
			// If not found, return an error with proper diagnostic
			Err(error!(column_not_found(Fragment::Owned(
				OwnedFragment::Statement {
					column: expr.source.column(),
					line: expr.source.line(),
					text: format!("{}.{}", source, column),
				}
			))))
		}
	}
}
