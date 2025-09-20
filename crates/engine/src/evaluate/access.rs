// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{EvaluationContext, evaluate::expression::AccessSourceExpression},
	value::columnar::Column,
};
use reifydb_type::{Fragment, OwnedFragment, diagnostic::query::column_not_found, error};

use crate::evaluate::StandardEvaluator;

impl StandardEvaluator {
	pub(crate) fn access<'a>(
		&self,
		ctx: &EvaluationContext<'a>,
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

		// Find columns where source matches and name matches
		let matching_col = ctx.columns.iter().find(|col| {
			// Check if column name matches
			if col.name().text() != column {
				return false;
			}

			// Check if source matches
			match col {
				Column::SourceQualified(sq) => sq.source.text() == source.text(),
				_ => false,
			}
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
