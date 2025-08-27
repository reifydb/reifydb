// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	OwnedFragment,
	interface::{
		EvaluationContext, Evaluator,
		evaluate::expression::{
			AccessSourceExpression, ColumnExpression, Expression,
		},
	},
};

use crate::{
	columnar::{Column, SourceQualified},
	evaluate::StandardEvaluator,
};

impl StandardEvaluator {
	pub(crate) fn access(
		&self,
		ctx: &EvaluationContext,
		expr: &AccessSourceExpression,
	) -> crate::Result<Column> {
		let source = expr.source.text().to_string();
		let column = expr.column.text().to_string();

		let data = self
			.evaluate(
				ctx,
				&Expression::Column(ColumnExpression(
					OwnedFragment::Statement {
						column: expr.source.column(),
						line: expr.source.line(),
						text: format!(
							"{}.{}",
							source, column
						),
					},
				)),
			)?
			.data()
			.clone();

		Ok(Column::SourceQualified(SourceQualified {
			source,
			name: column,
			data,
		}))
	}
}
