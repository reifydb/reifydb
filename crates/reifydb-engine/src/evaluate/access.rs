// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	OwnedSpan,
	interface::{
		EvaluationContext,
		evaluate::expression::{
			AccessSourceExpression, ColumnExpression, Expression,
		},
	},
};

use crate::{
	columnar::{Column, SourceQualified},
	evaluate::Evaluator,
};

impl Evaluator {
	pub(crate) fn access(
		&mut self,
		expr: &AccessSourceExpression,
		ctx: &EvaluationContext,
	) -> crate::Result<Column> {
		let source = expr.source.fragment.clone();
		let column = expr.column.fragment.clone();

		let data = self
			.evaluate(
				&Expression::Column(ColumnExpression(
					OwnedSpan {
						column: expr.source.column,
						line: expr.source.line,
						fragment: format!(
							"{}.{}",
							source, column
						),
					},
				)),
				&ctx,
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
