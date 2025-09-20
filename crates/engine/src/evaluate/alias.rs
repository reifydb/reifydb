// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{Evaluator, evaluate::expression::AliasExpression},
	value::columnar::{Column, ColumnQualified, SourceQualified},
};

use crate::evaluate::{EvaluationContext, StandardEvaluator};

impl StandardEvaluator {
	pub(crate) fn alias<'a>(
		&self,
		ctx: &EvaluationContext<'a>,
		expr: &AliasExpression<'a>,
	) -> crate::Result<Column<'a>> {
		let evaluated = self.evaluate(ctx, &expr.expression)?;
		let alias_name = expr.alias.0.clone();

		let source = ctx.target_column.as_ref().and_then(|c| c.table.as_ref()).or(ctx
			.columns
			.first()
			.as_ref()
			.and_then(|c| c.table()));

		Ok(match source {
			Some(src) => Column::SourceQualified(SourceQualified {
				source: src.clone(),
				name: alias_name.clone(),
				data: evaluated.data().clone(),
			}),
			None => Column::ColumnQualified(ColumnQualified {
				name: alias_name.clone(),
				data: evaluated.data().clone(),
			}),
		})
	}
}
