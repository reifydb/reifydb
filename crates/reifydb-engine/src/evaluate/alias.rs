// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{Evaluator, evaluate::expression::AliasExpression},
	value::columnar::{Column, ColumnQualified, SourceQualified},
};

use crate::evaluate::{EvaluationContext, StandardEvaluator};

impl StandardEvaluator {
	pub(crate) fn alias(
		&self,
		ctx: &EvaluationContext,
		expr: &AliasExpression,
	) -> crate::Result<Column> {
		let evaluated = self.evaluate(ctx, &expr.expression)?;
		let alias_name = expr.alias.0.fragment().to_string();

		let columns: Option<String> = ctx
			.target_column
			.as_ref()
			.and_then(|c| c.table.map(|c| c.to_string()))
			.or(ctx.columns.first().as_ref().and_then(|c| {
				c.table().map(|f| f.to_string())
			}));

		Ok(match columns {
			Some(source) => {
				Column::SourceQualified(SourceQualified {
					source,
					name: alias_name.clone(),
					data: evaluated.data().clone(),
				})
			}
			None => Column::ColumnQualified(ColumnQualified {
				name: alias_name.clone(),
				data: evaluated.data().clone(),
			}),
		})
	}
}
