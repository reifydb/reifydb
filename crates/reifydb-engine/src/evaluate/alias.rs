// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_rql::expression::AliasExpression;

use crate::{
	columnar::{Column, ColumnQualified, SourceQualified},
	evaluate::{EvaluationContext, Evaluator},
};

impl Evaluator {
	pub(crate) fn alias(
		&mut self,
		expr: &AliasExpression,
		ctx: &EvaluationContext,
	) -> crate::Result<Column> {
		let evaluated = self.evaluate(&expr.expression, ctx)?;
		let alias_name = &expr.alias.0.fragment;

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
