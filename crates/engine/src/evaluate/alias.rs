// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{Evaluator, TargetColumn, evaluate::expression::AliasExpression},
	value::column::Column,
};
use reifydb_type::Fragment;

use crate::evaluate::{EvaluationContext, StandardEvaluator};

impl StandardEvaluator {
	pub(crate) fn alias<'a>(
		&self,
		ctx: &EvaluationContext<'a>,
		expr: &AliasExpression<'a>,
	) -> crate::Result<Column<'a>> {
		let evaluated = self.evaluate(ctx, &expr.expression)?;
		let alias_name = expr.alias.0.clone();

		let source = ctx.target.as_ref().and_then(|c| match c {
			TargetColumn::Resolved(col) => Some(Fragment::owned_internal(col.source().identifier().text())),
			TargetColumn::Partial {
				..
			} => None,
		});

		// Source qualification is no longer needed in the Column struct
		// The alias_name can include the source if needed
		let _source = source; // Unused now
		Ok(Column {
			name: alias_name,
			data: evaluated.data().clone(),
		})
	}
}
