// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::value::column::Column;
use reifydb_rql::expression::AliasExpression;
use reifydb_type::Fragment;

use crate::evaluate::{
	TargetColumn,
	column::{ColumnEvaluationContext, StandardColumnEvaluator},
};

impl StandardColumnEvaluator {
	pub(crate) fn alias<'a>(&self, ctx: &ColumnEvaluationContext, expr: &AliasExpression) -> crate::Result<Column> {
		let evaluated = self.evaluate(ctx, &expr.expression)?;
		let alias_name = expr.alias.0.clone();

		let source = ctx.target.as_ref().and_then(|c| match c {
			TargetColumn::Resolved(col) => Some(Fragment::internal(col.primitive().identifier().text())),
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
