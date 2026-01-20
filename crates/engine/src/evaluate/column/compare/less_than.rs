// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::{super::StandardColumnEvaluator, compare_columns, LessThan};
use crate::evaluate::ColumnEvaluationContext;
use reifydb_rql::expression::LessThanExpression;
use reifydb_type::error::diagnostic::operator::less_than_cannot_be_applied_to_incompatible_types;

impl StandardColumnEvaluator {
	pub(crate) fn less_than(
		&self,
		ctx: &ColumnEvaluationContext,
		lt: &LessThanExpression,
	) -> crate::Result<reifydb_core::value::column::Column> {
		let left = self.evaluate(ctx, &lt.left)?;
		let right = self.evaluate(ctx, &lt.right)?;
		compare_columns::<LessThan>(
			ctx,
			&left,
			&right,
			lt.full_fragment_owned(),
			less_than_cannot_be_applied_to_incompatible_types,
		)
	}
}
