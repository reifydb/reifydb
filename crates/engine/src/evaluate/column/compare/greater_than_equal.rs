// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::{super::StandardColumnEvaluator, compare_columns, GreaterThanEqual};
use crate::evaluate::ColumnEvaluationContext;
use reifydb_rql::expression::GreaterThanEqExpression;
use reifydb_type::error::diagnostic::operator::greater_than_equal_cannot_be_applied_to_incompatible_types;

impl StandardColumnEvaluator {
	pub(crate) fn greater_than_equal(
		&self,
		ctx: &ColumnEvaluationContext,
		gte: &GreaterThanEqExpression,
	) -> crate::Result<reifydb_core::value::column::Column> {
		let left = self.evaluate(ctx, &gte.left)?;
		let right = self.evaluate(ctx, &gte.right)?;
		compare_columns::<GreaterThanEqual>(
			ctx,
			&left,
			&right,
			gte.full_fragment_owned(),
			greater_than_equal_cannot_be_applied_to_incompatible_types,
		)
	}
}
