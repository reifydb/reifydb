// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::{super::StandardColumnEvaluator, compare_columns, GreaterThan};
use crate::evaluate::ColumnEvaluationContext;
use reifydb_rql::expression::GreaterThanExpression;
use reifydb_type::error::diagnostic::operator::greater_than_cannot_be_applied_to_incompatible_types;

impl StandardColumnEvaluator {
	pub(crate) fn greater_than(
		&self,
		ctx: &ColumnEvaluationContext,
		gt: &GreaterThanExpression,
	) -> crate::Result<reifydb_core::value::column::Column> {
		let left = self.evaluate(ctx, &gt.left)?;
		let right = self.evaluate(ctx, &gt.right)?;
		compare_columns::<GreaterThan>(
			ctx,
			&left,
			&right,
			gt.full_fragment_owned(),
			greater_than_cannot_be_applied_to_incompatible_types,
		)
	}
}
