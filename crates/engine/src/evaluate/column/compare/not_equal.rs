// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::{super::StandardColumnEvaluator, compare_columns, NotEqual};
use crate::evaluate::ColumnEvaluationContext;
use reifydb_rql::expression::NotEqExpression;
use reifydb_type::error::diagnostic::operator::not_equal_cannot_be_applied_to_incompatible_types;

impl StandardColumnEvaluator {
	pub(crate) fn not_equal(
		&self,
		ctx: &ColumnEvaluationContext,
		ne: &NotEqExpression,
	) -> crate::Result<reifydb_core::value::column::Column> {
		let left = self.evaluate(ctx, &ne.left)?;
		let right = self.evaluate(ctx, &ne.right)?;
		compare_columns::<NotEqual>(
			ctx,
			&left,
			&right,
			ne.full_fragment_owned(),
			not_equal_cannot_be_applied_to_incompatible_types,
		)
	}
}
