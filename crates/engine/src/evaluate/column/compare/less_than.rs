// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_rql::expression::LessThanExpression;
use reifydb_type::diagnostic::operator::less_than_cannot_be_applied_to_incompatible_types;

use super::{LessThan, compare_columns};
use crate::evaluate::column::{ColumnEvaluationContext, StandardColumnEvaluator};

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
