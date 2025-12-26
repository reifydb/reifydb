// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_rql::expression::GreaterThanExpression;
use reifydb_type::diagnostic::operator::greater_than_cannot_be_applied_to_incompatible_types;

use super::{GreaterThan, compare_columns};
use crate::evaluate::column::{ColumnEvaluationContext, StandardColumnEvaluator};

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
