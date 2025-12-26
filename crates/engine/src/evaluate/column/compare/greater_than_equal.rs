// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_rql::expression::GreaterThanEqExpression;
use reifydb_type::diagnostic::operator::greater_than_equal_cannot_be_applied_to_incompatible_types;

use super::{GreaterThanEqual, compare_columns};
use crate::evaluate::column::{ColumnEvaluationContext, StandardColumnEvaluator};

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
