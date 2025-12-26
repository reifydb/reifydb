// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_rql::expression::LessThanEqExpression;
use reifydb_type::diagnostic::operator::less_than_equal_cannot_be_applied_to_incompatible_types;

use super::{LessThanEqual, compare_columns};
use crate::evaluate::column::{ColumnEvaluationContext, StandardColumnEvaluator};

impl StandardColumnEvaluator {
	pub(crate) fn less_than_equal(
		&self,
		ctx: &ColumnEvaluationContext,
		lte: &LessThanEqExpression,
	) -> crate::Result<reifydb_core::value::column::Column> {
		let left = self.evaluate(ctx, &lte.left)?;
		let right = self.evaluate(ctx, &lte.right)?;
		compare_columns::<LessThanEqual>(
			ctx,
			&left,
			&right,
			lte.full_fragment_owned(),
			less_than_equal_cannot_be_applied_to_incompatible_types,
		)
	}
}
