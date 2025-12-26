// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::value::column::Column;
use reifydb_rql::expression::EqExpression;
use reifydb_type::diagnostic::operator::equal_cannot_be_applied_to_incompatible_types;

use super::{Equal, compare_columns};
use crate::evaluate::column::{ColumnEvaluationContext, StandardColumnEvaluator};

impl StandardColumnEvaluator {
	pub(crate) fn equal(&self, ctx: &ColumnEvaluationContext, eq: &EqExpression) -> crate::Result<Column> {
		let left = self.evaluate(ctx, &eq.left)?;
		let right = self.evaluate(ctx, &eq.right)?;
		compare_columns::<Equal>(
			ctx,
			&left,
			&right,
			eq.full_fragment_owned(),
			equal_cannot_be_applied_to_incompatible_types,
		)
	}
}
