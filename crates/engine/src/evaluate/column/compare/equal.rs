// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::Column;
use reifydb_rql::expression::EqExpression;
use reifydb_type::error::diagnostic::operator::equal_cannot_be_applied_to_incompatible_types;

use super::{super::StandardColumnEvaluator, Equal, compare_columns};
use crate::evaluate::ColumnEvaluationContext;

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
