// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, data::ColumnData};
use reifydb_rql::expression::{BetweenExpression, GreaterThanEqExpression, LessThanEqExpression};
use reifydb_type::{error::diagnostic::operator::between_cannot_be_applied_to_incompatible_types, return_error};

use super::super::StandardColumnEvaluator;
use crate::evaluate::ColumnEvaluationContext;

impl StandardColumnEvaluator {
	pub(crate) fn between(&self, ctx: &ColumnEvaluationContext, expr: &BetweenExpression) -> crate::Result<Column> {
		// Create temporary expressions for the comparisons
		let greater_equal_expr = GreaterThanEqExpression {
			left: expr.value.clone(),
			right: expr.lower.clone(),
			fragment: expr.fragment.clone(),
		};

		let less_equal_expr = LessThanEqExpression {
			left: expr.value.clone(),
			right: expr.upper.clone(),
			fragment: expr.fragment.clone(),
		};

		// Evaluate both comparisons
		let ge_result = self.greater_than_equal(ctx, &greater_equal_expr)?;
		let le_result = self.less_than_equal(ctx, &less_equal_expr)?;

		// Check that both results are boolean (they should be if the
		// comparison succeeded)
		if !matches!(ge_result.data(), ColumnData::Bool(_)) || !matches!(le_result.data(), ColumnData::Bool(_))
		{
			// This should not happen if the comparison operator
			// work correctly, but we handle it as a safety
			// measure
			let value = self.evaluate(ctx, &expr.value)?;
			let lower = self.evaluate(ctx, &expr.lower)?;
			return_error!(between_cannot_be_applied_to_incompatible_types(
				expr.full_fragment_owned(),
				value.get_type(),
				lower.get_type(),
			))
		}

		// Combine the results with AND logic
		let ge_data = ge_result.data();
		let le_data = le_result.data();

		match (ge_data, le_data) {
			(ColumnData::Bool(ge_container), ColumnData::Bool(le_container)) => {
				let mut data = Vec::with_capacity(ge_container.len());
				let mut bitvec = Vec::with_capacity(ge_container.len());

				for i in 0..ge_container.len() {
					if ge_container.is_defined(i) && le_container.is_defined(i) {
						data.push(ge_container.data().get(i) && le_container.data().get(i));
						bitvec.push(true);
					} else {
						data.push(false);
						bitvec.push(false);
					}
				}

				Ok(Column {
					name: expr.fragment.clone(),
					data: ColumnData::bool_with_bitvec(data, bitvec),
				})
			}
			_ => {
				// This should never be reached due to the check
				// above
				unreachable!("Both comparison results should be boolean after the check above")
			}
		}
	}
}
