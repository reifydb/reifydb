// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, ColumnData};
use reifydb_rql::expression::{EqExpression, Expression, InExpression};
use reifydb_type::Fragment;

use crate::evaluate::column::{ColumnEvaluationContext, StandardColumnEvaluator};

impl StandardColumnEvaluator {
	/// Evaluate an IN expression: `value IN (list)` or `value NOT IN (list)`
	///
	/// SQL semantics for undefined:
	/// - If value is undefined: result is undefined
	/// - If value matches any list element: result is TRUE
	/// - If no match and any list element is undefined: result is undefined
	/// - If no match and all list elements are defined: result is FALSE
	///
	/// For NOT IN, the boolean result is negated (undefined stays undefined).
	pub(crate) fn in_expr(&self, ctx: &ColumnEvaluationContext, expr: &InExpression) -> crate::Result<Column> {
		// Get the list of expressions to check against
		let list_expressions = match expr.list.as_ref() {
			Expression::Tuple(tuple) => &tuple.expressions,
			// Single value case - wrap in a slice
			_ => std::slice::from_ref(expr.list.as_ref()),
		};

		// Handle empty list case
		if list_expressions.is_empty() {
			let value_col = self.evaluate(ctx, &expr.value)?;
			let len = value_col.data().len();
			// Empty list: IN always returns false, NOT IN always returns true
			let result = vec![expr.negated; len];
			return Ok(Column {
				name: expr.fragment.clone(),
				data: ColumnData::bool(result),
			});
		}

		// Evaluate value == first_list_element to get initial result
		let first_eq = EqExpression {
			left: expr.value.clone(),
			right: Box::new(list_expressions[0].clone()),
			fragment: expr.fragment.clone(),
		};
		let mut result = self.equal(ctx, &first_eq)?;

		// For each additional list element, OR with equality check
		for list_expr in list_expressions.iter().skip(1) {
			let eq = EqExpression {
				left: expr.value.clone(),
				right: Box::new(list_expr.clone()),
				fragment: expr.fragment.clone(),
			};
			let eq_result = self.equal(ctx, &eq)?;

			// Combine with OR logic, handling undefined
			result = self.or_columns(ctx, result, eq_result, expr.fragment.clone())?;
		}

		// If negated (NOT IN), invert the result
		if expr.negated {
			result = self.negate_column(result, expr.fragment.clone());
		}

		Ok(result)
	}

	/// OR two boolean columns together with proper undefined handling.
	/// SQL semantics: TRUE OR undefined = TRUE, FALSE OR undefined = undefined
	fn or_columns(
		&self,
		_ctx: &ColumnEvaluationContext,
		left: Column,
		right: Column,
		fragment: Fragment,
	) -> crate::Result<Column> {
		match (left.data(), right.data()) {
			(ColumnData::Bool(l), ColumnData::Bool(r)) => {
				let len = l.len();
				let mut data = Vec::with_capacity(len);
				let mut bitvec = Vec::with_capacity(len);

				for i in 0..len {
					let l_defined = l.is_defined(i);
					let r_defined = r.is_defined(i);
					let l_val = l.data().get(i);
					let r_val = r.data().get(i);

					if l_defined && l_val {
						// TRUE OR anything = TRUE
						data.push(true);
						bitvec.push(true);
					} else if r_defined && r_val {
						// anything OR TRUE = TRUE
						data.push(true);
						bitvec.push(true);
					} else if l_defined && r_defined {
						// Both defined and both false
						data.push(false);
						bitvec.push(true);
					} else {
						// At least one undefined and no TRUE
						data.push(false);
						bitvec.push(false);
					}
				}

				Ok(Column {
					name: fragment,
					data: ColumnData::bool_with_bitvec(data, bitvec),
				})
			}
			_ => {
				// Non-boolean columns - this shouldn't happen with equality comparisons
				unreachable!(
					"OR columns should only be called with boolean columns from equality comparisons"
				)
			}
		}
	}

	/// Negate a boolean column. Undefined stays undefined.
	fn negate_column(&self, col: Column, fragment: Fragment) -> Column {
		match col.data() {
			ColumnData::Bool(container) => {
				let len = container.len();
				let mut data = Vec::with_capacity(len);
				let mut bitvec = Vec::with_capacity(len);

				for i in 0..len {
					if container.is_defined(i) {
						data.push(!container.data().get(i));
						bitvec.push(true);
					} else {
						data.push(false);
						bitvec.push(false);
					}
				}

				Column {
					name: fragment,
					data: ColumnData::bool_with_bitvec(data, bitvec),
				}
			}
			_ => unreachable!("negate_column should only be called with boolean columns"),
		}
	}
}
