// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Series predicate extraction for key/tag pushdown.
//!
//! This module detects patterns in filter expressions that can be pushed down
//! into the series key range scan instead of post-filtering:
//! - `<key_column> >= X` / `<key_column> > X` → key_start bound
//! - `<key_column> <= X` / `<key_column> < X` → key_end bound
//! - `<key_column> BETWEEN A AND B` → both bounds
//! - `tag == X` → variant_tag filter

use crate::expression::{ColumnExpression, ConstantExpression, Expression};

/// Extracted series predicates that can be pushed into the scan.
#[derive(Debug, Clone, Default)]
pub struct SeriesPredicate {
	pub key_start: Option<u64>,
	pub key_end: Option<u64>,
	pub variant_tag: Option<u8>,
	/// Predicates that couldn't be pushed down and must remain as post-filters.
	pub remaining: Vec<Expression>,
}

impl SeriesPredicate {
	pub fn has_pushdown(&self) -> bool {
		self.key_start.is_some() || self.key_end.is_some() || self.variant_tag.is_some()
	}
}

/// Attempts to extract pushable series predicates from a filter condition.
///
/// `key_column_name` is the name of the series key column to match against.
/// Returns `Some(SeriesPredicate)` if at least one predicate can be pushed down.
/// Returns `None` if nothing can be pushed.
pub fn extract_series_predicate(condition: &Expression, key_column_name: &str) -> Option<SeriesPredicate> {
	let mut conjuncts = Vec::new();
	flatten_and(condition, &mut conjuncts);

	let mut result = SeriesPredicate::default();

	for expr in conjuncts {
		if !try_extract_one(expr, &mut result, key_column_name) {
			result.remaining.push(expr.clone());
		}
	}

	if result.has_pushdown() {
		Some(result)
	} else {
		None
	}
}

/// Flatten AND expressions into a list of conjuncts.
fn flatten_and<'a>(expr: &'a Expression, out: &mut Vec<&'a Expression>) {
	match expr {
		Expression::And(and) => {
			flatten_and(&and.left, out);
			flatten_and(&and.right, out);
		}
		other => out.push(other),
	}
}

/// Try to extract a single series predicate from an expression.
/// Returns true if the expression was consumed (pushed down).
fn try_extract_one(expr: &Expression, result: &mut SeriesPredicate, key_column_name: &str) -> bool {
	match expr {
		// key_column >= CONST
		Expression::GreaterThanEqual(gte) => {
			if let Some(val) = try_key_const(&gte.left, &gte.right, key_column_name) {
				result.key_start = Some(merge_max(result.key_start, val));
				return true;
			}
			// CONST <= key_column  →  key_column >= CONST
			if let Some(val) = try_key_const(&gte.right, &gte.left, key_column_name) {
				result.key_end = Some(merge_min(result.key_end, val));
				return true;
			}
			false
		}
		// key_column > CONST → key_start = CONST + 1
		Expression::GreaterThan(gt) => {
			if let Some(val) = try_key_const(&gt.left, &gt.right, key_column_name) {
				result.key_start = Some(merge_max(result.key_start, val.saturating_add(1)));
				return true;
			}
			// CONST < key_column  →  key_column > CONST
			if let Some(val) = try_key_const(&gt.right, &gt.left, key_column_name) {
				result.key_end = Some(merge_min(result.key_end, val.saturating_sub(1)));
				return true;
			}
			false
		}
		// key_column <= CONST
		Expression::LessThanEqual(lte) => {
			if let Some(val) = try_key_const(&lte.left, &lte.right, key_column_name) {
				result.key_end = Some(merge_min(result.key_end, val));
				return true;
			}
			// CONST >= key_column  →  key_column <= CONST
			if let Some(val) = try_key_const(&lte.right, &lte.left, key_column_name) {
				result.key_start = Some(merge_max(result.key_start, val));
				return true;
			}
			false
		}
		// key_column < CONST → key_end = CONST - 1
		Expression::LessThan(lt) => {
			if let Some(val) = try_key_const(&lt.left, &lt.right, key_column_name) {
				result.key_end = Some(merge_min(result.key_end, val.saturating_sub(1)));
				return true;
			}
			// CONST > key_column  →  key_column < CONST
			if let Some(val) = try_key_const(&lt.right, &lt.left, key_column_name) {
				result.key_start = Some(merge_max(result.key_start, val.saturating_add(1)));
				return true;
			}
			false
		}
		// key_column BETWEEN A AND B
		Expression::Between(between) => {
			if is_key_column(&between.value, key_column_name)
				&& let (Some(lower), Some(upper)) =
					(extract_constant_u64(&between.lower), extract_constant_u64(&between.upper))
			{
				result.key_start = Some(merge_max(result.key_start, lower));
				result.key_end = Some(merge_min(result.key_end, upper));
				return true;
			}
			false
		}
		// tag == CONST
		Expression::Equal(eq) => {
			if let Some(val) = try_tag_eq(&eq.left, &eq.right) {
				result.variant_tag = Some(val);
				return true;
			}
			if let Some(val) = try_tag_eq(&eq.right, &eq.left) {
				result.variant_tag = Some(val);
				return true;
			}
			false
		}
		_ => false,
	}
}

/// Check if `maybe_col` is the key column and `maybe_val` is a constant u64.
fn try_key_const(maybe_col: &Expression, maybe_val: &Expression, key_column_name: &str) -> Option<u64> {
	if !is_key_column(maybe_col, key_column_name) {
		return None;
	}
	extract_constant_u64(maybe_val)
}

/// Check if `maybe_col` is the "tag" column and `maybe_val` is a constant u8.
fn try_tag_eq(maybe_col: &Expression, maybe_val: &Expression) -> Option<u8> {
	if !is_tag_column(maybe_col) {
		return None;
	}
	extract_constant_u8(maybe_val)
}

fn is_key_column(expr: &Expression, key_column_name: &str) -> bool {
	match expr {
		Expression::Column(ColumnExpression(col_id)) => col_id.name.text() == key_column_name,
		Expression::AccessSource(access) => access.column.name.text() == key_column_name,
		_ => false,
	}
}

fn is_tag_column(expr: &Expression) -> bool {
	match expr {
		Expression::Column(ColumnExpression(col_id)) => col_id.name.text() == "tag",
		Expression::AccessSource(access) => access.column.name.text() == "tag",
		_ => false,
	}
}

fn extract_constant_u64(expr: &Expression) -> Option<u64> {
	match expr {
		Expression::Constant(ConstantExpression::Number {
			fragment,
		}) => fragment.text().parse::<u64>().ok(),
		_ => None,
	}
}

fn extract_constant_u8(expr: &Expression) -> Option<u8> {
	match expr {
		Expression::Constant(ConstantExpression::Number {
			fragment,
		}) => fragment.text().parse::<u8>().ok(),
		_ => None,
	}
}

fn merge_max(current: Option<u64>, new: u64) -> u64 {
	match current {
		Some(existing) => existing.max(new),
		None => new,
	}
}

fn merge_min(current: Option<u64>, new: u64) -> u64 {
	match current {
		Some(existing) => existing.min(new),
		None => new,
	}
}
