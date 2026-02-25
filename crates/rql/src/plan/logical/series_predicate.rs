// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Series predicate extraction for timestamp/tag pushdown.
//!
//! This module detects patterns in filter expressions that can be pushed down
//! into the series key range scan instead of post-filtering:
//! - `timestamp >= X` / `timestamp > X` → time_start bound
//! - `timestamp <= X` / `timestamp < X` → time_end bound
//! - `timestamp BETWEEN A AND B` → both bounds
//! - `tag == X` → variant_tag filter

use crate::expression::{ColumnExpression, ConstantExpression, Expression};

/// Extracted series predicates that can be pushed into the scan.
#[derive(Debug, Clone, Default)]
pub struct SeriesPredicate {
	pub time_start: Option<i64>,
	pub time_end: Option<i64>,
	pub variant_tag: Option<u8>,
	/// Predicates that couldn't be pushed down and must remain as post-filters.
	pub remaining: Vec<Expression>,
}

impl SeriesPredicate {
	pub fn has_pushdown(&self) -> bool {
		self.time_start.is_some() || self.time_end.is_some() || self.variant_tag.is_some()
	}
}

/// Attempts to extract pushable series predicates from a filter condition.
///
/// Returns `Some(SeriesPredicate)` if at least one predicate can be pushed down.
/// Returns `None` if nothing can be pushed.
pub fn extract_series_predicate(condition: &Expression) -> Option<SeriesPredicate> {
	let mut conjuncts = Vec::new();
	flatten_and(condition, &mut conjuncts);

	let mut result = SeriesPredicate::default();

	for expr in conjuncts {
		if !try_extract_one(expr, &mut result) {
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
fn try_extract_one(expr: &Expression, result: &mut SeriesPredicate) -> bool {
	match expr {
		// timestamp >= CONST
		Expression::GreaterThanEqual(gte) => {
			if let Some(val) = try_timestamp_const(&gte.left, &gte.right) {
				result.time_start = Some(merge_max(result.time_start, val));
				return true;
			}
			// CONST <= timestamp  →  timestamp >= CONST
			if let Some(val) = try_timestamp_const(&gte.right, &gte.left) {
				result.time_end = Some(merge_min(result.time_end, val));
				return true;
			}
			false
		}
		// timestamp > CONST → time_start = CONST + 1
		Expression::GreaterThan(gt) => {
			if let Some(val) = try_timestamp_const(&gt.left, &gt.right) {
				result.time_start = Some(merge_max(result.time_start, val.saturating_add(1)));
				return true;
			}
			// CONST < timestamp  →  timestamp > CONST
			if let Some(val) = try_timestamp_const(&gt.right, &gt.left) {
				result.time_end = Some(merge_min(result.time_end, val.saturating_sub(1)));
				return true;
			}
			false
		}
		// timestamp <= CONST
		Expression::LessThanEqual(lte) => {
			if let Some(val) = try_timestamp_const(&lte.left, &lte.right) {
				result.time_end = Some(merge_min(result.time_end, val));
				return true;
			}
			// CONST >= timestamp  →  timestamp <= CONST
			if let Some(val) = try_timestamp_const(&lte.right, &lte.left) {
				result.time_start = Some(merge_max(result.time_start, val));
				return true;
			}
			false
		}
		// timestamp < CONST → time_end = CONST - 1
		Expression::LessThan(lt) => {
			if let Some(val) = try_timestamp_const(&lt.left, &lt.right) {
				result.time_end = Some(merge_min(result.time_end, val.saturating_sub(1)));
				return true;
			}
			// CONST > timestamp  →  timestamp < CONST
			if let Some(val) = try_timestamp_const(&lt.right, &lt.left) {
				result.time_start = Some(merge_max(result.time_start, val.saturating_add(1)));
				return true;
			}
			false
		}
		// timestamp BETWEEN A AND B
		Expression::Between(between) => {
			if is_timestamp_column(&between.value) {
				if let (Some(lower), Some(upper)) =
					(extract_constant_i64(&between.lower), extract_constant_i64(&between.upper))
				{
					result.time_start = Some(merge_max(result.time_start, lower));
					result.time_end = Some(merge_min(result.time_end, upper));
					return true;
				}
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

/// Check if `maybe_col` is the "timestamp" column and `maybe_val` is a constant i64.
fn try_timestamp_const(maybe_col: &Expression, maybe_val: &Expression) -> Option<i64> {
	if !is_timestamp_column(maybe_col) {
		return None;
	}
	extract_constant_i64(maybe_val)
}

/// Check if `maybe_col` is the "tag" column and `maybe_val` is a constant u8.
fn try_tag_eq(maybe_col: &Expression, maybe_val: &Expression) -> Option<u8> {
	if !is_tag_column(maybe_col) {
		return None;
	}
	extract_constant_u8(maybe_val)
}

fn is_timestamp_column(expr: &Expression) -> bool {
	match expr {
		Expression::Column(ColumnExpression(col_id)) => col_id.name.text() == "timestamp",
		Expression::AccessSource(access) => access.column.name.text() == "timestamp",
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

fn extract_constant_i64(expr: &Expression) -> Option<i64> {
	match expr {
		Expression::Constant(ConstantExpression::Number {
			fragment,
		}) => fragment.text().parse::<i64>().ok(),
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

fn merge_max(current: Option<i64>, new: i64) -> i64 {
	match current {
		Some(existing) => existing.max(new),
		None => new,
	}
}

fn merge_min(current: Option<i64>, new: i64) -> i64 {
	match current {
		Some(existing) => existing.min(new),
		None => new,
	}
}
