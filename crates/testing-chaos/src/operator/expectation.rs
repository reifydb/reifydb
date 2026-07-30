// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::Value;

use crate::operator::{
	compare::{Tolerances, contains_all, compare},
	view::MaterializedView,
};

/// Which side of the operator's view an expectation constrains.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bound {
	/// Everything here must be present. The view may hold more.
	AtLeast,
	/// Nothing outside this may be present. The view may hold less.
	AtMost,
	/// The view must match exactly.
	Exactly,
}

/// What a model says the operator should be publishing, and how to check it.
///
/// The two families make structurally different claims and neither is reducible to the other. A window
/// model bounds the view from both sides without naming rows: its projection drops the window start, so
/// several rows of one group are indistinguishable and the claim is about a multiset. A guest model
/// names every row by the operator's own output key and claims an exact table, which lets it report
/// which column of which row disagreed and by how much - and for an operator publishing twenty
/// floating-point columns, that report is the difference between a usable failure and an unreadable one.
///
/// Making the claim's shape part of the model contract is what lets one driver serve both without
/// weakening either.
pub trait Expectation {
	fn check(
		&self,
		actual: &MaterializedView,
		projection: &[usize],
		tolerances: &[Option<f64>],
		bound: Bound,
	) -> Result<(), String>;
}

/// A multiset of projected rows, bounded from one or both sides. What a window model claims.
impl Expectation for Vec<Vec<Value>> {
	fn check(
		&self,
		actual: &MaterializedView,
		projection: &[usize],
		tolerances: &[Option<f64>],
		bound: Bound,
	) -> Result<(), String> {
		let published = actual.projected(projection);
		let missing = || {
			format!(
				"a row the model requires is missing from the view or holds the wrong value.\n  \
				 published: {published:?}\n  required: {self:?}"
			)
		};
		let extra = || {
			format!(
				"the operator published a row the model never produced.\n  published: \
				 {published:?}\n  permitted: {self:?}"
			)
		};
		match bound {
			Bound::AtLeast => contains_all(&published, self, tolerances).then_some(()).ok_or_else(missing),
			Bound::AtMost => contains_all(self, &published, tolerances).then_some(()).ok_or_else(extra),
			Bound::Exactly => {
				if !contains_all(&published, self, tolerances) {
					return Err(missing());
				}
				if !contains_all(self, &published, tolerances) {
					return Err(extra());
				}
				Ok(())
			}
		}
	}
}

/// An exact table, keyed by the operator's output columns. What a guest model claims.
///
/// Every bound collapses to equality: a model that can name each row has no latitude to grant, so
/// `AtLeast` and `AtMost` would each be a weaker statement than the model is actually making.
impl Expectation for MaterializedView {
	fn check(
		&self,
		actual: &MaterializedView,
		_projection: &[usize],
		_tolerances: &[Option<f64>],
		_bound: Bound,
	) -> Result<(), String> {
		// Tolerances travel with the expectation here rather than positionally, because a keyed table
		// addresses its columns by name.
		let result = compare(actual, self, &Tolerances::new());
		if result.is_match() {
			return Ok(());
		}
		Err(result.format_failure(&["the operator's table disagrees with the model".to_string()], 5))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::operator::view::{MaterializedRow, OutputKey};

	fn view(rows: &[(u64, &[(&str, Value)])], columns: &[&str]) -> MaterializedView {
		let mut v = MaterializedView::empty();
		v.columns = columns.iter().map(|c| c.to_string()).collect();
		for (key, pairs) in rows {
			v.insert(
				OutputKey::new(vec![Value::Uint8((*key).into())]),
				MaterializedRow::from_pairs(pairs.iter().map(|(k, x)| (k.to_string(), x.clone()))),
			);
		}
		v
	}

	#[test]
	fn a_multiset_claim_bounds_the_view_from_whichever_side_it_names() {
		// A window model cannot name its rows, so it constrains the view with two one-sided bounds and
		// deliberately leaves a gap between them: a closed window may linger until a tick withdraws it.
		// Collapsing either bound into equality would make one of those legitimate states a failure.
		let actual = view(
			&[(1, &[("g", Value::Int4(1)), ("total", Value::Int4(5))]),
			  (2, &[("g", Value::Int4(2)), ("total", Value::Int4(9))])],
			&["g", "total"],
		);
		let subset: Vec<Vec<Value>> = vec![vec![Value::Int4(1), Value::Int4(5)]];
		let superset: Vec<Vec<Value>> = vec![
			vec![Value::Int4(1), Value::Int4(5)],
			vec![Value::Int4(2), Value::Int4(9)],
			vec![Value::Int4(3), Value::Int4(1)],
		];

		assert!(subset.check(&actual, &[0, 1], &[], Bound::AtLeast).is_ok(), "a required subset must be admitted");
		assert!(subset.check(&actual, &[0, 1], &[], Bound::AtMost).is_err(), "the view holds more than this permits");
		assert!(superset.check(&actual, &[0, 1], &[], Bound::AtMost).is_ok(), "a wider permission must admit the view");
		assert!(superset.check(&actual, &[0, 1], &[], Bound::AtLeast).is_err(), "a row the view lacks must be caught");
		assert!(subset.check(&actual, &[0, 1], &[], Bound::Exactly).is_err(), "exact must reject a mere subset");
	}

	#[test]
	fn a_keyed_claim_reports_which_column_disagreed() {
		// This is why the guest keeps its own claim shape rather than projecting to a multiset. For an
		// operator publishing many float columns, "these two multisets differ" is unusable; naming the
		// column and both values is what makes a failure triageable. If this degraded to a multiset diff
		// the report would still be a failure, just not an actionable one.
		let actual = view(&[(1, &[("k", Value::Int4(1)), ("rsi", Value::float8(47.9_f64))])], &["k", "rsi"]);
		let expected = view(&[(1, &[("k", Value::Int4(1)), ("rsi", Value::float8(47.3_f64))])], &["k", "rsi"]);

		let report = expected.check(&actual, &[0, 1], &[], Bound::Exactly).expect_err("values differ");
		assert!(report.contains("rsi"), "the report must name the disagreeing column, got:\n{report}");
		assert!(report.contains("47.3") && report.contains("47.9"), "both values must appear, got:\n{report}");

		assert!(
			expected.check(&expected.clone(), &[0, 1], &[], Bound::Exactly).is_ok(),
			"a table must agree with itself"
		);
	}
}
