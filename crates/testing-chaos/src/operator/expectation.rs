// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::Value;

use crate::operator::{
	compare::{Tolerances, compare, contains_all},
	view::MaterializedView,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bound {
	AtLeast,

	AtMost,

	Exactly,
}

pub trait Expectation {
	fn check(
		&self,
		actual: &MaterializedView,
		projection: &[usize],
		tolerances: &[Option<f64>],
		bound: Bound,
	) -> Result<(), String>;
}

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
			format!("a row the model requires is missing from the view or holds the wrong value.\n  \
				 published: {published:?}\n  required: {self:?}")
		};
		let extra = || {
			format!("the operator published a row the model never produced.\n  published: \
				 {published:?}\n  permitted: {self:?}")
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

#[derive(Debug, Clone)]
pub struct ViewClaim {
	pub view: MaterializedView,
	pub key_columns: Vec<String>,
	pub tolerances: Tolerances,
}

impl ViewClaim {
	pub fn new(view: MaterializedView, key_columns: Vec<String>, tolerances: Tolerances) -> Self {
		Self {
			view,
			key_columns,
			tolerances,
		}
	}
}

impl Expectation for ViewClaim {
	fn check(
		&self,
		actual: &MaterializedView,
		_projection: &[usize],
		_tolerances: &[Option<f64>],
		bound: Bound,
	) -> Result<(), String> {
		let published = actual.rekey(&self.key_columns);
		let result = compare(&published, &self.view, &self.tolerances);
		let breached = match bound {
			Bound::AtLeast => !result.only_in_oracle.is_empty() || !result.divergent.is_empty(),
			Bound::AtMost => !result.only_in_operator.is_empty() || !result.divergent.is_empty(),
			Bound::Exactly => !result.is_match(),
		};
		if breached {
			return Err(result.format_failure(&[format!("claim bound: {bound:?}")], 5));
		}
		Ok(())
	}
}

impl<E: Expectation> Expectation for Option<E> {
	fn check(
		&self,
		actual: &MaterializedView,
		projection: &[usize],
		tolerances: &[Option<f64>],
		bound: Bound,
	) -> Result<(), String> {
		match self {
			Some(claim) => claim.check(actual, projection, tolerances, bound),
			None => Ok(()),
		}
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
			&[
				(1, &[("g", Value::Int4(1)), ("total", Value::Int4(5))]),
				(2, &[("g", Value::Int4(2)), ("total", Value::Int4(9))]),
			],
			&["g", "total"],
		);
		let subset: Vec<Vec<Value>> = vec![vec![Value::Int4(1), Value::Int4(5)]];
		let superset: Vec<Vec<Value>> = vec![
			vec![Value::Int4(1), Value::Int4(5)],
			vec![Value::Int4(2), Value::Int4(9)],
			vec![Value::Int4(3), Value::Int4(1)],
		];

		assert!(
			subset.check(&actual, &[0, 1], &[], Bound::AtLeast).is_ok(),
			"a required subset must be admitted"
		);
		assert!(
			subset.check(&actual, &[0, 1], &[], Bound::AtMost).is_err(),
			"the view holds more than this permits"
		);
		assert!(
			superset.check(&actual, &[0, 1], &[], Bound::AtMost).is_ok(),
			"a wider permission must admit the view"
		);
		assert!(
			superset.check(&actual, &[0, 1], &[], Bound::AtLeast).is_err(),
			"a row the view lacks must be caught"
		);
		assert!(
			subset.check(&actual, &[0, 1], &[], Bound::Exactly).is_err(),
			"exact must reject a mere subset"
		);
	}

	#[test]
	fn abstaining_is_not_the_same_claim_as_naming_no_rows() {
		// A model whose oracle only describes the operator once every horizon has been crossed must be
		// able to say nothing mid-run. Saying it with an empty multiset instead would read as "the view
		// must hold nothing", so every operator that emits on a tick rather than on arrival would be
		// reported as divergent for the whole run. These two must not collapse into each other.
		let actual = view(&[(1, &[("g", Value::Int4(1)), ("total", Value::Int4(5))])], &["g", "total"]);
		let abstain: Option<Vec<Vec<Value>>> = None;
		let names_nothing: Option<Vec<Vec<Value>>> = Some(vec![]);

		for bound in [Bound::AtLeast, Bound::AtMost, Bound::Exactly] {
			assert!(
				abstain.check(&actual, &[0, 1], &[], bound).is_ok(),
				"abstaining must admit any view, including under {bound:?}"
			);
		}
		assert!(
			names_nothing.check(&actual, &[0, 1], &[], Bound::AtMost).is_err(),
			"claiming an empty view must still reject a populated one"
		);
		assert!(
			Some(vec![vec![Value::Int4(1), Value::Int4(5)]])
				.check(&actual, &[0, 1], &[], Bound::Exactly)
				.is_ok(),
			"a present claim must still be checked against the view"
		);
	}

	#[test]
	fn a_view_claim_rekeys_the_published_view_before_comparing_it() {
		// A session folds what the operator emitted under row numbers, but a keyed oracle describes the
		// table its consumer sees, keyed on output columns. Comparing the two without rekeying would
		// report every row as both missing and extra, so the rekey is what makes the claim mean anything.
		let mut oracle = MaterializedView::empty();
		oracle.columns = vec!["g".to_string(), "total".to_string()];
		oracle.insert(
			OutputKey::new(vec![Value::Int4(1)]),
			MaterializedRow::from_pairs(vec![
				("g".to_string(), Value::Int4(1)),
				("total".to_string(), Value::Int4(5)),
			]),
		);
		let claim = ViewClaim::new(oracle, vec!["g".to_string()], Tolerances::new());

		let published = view(&[(77, &[("g", Value::Int4(1)), ("total", Value::Int4(5))])], &["g", "total"]);

		assert!(
			claim.check(&published, &[], &[], Bound::Exactly).is_ok(),
			"the same row under a different row number must still satisfy a claim keyed on g"
		);
	}

	#[test]
	fn a_view_claim_reports_only_the_side_its_bound_names() {
		// The two one-sided bounds exist so a model can admit a lagging view without permitting a wrong
		// one. AtLeast must ignore rows the operator published early; AtMost must ignore rows the model
		// requires but the operator has not emitted yet. A value that disagrees is not lag, so it must
		// fail under either.
		let claim_row = |key: i32, total: i32| {
			(
				OutputKey::new(vec![Value::Int4(key)]),
				MaterializedRow::from_pairs(vec![
					("g".to_string(), Value::Int4(key)),
					("total".to_string(), Value::Int4(total)),
				]),
			)
		};
		let mut oracle = MaterializedView::empty();
		oracle.columns = vec!["g".to_string(), "total".to_string()];
		let (k, r) = claim_row(1, 5);
		oracle.insert(k, r);
		let claim = ViewClaim::new(oracle, vec!["g".to_string()], Tolerances::new());

		let extra = view(
			&[
				(1, &[("g", Value::Int4(1)), ("total", Value::Int4(5))]),
				(2, &[("g", Value::Int4(2)), ("total", Value::Int4(9))]),
			],
			&["g", "total"],
		);
		assert!(claim.check(&extra, &[], &[], Bound::AtLeast).is_ok(), "an extra row is not a shortfall");
		assert!(claim.check(&extra, &[], &[], Bound::AtMost).is_err(), "an extra row exceeds the permission");

		let missing = view(&[], &["g", "total"]);
		assert!(claim.check(&missing, &[], &[], Bound::AtMost).is_ok(), "an absent row is not an excess");
		assert!(claim.check(&missing, &[], &[], Bound::AtLeast).is_err(), "an absent row is a shortfall");

		let wrong = view(&[(1, &[("g", Value::Int4(1)), ("total", Value::Int4(6))])], &["g", "total"]);
		for bound in [Bound::AtLeast, Bound::AtMost, Bound::Exactly] {
			assert!(
				claim.check(&wrong, &[], &[], bound).is_err(),
				"a diverging value is not lag and must fail under {bound:?}"
			);
		}
	}

	#[test]
	fn a_view_claim_honours_its_own_named_tolerances_not_the_workloads() {
		// The workload's positional tolerances describe a projection; a keyed claim compares whole rows by
		// column name and carries its own. Passing the workload's slice through would silently apply the
		// wrong column's tolerance, so the claim must ignore it.
		let mut oracle = MaterializedView::empty();
		oracle.columns = vec!["g".to_string(), "total".to_string()];
		oracle.insert(
			OutputKey::new(vec![Value::Int4(1)]),
			MaterializedRow::from_pairs(vec![
				("g".to_string(), Value::Int4(1)),
				("total".to_string(), Value::float8(1.0_f64)),
			]),
		);
		let claim = ViewClaim::new(oracle, vec!["g".to_string()], Tolerances::new().with("total", 0.5));

		let within = view(&[(1, &[("g", Value::Int4(1)), ("total", Value::float8(1.4_f64))])], &["g", "total"]);
		assert!(
			claim.check(&within, &[0, 1], &[None, None], Bound::Exactly).is_ok(),
			"a drift inside the claim's own tolerance must pass even though the workload allows none"
		);

		let beyond = view(&[(1, &[("g", Value::Int4(1)), ("total", Value::float8(1.6_f64))])], &["g", "total"]);
		assert!(
			claim.check(&beyond, &[0, 1], &[None, None], Bound::Exactly).is_err(),
			"a drift past the claim's tolerance must still fail"
		);
	}
}
