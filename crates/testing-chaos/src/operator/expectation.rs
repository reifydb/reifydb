// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::Value;

use crate::operator::{compare::contains_all, view::MaterializedView};

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
}
