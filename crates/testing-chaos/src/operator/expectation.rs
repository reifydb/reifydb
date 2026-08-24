// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeSet;

use reifydb_value::value::Value;

use crate::operator::{
	compare::{Tolerances, compare, contains_all},
	view::{MaterializedView, OutputKey, RowKey},
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
pub struct KeyedMultiset {
	pub key: RowKey,
	pub rows: Vec<Vec<Value>>,
}

impl KeyedMultiset {
	pub fn new(key: RowKey, rows: Vec<Vec<Value>>) -> Self {
		Self {
			key,
			rows,
		}
	}
}

impl Expectation for KeyedMultiset {
	fn check(
		&self,
		actual: &MaterializedView,
		projection: &[usize],
		tolerances: &[Option<f64>],
		bound: Bound,
	) -> Result<(), String> {
		let published = actual.rekey(&self.key);
		if !published.incoherent.is_empty() {
			return Err(format!(
				"the published view holds rows its own key {:?} cannot tell apart, which is what a row \
				 minted over one already published looks like: {:?}",
				self.key, published.incoherent
			));
		}

		self.rows.check(actual, projection, tolerances, bound)
	}
}

#[derive(Debug, Clone)]
pub struct ViewClaim {
	pub view: MaterializedView,
	pub key_columns: Vec<String>,
	pub tolerances: Tolerances,

	pub unconstrained: BTreeSet<OutputKey>,
}

impl ViewClaim {
	pub fn new(view: MaterializedView, key_columns: Vec<String>, tolerances: Tolerances) -> Self {
		Self {
			view,
			key_columns,
			tolerances,
			unconstrained: BTreeSet::new(),
		}
	}

	pub fn with_unconstrained(mut self, keys: BTreeSet<OutputKey>) -> Self {
		self.unconstrained = keys;
		self
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
		if !self.view.incoherent.is_empty() {
			return Err(format!(
				"the claim's own view holds rows its key columns {:?} cannot tell apart, so no operator \
				 could satisfy it: {:?}",
				self.key_columns, self.view.incoherent
			));
		}

		let published = actual.rekey(&RowKey::columns(self.key_columns.clone()));

		if !published.incoherent.is_empty() {
			return Err(format!(
				"the published view holds rows the claim's key columns {:?} cannot tell apart: {:?}",
				self.key_columns, published.incoherent
			));
		}
		let mut result = compare(&published, &self.view, &self.tolerances);
		result.only_in_oracle.retain(|key| !self.unconstrained.contains(key));
		result.only_in_operator.retain(|key| !self.unconstrained.contains(key));
		result.divergent.retain(|row| !self.unconstrained.contains(&row.key));
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
	use reifydb_value::value::datetime::DateTime;

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
		// A model must be able to say nothing mid-run. Saying it with an empty multiset would read
		// as "the view must hold nothing", failing every operator that emits on a tick.
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

	fn windowed(rows: &[(u64, i32, i32, u64)]) -> MaterializedView {
		// (row number, g, total, window start). The row number is what the view folds on; the window
		// start rides the event position, exactly as a window operator emits it.
		let mut v = MaterializedView::empty();
		v.columns = vec!["g".to_string(), "total".to_string()];
		for (number, g, total, at) in rows {
			v.insert(
				OutputKey::new(vec![Value::Uint8(*number)]),
				MaterializedRow::from_pairs(vec![
					("g".to_string(), Value::Int4(*g)),
					("total".to_string(), Value::Int4(*total)),
				])
				.at(DateTime::from_epoch_millis(*at).ok()),
			);
		}
		v
	}

	#[test]
	fn a_second_row_for_one_window_is_caught_even_when_the_multiset_permits_its_value() {
		// A key whose row-number mapping was reclaimed mints a fresh number, so its duplicate
		// folds in without colliding; both tuples are values the model permits for other windows
		// of the group, so a positional multiset alone would let the duplicate through.
		let duplicate = windowed(&[(1, 1, 5, 1_000), (2, 1, 9, 1_000)]);
		let permitted: Vec<Vec<Value>> =
			vec![vec![Value::Int4(1), Value::Int4(5)], vec![Value::Int4(1), Value::Int4(9)]];

		assert!(
			permitted.check(&duplicate, &[0, 1], &[], Bound::AtMost).is_ok(),
			"precondition: the bare multiset must be satisfied, or this proves nothing about the key"
		);

		let keyed = KeyedMultiset::new(RowKey::columns(["g"]).with_time(), permitted.clone());
		let report = keyed.check(&duplicate, &[0, 1], &[], Bound::AtMost).expect_err("the key must catch it");
		assert!(report.contains("cannot tell apart"), "the failure must name the collision: {report}");

		// The control that gives it meaning: the same two rows belonging to two different windows are
		// an ordinary view, not a duplicate. Without this, a key that rejected everything would pass.
		let distinct = windowed(&[(1, 1, 5, 1_000), (2, 1, 9, 2_000)]);
		assert!(
			keyed.check(&distinct, &[0, 1], &[], Bound::AtMost).is_ok(),
			"two windows of one group are not a duplicate"
		);
	}

	#[test]
	fn a_row_carrying_no_event_time_is_reported_rather_than_folded_onto_one_key() {
		// rekey substitutes a placeholder for a named column a row lacks, but doing the same for a
		// missing event time would drop every row onto one key and report a duplicate on every
		// step of every suite.
		let mut timeless = windowed(&[(1, 1, 5, 1_000)]);
		timeless.insert(
			OutputKey::new(vec![Value::Uint8(2)]),
			MaterializedRow::from_pairs(vec![
				("g".to_string(), Value::Int4(2)),
				("total".to_string(), Value::Int4(7)),
			]),
		);

		let keyed = KeyedMultiset::new(
			RowKey::columns(["g"]).with_time(),
			vec![vec![Value::Int4(1), Value::Int4(5)], vec![Value::Int4(2), Value::Int4(7)]],
		);
		let report = keyed
			.check(&timeless, &[0, 1], &[], Bound::AtMost)
			.expect_err("a timeless row must be reported");
		assert!(report.contains("no event time"), "the failure must say what is missing: {report}");
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
	fn a_view_claim_whose_own_view_is_incoherent_blames_the_model_not_the_operator() {
		// An unsatisfiable claim reported against the operator sends triage after a defect that is not there.
		let mut oracle = MaterializedView::empty();
		oracle.columns = vec!["g".to_string(), "total".to_string()];
		oracle.insert(
			OutputKey::new(vec![Value::Int4(1)]),
			MaterializedRow::from_pairs(vec![
				("g".to_string(), Value::Int4(1)),
				("total".to_string(), Value::Int4(5)),
			]),
		);
		oracle.incoherent.push("two published rows share the key".to_string());
		let claim = ViewClaim::new(oracle, vec!["g".to_string()], Tolerances::new());

		let clean = view(&[(1, &[("g", Value::Int4(1)), ("total", Value::Int4(5))])], &["g", "total"]);
		let report =
			claim.check(&clean, &[], &[], Bound::Exactly).expect_err("an unsatisfiable claim must fail");

		assert!(report.contains("the claim's own view"), "the failure must name the model: {report}");
	}

	#[test]
	fn a_view_claim_reports_only_the_side_its_bound_names() {
		// The one-sided bounds let a model admit a lagging view without permitting a wrong one:
		// AtLeast ignores rows published early, AtMost ignores rows not emitted yet, and a value
		// that disagrees is not lag so it fails under both.
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
		// The workload's tolerances are positional; a keyed claim compares by column name, so
		// passing that slice through would apply the wrong column's tolerance.
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
