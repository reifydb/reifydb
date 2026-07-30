// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Debug;

use rand::rngs::StdRng;
use reifydb_core::interface::change::Change;
use reifydb_value::value::row_number::RowNumber;

/// The four values a driver folds into its corpus fingerprint.
///
/// The fingerprint is what makes a pinned regression fail loudly when a generator change re-points
/// its seed at a different sequence, so these are a compatibility surface: which lanes a branch mixes,
/// and in what order, is fixed by [`crate::operator::drive`]. A workload only projects its row onto
/// them.
pub struct Lanes {
	pub number: u64,
	pub group: u64,
	pub coord: u64,
	pub value: u64,
}

/// How a corpus is generated and turned into changes, for one family of operator.
///
/// This is the half of a chaos run that carries domain meaning: what a row is, which columns identify
/// it, and how it is shaped into a `Change`. The driver owns everything that does not - the step mix,
/// the live-row registry, the view bounds, the fingerprint and the drain loop.
///
/// The RNG contract is exact and load-bearing. [`Workload::sample`] must draw its fields in a fixed
/// order and draw the same number of values every call, and [`Workload::revalue`] must draw exactly
/// one. The driver interleaves its own draws with these on a single stream, so an extra or reordered
/// draw shifts every subsequent operation and silently re-points every pinned regression in the
/// family at a sequence that no longer contains the defect it names.
pub trait Workload {
	type Row: Clone + Debug;

	/// Draws one fresh row. Called once per row of an insert batch, in order.
	fn sample(&self, rng: &mut StdRng, number: RowNumber) -> Self::Row;

	/// Draws a replacement for the row's value, leaving its identity and coordinate alone.
	///
	/// Moving the coordinate would be a different test: a sliding window pins an updated row to the
	/// windows it was first indexed into rather than recomputing them, so a coordinate-moving update
	/// is not simply retract-then-admit and needs a model of its own.
	fn revalue(&self, rng: &mut StdRng, row: &Self::Row) -> Self::Row;

	fn lanes(&self, row: &Self::Row) -> Lanes;

	fn insert(&self, rows: &[Self::Row]) -> Change;

	fn remove(&self, row: &Self::Row) -> Change;

	fn update(&self, pre: &Self::Row, post: &Self::Row) -> Change;

	/// Which columns of the materialized view the model's rows are compared against.
	fn projection(&self) -> &[usize];

	/// Per-projected-column float tolerance, positional and aligned with [`Workload::projection`].
	///
	/// Empty means exact, which is what an integer aggregate wants. A family whose output is computed
	/// in floating point needs latitude here: the operator and the model reach the same total by
	/// different summation orders, so bit equality is the wrong assertion even when both are correct.
	fn tolerances(&self) -> &[Option<f64>] {
		&[]
	}
}
