// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fmt::{self, Debug, Formatter},
	hash::Hash,
	marker::PhantomData,
};

use reifydb_codec::row::operator::state::{OperatorState, StateCodec};
use reifydb_core::metrics::heap::HeapSize;
use reifydb_macro::operator_state;

use super::base::ImmutableBase;
use crate::window::{
	accumulator::WindowAccumulator,
	span::{Slot, SlotSpan},
};

pub trait ImmutableFoldStep {
	type Value: Clone + Debug;
	type State: Clone + Debug + Default;
	type Params: Clone + Debug + Default;
	type Output: Clone + Debug + PartialEq;

	fn fold(params: &Self::Params, state: &mut Self::State, prev: Option<&Self::Value>, cur: &Self::Value);

	fn output(params: &Self::Params, state: &Self::State) -> Option<Self::Output>;
}

#[operator_state]
pub struct ImmutableFold<S: Slot, F: ImmutableFoldStep> {
	base: ImmutableBase<S, F::Value>,
	params: F::Params,
	immutable: F::State,
	last_immutable: Option<F::Value>,
	marker: PhantomData<fn() -> F>,
}

impl<S: Slot, F: ImmutableFoldStep> Clone for ImmutableFold<S, F> {
	fn clone(&self) -> Self {
		Self {
			base: self.base.clone(),
			params: self.params.clone(),
			immutable: self.immutable.clone(),
			last_immutable: self.last_immutable.clone(),
			marker: PhantomData,
		}
	}
}

impl<S: Slot, F: ImmutableFoldStep> Debug for ImmutableFold<S, F> {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		f.debug_struct("ImmutableFold")
			.field("base", &self.base)
			.field("params", &self.params)
			.field("immutable", &self.immutable)
			.field("last_immutable", &self.last_immutable)
			.finish()
	}
}

impl<S: Slot, F: ImmutableFoldStep> Default for ImmutableFold<S, F> {
	fn default() -> Self {
		Self {
			base: ImmutableBase::default(),
			params: F::Params::default(),
			immutable: F::State::default(),
			last_immutable: None,
			marker: PhantomData,
		}
	}
}

impl<S: Slot, F: ImmutableFoldStep> ImmutableFold<S, F> {
	pub fn new(immutable: SlotSpan<S>, params: F::Params) -> Self {
		Self::maybe_immutable(Some(immutable), params)
	}

	pub fn maybe_immutable(immutable: Option<SlotSpan<S>>, params: F::Params) -> Self {
		Self {
			base: ImmutableBase::maybe_immutable(immutable),
			params,
			immutable: F::State::default(),
			last_immutable: None,
			marker: PhantomData,
		}
	}

	pub fn params(&self) -> &F::Params {
		&self.params
	}

	pub fn len(&self) -> u64 {
		self.base.len()
	}

	pub fn is_empty(&self) -> bool {
		self.base.len() == 0
	}

	pub fn immutable_count(&self) -> u64 {
		self.base.immutable_count()
	}
}

impl<S, F> WindowAccumulator for ImmutableFold<S, F>
where
	S: Slot + Hash,
	F: ImmutableFoldStep,
	ImmutableFold<S, F>: OperatorState + StateCodec + HeapSize,
{
	type Contributions = Vec<(S, F::Value)>;
	type Output = F::Output;

	fn add(&mut self, contributions: &[(S, F::Value)]) {
		for contribution in contributions {
			for (_, v) in self.base.push(contribution.0, contribution.1.clone()) {
				F::fold(&self.params, &mut self.immutable, self.last_immutable.as_ref(), &v);
				self.last_immutable = Some(v);
			}
		}
	}

	fn remove(&mut self, contributions: &[(S, F::Value)]) {
		for contribution in contributions {
			self.base.remove(&contribution.0);
		}
	}

	fn finalize(&self) -> Option<F::Output> {
		let mut state = self.immutable.clone();
		let mut prev = self.last_immutable.clone();
		for v in self.base.tail().values() {
			F::fold(&self.params, &mut state, prev.as_ref(), v);
			prev = Some(v.clone());
		}
		F::output(&self.params, &state)
	}
}

impl<S: Slot + HeapSize, F: ImmutableFoldStep> HeapSize for ImmutableFold<S, F>
where
	F::Value: HeapSize,
	F::State: HeapSize,
	F::Params: HeapSize,
{
	fn heap_size(&self) -> usize {
		self.base.heap_size()
			+ self.params.heap_size()
			+ self.immutable.heap_size()
			+ self.last_immutable.heap_size()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::row::operator::state::decode;
	use reifydb_value::factory::time::millis;

	use super::*;
	use crate::{
		factory::coord::event_coord_at_millis,
		window::{
			accumulator::testkit::{Op, assert_add_remove_is_inverse, assert_arms_agree, drive},
			coord::DateTimeCoord,
		},
	};

	struct AbsPathFold;

	impl ImmutableFoldStep for AbsPathFold {
		type Value = f64;
		type State = f64;
		type Params = ();
		type Output = f64;

		fn fold(_params: &(), state: &mut f64, prev: Option<&f64>, cur: &f64) {
			if let Some(p) = prev {
				*state += (cur - p).abs();
			}
		}

		fn output(_params: &(), state: &f64) -> Option<f64> {
			Some(*state)
		}
	}

	struct ScaledPathFold;

	impl ImmutableFoldStep for ScaledPathFold {
		type Value = f64;
		type State = f64;
		type Params = f64;
		type Output = f64;

		fn fold(params: &f64, state: &mut f64, prev: Option<&f64>, cur: &f64) {
			if let Some(p) = prev {
				*state += (cur - p).abs() * params;
			}
		}

		fn output(_params: &f64, state: &f64) -> Option<f64> {
			Some(*state)
		}
	}

	struct SumFold;

	impl ImmutableFoldStep for SumFold {
		type Value = f64;
		type State = f64;
		type Params = ();
		type Output = f64;

		fn fold(_params: &(), state: &mut f64, _prev: Option<&f64>, cur: &f64) {
			*state += cur;
		}

		fn output(_params: &(), state: &f64) -> Option<f64> {
			Some(*state)
		}
	}

	#[test]
	fn immutable_fold_no_seal_sums_all_adjacent_steps() {
		let mut accumulator: ImmutableFold<DateTimeCoord, AbsPathFold> = ImmutableFold::default();
		accumulator.add(&[(event_coord_at_millis(0), 10.0)]);
		accumulator.add(&[(event_coord_at_millis(1), 20.0)]);
		accumulator.add(&[(event_coord_at_millis(2), 15.0)]);

		assert_eq!(accumulator.finalize(), Some(15.0));
	}

	#[test]
	fn immutable_fold_seals_aged_prefix_exactly_for_forward_data() {
		let mut accumulator: ImmutableFold<DateTimeCoord, AbsPathFold> = ImmutableFold::new(millis(1), ());
		accumulator.add(&[(event_coord_at_millis(0), 10.0)]);
		accumulator.add(&[(event_coord_at_millis(1), 20.0)]);
		accumulator.add(&[(event_coord_at_millis(2), 15.0)]);
		assert_eq!(accumulator.finalize(), Some(15.0), "immutable prefix preserves the full path exactly");
	}

	#[test]
	fn immutable_fold_aged_removal_is_dropped_no_op_but_live_removal_is_safe() {
		let mut accumulator: ImmutableFold<DateTimeCoord, AbsPathFold> = ImmutableFold::new(millis(1), ());
		accumulator.add(&[(event_coord_at_millis(0), 10.0)]);
		accumulator.add(&[(event_coord_at_millis(1), 20.0)]);
		accumulator.add(&[(event_coord_at_millis(2), 15.0)]);

		accumulator.remove(&[(event_coord_at_millis(0), 10.0)]);
		assert_eq!(accumulator.finalize(), Some(15.0), "aged removal does not disturb the immutable path");

		accumulator.remove(&[(event_coord_at_millis(2), 15.0)]);
		assert_eq!(accumulator.finalize(), Some(10.0), "live removal recomputes the path");
	}

	#[test]
	fn immutable_fold_is_independent_of_arrival_order() {
		// A row landing behind the high water must still fold, otherwise the answer depends on arrival order.
		let mut forward: ImmutableFold<DateTimeCoord, SumFold> = ImmutableFold::new(millis(1), ());
		forward.add(&[(event_coord_at_millis(0), 10.0)]);
		forward.add(&[(event_coord_at_millis(2), 15.0)]);

		let mut reversed: ImmutableFold<DateTimeCoord, SumFold> = ImmutableFold::new(millis(1), ());
		reversed.add(&[(event_coord_at_millis(2), 15.0)]);
		reversed.add(&[(event_coord_at_millis(0), 10.0)]);

		assert_eq!(forward.immutable_count(), 1, "a plain arm makes this comparison vacuous");
		assert_eq!(reversed.immutable_count(), forward.immutable_count());
		assert_eq!(reversed.len(), forward.len());
		assert_eq!(reversed.finalize(), forward.finalize());
		assert_eq!(forward.finalize(), Some(25.0), "the late row contributes exactly once");
	}

	#[test]
	fn immutable_fold_unsealed_is_independent_of_arrival_order() {
		// Nothing folds, so no distance behind the high water may keep a row out of the fold.
		let mut forward: ImmutableFold<DateTimeCoord, SumFold> = ImmutableFold::maybe_immutable(None, ());
		forward.add(&[(event_coord_at_millis(0), 10.0)]);
		forward.add(&[(event_coord_at_millis(1_000_000), 15.0)]);

		let mut reversed: ImmutableFold<DateTimeCoord, SumFold> = ImmutableFold::maybe_immutable(None, ());
		reversed.add(&[(event_coord_at_millis(1_000_000), 15.0)]);
		reversed.add(&[(event_coord_at_millis(0), 10.0)]);

		assert_eq!(forward.immutable_count(), 0, "an immutable span here would make this an immutable test");
		assert_eq!(reversed.immutable_count(), 0);
		assert_eq!(reversed.len(), forward.len());
		assert_eq!(reversed.finalize(), forward.finalize());
		assert_eq!(forward.finalize(), Some(25.0), "each row contributes exactly once");
	}

	#[test]
	fn immutable_fold_does_not_fold_a_corrected_coordinate_twice() {
		// A repeat of an already immutable slot is a correction to that row, never a second contribution.
		let mut accumulator: ImmutableFold<DateTimeCoord, SumFold> = ImmutableFold::new(millis(1), ());
		accumulator.add(&[(event_coord_at_millis(0), 10.0)]);
		accumulator.add(&[(event_coord_at_millis(2), 20.0)]);
		assert_eq!(accumulator.finalize(), Some(30.0));

		accumulator.add(&[(event_coord_at_millis(0), 10.0)]);
		assert_eq!(
			accumulator.finalize(),
			Some(30.0),
			"re-folding a corrected coordinate must not add it twice"
		);
	}

	#[test]
	fn immutable_fold_matches_the_unsealed_arm_for_in_order_adds() {
		// The immutable prefix folds once at add and the tail replays at finalize; together they must be the
		// whole path.
		assert_arms_agree(
			ImmutableFold::<DateTimeCoord, AbsPathFold>::new(millis(1), ()),
			ImmutableFold::<DateTimeCoord, AbsPathFold>::maybe_immutable(None, ()),
			&[
				Op::Add((event_coord_at_millis(0), 10.0)),
				Op::Add((event_coord_at_millis(1), 20.0)),
				Op::Add((event_coord_at_millis(2), 15.0)),
				Op::Add((event_coord_at_millis(3), 40.0)),
			],
			"an immutable span must not change the folded path when every row arrives in order",
		);
	}

	#[test]
	fn immutable_fold_drops_a_new_row_that_lands_below_the_seal_line() {
		// This is the price of the fast path: the plain arm is what the dropped row was worth.
		let ops = [
			Op::Add((event_coord_at_millis(0), 10.0)),
			Op::Add((event_coord_at_millis(2), 20.0)),
			Op::Add((event_coord_at_millis(4), 30.0)),
			Op::Add((event_coord_at_millis(1), 7.0)),
		];
		let mut immutable: ImmutableFold<DateTimeCoord, SumFold> = ImmutableFold::new(millis(1), ());
		drive(&mut immutable, &ops);
		let mut plain: ImmutableFold<DateTimeCoord, SumFold> = ImmutableFold::maybe_immutable(None, ());
		drive(&mut plain, &ops);

		assert_eq!(plain.finalize(), Some(67.0), "retaining everything folds all four rows");
		assert_eq!(
			immutable.finalize(),
			Some(60.0),
			"a row older than the immutable line never reaches the fold"
		);
		assert_eq!(immutable.len(), 3, "and it is never counted either");
	}

	#[test]
	fn immutable_fold_default_add_remove_is_inverse() {
		assert_add_remove_is_inverse::<ImmutableFold<DateTimeCoord, AbsPathFold>>(
			&[(event_coord_at_millis(0), 10.0f64), (event_coord_at_millis(1), 20.0)],
			(event_coord_at_millis(2), 30.0f64),
		);
	}

	#[test]
	fn immutable_fold_roundtrip() {
		let mut accumulator: ImmutableFold<DateTimeCoord, AbsPathFold> = ImmutableFold::new(millis(1), ());
		accumulator.add(&[(event_coord_at_millis(0), 10.0)]);
		accumulator.add(&[(event_coord_at_millis(1), 20.0)]);
		accumulator.add(&[(event_coord_at_millis(2), 15.0)]);
		let bytes = accumulator.encode_state().expect("encode");
		let restored: ImmutableFold<DateTimeCoord, AbsPathFold> = decode(&bytes).expect("decode");
		assert_eq!(restored.finalize(), accumulator.finalize());
	}

	#[test]
	fn immutable_fold_params_drive_both_the_sealed_prefix_and_the_live_tail() {
		// Params must reach the aged step folded at add and the tail steps replayed at finalize.
		let mut accumulator: ImmutableFold<DateTimeCoord, ScaledPathFold> = ImmutableFold::new(millis(1), 2.0);
		accumulator.add(&[(event_coord_at_millis(0), 10.0)]);
		accumulator.add(&[(event_coord_at_millis(1), 20.0)]);
		accumulator.add(&[(event_coord_at_millis(2), 15.0)]);

		assert_eq!(accumulator.finalize(), Some(30.0));
	}

	#[test]
	fn immutable_fold_params_survive_the_state_roundtrip() {
		// Params are per-window state; a decode that defaulted them would finalize to 0.0.
		let mut accumulator: ImmutableFold<DateTimeCoord, ScaledPathFold> = ImmutableFold::new(millis(1), 2.0);
		accumulator.add(&[(event_coord_at_millis(0), 10.0)]);
		accumulator.add(&[(event_coord_at_millis(1), 20.0)]);
		accumulator.add(&[(event_coord_at_millis(2), 15.0)]);
		let bytes = accumulator.encode_state().expect("encode");
		let restored: ImmutableFold<DateTimeCoord, ScaledPathFold> = decode(&bytes).expect("decode");

		assert_eq!(restored.params(), &2.0);
		assert_eq!(restored.finalize(), Some(30.0));
	}

	#[test]
	fn immutable_fold_len_counts_distinct_keys_across_the_seal_boundary() {
		// Folding moves a key from the tail into the immutable count, so the total must not move.
		let mut accumulator: ImmutableFold<DateTimeCoord, AbsPathFold> = ImmutableFold::new(millis(1), ());
		accumulator.add(&[(event_coord_at_millis(0), 10.0)]);
		accumulator.add(&[(event_coord_at_millis(1), 20.0)]);
		assert_eq!(accumulator.len(), 2);

		accumulator.add(&[(event_coord_at_millis(2), 15.0)]);
		assert_eq!(accumulator.len(), 3, "the aged key is still one observation once folded");
	}

	#[test]
	fn immutable_fold_len_collapses_a_repeated_key_and_drops_on_live_removal() {
		// A repeated slot is one corrected observation, never two.
		let mut accumulator: ImmutableFold<DateTimeCoord, AbsPathFold> = ImmutableFold::new(millis(10), ());
		accumulator.add(&[(event_coord_at_millis(0), 10.0)]);
		accumulator.add(&[(event_coord_at_millis(0), 25.0)]);
		assert_eq!(accumulator.len(), 1);

		accumulator.add(&[(event_coord_at_millis(1), 30.0)]);
		accumulator.remove(&[(event_coord_at_millis(1), 30.0)]);
		assert_eq!(accumulator.len(), 1, "removing a live key drops it from the count");
	}

	#[test]
	fn immutable_fold_unsealed_carries_params_and_never_ages() {
		// Without an immutable bound nothing folds, so a live removal must still be exact.
		let mut accumulator: ImmutableFold<DateTimeCoord, ScaledPathFold> =
			ImmutableFold::maybe_immutable(None, 3.0);
		accumulator.add(&[(event_coord_at_millis(0), 10.0)]);
		accumulator.add(&[(event_coord_at_millis(100), 20.0)]);
		assert_eq!(accumulator.finalize(), Some(30.0));

		accumulator.remove(&[(event_coord_at_millis(0), 10.0)]);
		assert_eq!(accumulator.finalize(), Some(0.0));
	}
}
