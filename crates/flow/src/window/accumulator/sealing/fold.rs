// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fmt::{self, Debug, Formatter},
	hash::Hash,
	marker::PhantomData,
};

use reifydb_codec::row::operator::{OperatorState, StateCodec};
use reifydb_core::metrics::heap::HeapSize;
use reifydb_macro::operator_state;

use crate::window::{
	accumulator::{WindowAccumulator, sealing::base::SealingBase},
	span::{Slot, SlotSpan},
};

pub trait SealFold {
	type Value: Clone + Debug;
	type State: Clone + Debug + Default;
	type Params: Clone + Debug + Default;
	type Output: Clone + Debug + PartialEq;

	fn fold(params: &Self::Params, state: &mut Self::State, prev: Option<&Self::Value>, cur: &Self::Value);

	fn output(params: &Self::Params, state: &Self::State) -> Option<Self::Output>;
}

#[operator_state]
pub struct SealingFold<C: Slot, F: SealFold> {
	base: SealingBase<C, F::Value>,
	params: F::Params,
	sealed: F::State,
	last_sealed: Option<F::Value>,
	marker: PhantomData<fn() -> F>,
}

impl<C: Slot, F: SealFold> Clone for SealingFold<C, F> {
	fn clone(&self) -> Self {
		Self {
			base: self.base.clone(),
			params: self.params.clone(),
			sealed: self.sealed.clone(),
			last_sealed: self.last_sealed.clone(),
			marker: PhantomData,
		}
	}
}

impl<C: Slot, F: SealFold> Debug for SealingFold<C, F> {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		f.debug_struct("SealingFold")
			.field("base", &self.base)
			.field("params", &self.params)
			.field("sealed", &self.sealed)
			.field("last_sealed", &self.last_sealed)
			.finish()
	}
}

impl<C: Slot, F: SealFold> Default for SealingFold<C, F> {
	fn default() -> Self {
		Self {
			base: SealingBase::default(),
			params: F::Params::default(),
			sealed: F::State::default(),
			last_sealed: None,
			marker: PhantomData,
		}
	}
}

impl<C: Slot, F: SealFold> SealingFold<C, F> {
	pub fn new(amendable: SlotSpan<C>, params: F::Params) -> Self {
		Self {
			base: SealingBase::amendable(amendable),
			params,
			sealed: F::State::default(),
			last_sealed: None,
			marker: PhantomData,
		}
	}

	pub fn unsealed(params: F::Params) -> Self {
		Self {
			base: SealingBase::default(),
			params,
			sealed: F::State::default(),
			last_sealed: None,
			marker: PhantomData,
		}
	}

	pub fn params(&self) -> &F::Params {
		&self.params
	}

	pub fn len(&self) -> u64 {
		self.base.len()
	}

	pub fn sealed_count(&self) -> u64 {
		self.base.sealed_count()
	}
}

impl<C, F> WindowAccumulator for SealingFold<C, F>
where
	C: Slot + Hash,
	F: SealFold,
	SealingFold<C, F>: OperatorState + StateCodec + HeapSize,
{
	type Contribution = (C, F::Value);
	type Output = F::Output;

	fn add(&mut self, contribution: &(C, F::Value)) {
		for (_, v) in self.base.push(contribution.0, contribution.1.clone()) {
			F::fold(&self.params, &mut self.sealed, self.last_sealed.as_ref(), &v);
			self.last_sealed = Some(v);
		}
	}

	fn remove(&mut self, contribution: &(C, F::Value)) {
		self.base.remove(&contribution.0);
	}

	fn finalize(&self) -> Option<F::Output> {
		let mut state = self.sealed.clone();
		let mut prev = self.last_sealed.clone();
		for v in self.base.tail().values() {
			F::fold(&self.params, &mut state, prev.as_ref(), v);
			prev = Some(v.clone());
		}
		F::output(&self.params, &state)
	}

	fn is_empty(&self) -> bool {
		self.last_sealed.is_none() && self.base.is_tail_empty()
	}
}

impl<C: Slot + HeapSize, F: SealFold> HeapSize for SealingFold<C, F>
where
	F::Value: HeapSize,
	F::State: HeapSize,
	F::Params: HeapSize,
{
	fn heap_size(&self) -> usize {
		self.base.heap_size() + self.params.heap_size() + self.sealed.heap_size() + self.last_sealed.heap_size()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::row::operator::decode;
	use reifydb_value::{
		factory::time::{at_millis, millis},
		value::datetime::DateTime,
	};

	use super::*;
	use crate::window::accumulator::testkit::{Op, assert_add_remove_is_inverse, assert_arms_agree, drive};

	struct AbsPathFold;

	impl SealFold for AbsPathFold {
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

	impl SealFold for ScaledPathFold {
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

	impl SealFold for SumFold {
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
	fn sealing_fold_no_seal_sums_all_adjacent_steps() {
		let mut accumulator: SealingFold<DateTime, AbsPathFold> = SealingFold::default();
		accumulator.add(&(at_millis(0), 10.0));
		accumulator.add(&(at_millis(1), 20.0));
		accumulator.add(&(at_millis(2), 15.0));

		assert_eq!(accumulator.finalize(), Some(15.0));
	}

	#[test]
	fn sealing_fold_seals_aged_prefix_exactly_for_forward_data() {
		let mut accumulator: SealingFold<DateTime, AbsPathFold> = SealingFold::new(millis(1), ());
		accumulator.add(&(at_millis(0), 10.0));
		accumulator.add(&(at_millis(1), 20.0));
		accumulator.add(&(at_millis(2), 15.0));
		assert_eq!(accumulator.finalize(), Some(15.0), "sealed prefix preserves the full path exactly");
	}

	#[test]
	fn sealing_fold_aged_removal_is_dropped_no_op_but_live_removal_is_safe() {
		let mut accumulator: SealingFold<DateTime, AbsPathFold> = SealingFold::new(millis(1), ());
		accumulator.add(&(at_millis(0), 10.0));
		accumulator.add(&(at_millis(1), 20.0));
		accumulator.add(&(at_millis(2), 15.0));

		accumulator.remove(&(at_millis(0), 10.0));
		assert_eq!(accumulator.finalize(), Some(15.0), "aged removal does not disturb the sealed path");

		accumulator.remove(&(at_millis(2), 15.0));
		assert_eq!(accumulator.finalize(), Some(10.0), "live removal recomputes the path");
	}

	#[test]
	fn sealing_fold_is_independent_of_arrival_order() {
		// A row landing behind the high water must still fold, otherwise the answer depends on arrival order.
		let mut forward: SealingFold<DateTime, SumFold> = SealingFold::new(millis(1), ());
		forward.add(&(at_millis(0), 10.0));
		forward.add(&(at_millis(2), 15.0));

		let mut reversed: SealingFold<DateTime, SumFold> = SealingFold::new(millis(1), ());
		reversed.add(&(at_millis(2), 15.0));
		reversed.add(&(at_millis(0), 10.0));

		assert_eq!(forward.sealed_count(), 1, "an unsealed arm makes this comparison vacuous");
		assert_eq!(reversed.sealed_count(), forward.sealed_count());
		assert_eq!(reversed.len(), forward.len());
		assert_eq!(reversed.finalize(), forward.finalize());
		assert_eq!(forward.finalize(), Some(25.0), "the late row contributes exactly once");
	}

	#[test]
	fn sealing_fold_does_not_fold_a_corrected_coordinate_twice() {
		// A repeat of an already sealed coordinate is a correction to that row, never a second contribution.
		let mut accumulator: SealingFold<DateTime, SumFold> = SealingFold::new(millis(1), ());
		accumulator.add(&(at_millis(0), 10.0));
		accumulator.add(&(at_millis(2), 20.0));
		assert_eq!(accumulator.finalize(), Some(30.0));

		accumulator.add(&(at_millis(0), 10.0));
		assert_eq!(
			accumulator.finalize(),
			Some(30.0),
			"re-sealing a corrected coordinate must not add it twice"
		);
	}

	#[test]
	fn sealing_fold_matches_the_unsealed_arm_for_in_order_adds() {
		// The sealed prefix folds once at add and the tail replays at finalize; together they must be the whole
		// path.
		assert_arms_agree(
			SealingFold::<DateTime, AbsPathFold>::new(millis(1), ()),
			SealingFold::<DateTime, AbsPathFold>::unsealed(()),
			&[
				Op::Add((at_millis(0), 10.0)),
				Op::Add((at_millis(1), 20.0)),
				Op::Add((at_millis(2), 15.0)),
				Op::Add((at_millis(3), 40.0)),
			],
			"an amendable span must not change the folded path when every row arrives in order",
		);
	}

	#[test]
	fn sealing_fold_drops_a_new_row_that_lands_below_the_seal_line() {
		// This is the price of the fast path: the unsealed arm is what the dropped row was worth.
		let ops = [
			Op::Add((at_millis(0), 10.0)),
			Op::Add((at_millis(2), 20.0)),
			Op::Add((at_millis(4), 30.0)),
			Op::Add((at_millis(1), 7.0)),
		];
		let mut sealed: SealingFold<DateTime, SumFold> = SealingFold::new(millis(1), ());
		drive(&mut sealed, &ops);
		let mut unsealed: SealingFold<DateTime, SumFold> = SealingFold::unsealed(());
		drive(&mut unsealed, &ops);

		assert_eq!(unsealed.finalize(), Some(67.0), "retaining everything folds all four rows");
		assert_eq!(sealed.finalize(), Some(60.0), "a row older than the seal line never reaches the fold");
		assert_eq!(sealed.len(), 3, "and it is never counted either");
	}

	#[test]
	fn sealing_fold_default_add_remove_is_inverse() {
		assert_add_remove_is_inverse::<SealingFold<DateTime, AbsPathFold>>(
			&[(at_millis(0), 10.0f64), (at_millis(1), 20.0)],
			(at_millis(2), 30.0f64),
		);
	}

	#[test]
	fn sealing_fold_roundtrip() {
		let mut accumulator: SealingFold<DateTime, AbsPathFold> = SealingFold::new(millis(1), ());
		accumulator.add(&(at_millis(0), 10.0));
		accumulator.add(&(at_millis(1), 20.0));
		accumulator.add(&(at_millis(2), 15.0));
		let bytes = accumulator.encode_state(DateTime::EPOCH).expect("encode");
		let restored: SealingFold<DateTime, AbsPathFold> = decode(&bytes).expect("decode");
		assert_eq!(restored.finalize(), accumulator.finalize());
	}

	#[test]
	fn sealing_fold_params_drive_both_the_sealed_prefix_and_the_live_tail() {
		// Params must reach the aged step folded at add and the tail steps replayed at finalize.
		let mut accumulator: SealingFold<DateTime, ScaledPathFold> = SealingFold::new(millis(1), 2.0);
		accumulator.add(&(at_millis(0), 10.0));
		accumulator.add(&(at_millis(1), 20.0));
		accumulator.add(&(at_millis(2), 15.0));

		assert_eq!(accumulator.finalize(), Some(30.0));
	}

	#[test]
	fn sealing_fold_params_survive_the_state_roundtrip() {
		// Params are per-window state; a decode that defaulted them would finalize to 0.0.
		let mut accumulator: SealingFold<DateTime, ScaledPathFold> = SealingFold::new(millis(1), 2.0);
		accumulator.add(&(at_millis(0), 10.0));
		accumulator.add(&(at_millis(1), 20.0));
		accumulator.add(&(at_millis(2), 15.0));
		let bytes = accumulator.encode_state(DateTime::EPOCH).expect("encode");
		let restored: SealingFold<DateTime, ScaledPathFold> = decode(&bytes).expect("decode");

		assert_eq!(restored.params(), &2.0);
		assert_eq!(restored.finalize(), Some(30.0));
	}

	#[test]
	fn sealing_fold_len_counts_distinct_keys_across_the_seal_boundary() {
		// Sealing moves a key from the tail into the sealed count, so the total must not move.
		let mut accumulator: SealingFold<DateTime, AbsPathFold> = SealingFold::new(millis(1), ());
		accumulator.add(&(at_millis(0), 10.0));
		accumulator.add(&(at_millis(1), 20.0));
		assert_eq!(accumulator.len(), 2);

		accumulator.add(&(at_millis(2), 15.0));
		assert_eq!(accumulator.len(), 3, "the aged key is still one observation once sealed");
	}

	#[test]
	fn sealing_fold_len_collapses_a_repeated_key_and_drops_on_live_removal() {
		// A repeated coordinate is one corrected observation, never two.
		let mut accumulator: SealingFold<DateTime, AbsPathFold> = SealingFold::new(millis(10), ());
		accumulator.add(&(at_millis(0), 10.0));
		accumulator.add(&(at_millis(0), 25.0));
		assert_eq!(accumulator.len(), 1);

		accumulator.add(&(at_millis(1), 30.0));
		accumulator.remove(&(at_millis(1), 30.0));
		assert_eq!(accumulator.len(), 1, "removing a live key drops it from the count");
	}

	#[test]
	fn sealing_fold_unsealed_carries_params_and_never_ages() {
		// Without an amendable bound nothing seals, so a live removal must still be exact.
		let mut accumulator: SealingFold<DateTime, ScaledPathFold> = SealingFold::unsealed(3.0);
		accumulator.add(&(at_millis(0), 10.0));
		accumulator.add(&(at_millis(100), 20.0));
		assert_eq!(accumulator.finalize(), Some(30.0));

		accumulator.remove(&(at_millis(0), 10.0));
		assert_eq!(accumulator.finalize(), Some(0.0));
	}
}
