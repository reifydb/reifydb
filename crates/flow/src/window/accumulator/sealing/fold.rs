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
	type Output: Clone + Debug + PartialEq;

	fn fold(state: &mut Self::State, prev: Option<&Self::Value>, cur: &Self::Value);

	fn output(state: &Self::State) -> Option<Self::Output>;
}

#[operator_state]
pub struct SealingFold<C: Slot, F: SealFold> {
	base: SealingBase<C, F::Value>,
	sealed: F::State,
	last_sealed: Option<F::Value>,
	marker: PhantomData<fn() -> F>,
}

impl<C: Slot, F: SealFold> Clone for SealingFold<C, F> {
	fn clone(&self) -> Self {
		Self {
			base: self.base.clone(),
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
			.field("sealed", &self.sealed)
			.field("last_sealed", &self.last_sealed)
			.finish()
	}
}

impl<C: Slot, F: SealFold> Default for SealingFold<C, F> {
	fn default() -> Self {
		Self {
			base: SealingBase::default(),
			sealed: F::State::default(),
			last_sealed: None,
			marker: PhantomData,
		}
	}
}

impl<C: Slot, F: SealFold> SealingFold<C, F> {
	pub fn amendable(amendable: SlotSpan<C>) -> Self {
		Self {
			base: SealingBase::amendable(amendable),
			sealed: F::State::default(),
			last_sealed: None,
			marker: PhantomData,
		}
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
			F::fold(&mut self.sealed, self.last_sealed.as_ref(), &v);
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
			F::fold(&mut state, prev.as_ref(), v);
			prev = Some(v.clone());
		}
		F::output(&state)
	}

	fn is_empty(&self) -> bool {
		self.last_sealed.is_none() && self.base.is_tail_empty()
	}
}

impl<C: Slot + HeapSize, F: SealFold> HeapSize for SealingFold<C, F>
where
	F::Value: HeapSize,
	F::State: HeapSize,
{
	fn heap_size(&self) -> usize {
		self.base.heap_size() + self.sealed.heap_size() + self.last_sealed.heap_size()
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
	use crate::window::accumulator::testkit::assert_add_remove_is_inverse;

	struct AbsPathFold;

	impl SealFold for AbsPathFold {
		type Value = f64;
		type State = f64;
		type Output = f64;

		fn fold(state: &mut f64, prev: Option<&f64>, cur: &f64) {
			if let Some(p) = prev {
				*state += (cur - p).abs();
			}
		}

		fn output(state: &f64) -> Option<f64> {
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
		let mut accumulator: SealingFold<DateTime, AbsPathFold> = SealingFold::amendable(millis(1));
		accumulator.add(&(at_millis(0), 10.0));
		accumulator.add(&(at_millis(1), 20.0));
		accumulator.add(&(at_millis(2), 15.0));
		assert_eq!(accumulator.finalize(), Some(15.0), "sealed prefix preserves the full path exactly");
	}

	#[test]
	fn sealing_fold_aged_removal_is_dropped_no_op_but_live_removal_is_safe() {
		let mut accumulator: SealingFold<DateTime, AbsPathFold> = SealingFold::amendable(millis(1));
		accumulator.add(&(at_millis(0), 10.0));
		accumulator.add(&(at_millis(1), 20.0));
		accumulator.add(&(at_millis(2), 15.0));

		accumulator.remove(&(at_millis(0), 10.0));
		assert_eq!(accumulator.finalize(), Some(15.0), "aged removal does not disturb the sealed path");

		accumulator.remove(&(at_millis(2), 15.0));
		assert_eq!(accumulator.finalize(), Some(10.0), "live removal recomputes the path");
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
		let mut accumulator: SealingFold<DateTime, AbsPathFold> = SealingFold::amendable(millis(1));
		accumulator.add(&(at_millis(0), 10.0));
		accumulator.add(&(at_millis(1), 20.0));
		accumulator.add(&(at_millis(2), 15.0));
		let bytes = accumulator.encode_state(DateTime::EPOCH).expect("encode");
		let restored: SealingFold<DateTime, AbsPathFold> = decode(&bytes).expect("decode");
		assert_eq!(restored.finalize(), accumulator.finalize());
	}
}
