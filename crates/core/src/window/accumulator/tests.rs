// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};

use super::{WindowAccumulator, invertible::*, sealing::*};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct SumAccumulator {
	moments: Moments,
}

impl WindowAccumulator for SumAccumulator {
	type Contribution = f64;
	type Output = OrdF64;

	fn add(&mut self, contribution: &f64) {
		self.moments.add(*contribution);
	}

	fn remove(&mut self, contribution: &f64) {
		self.moments.remove(*contribution);
	}

	fn finalize(&self) -> Option<OrdF64> {
		(!self.moments.is_empty()).then(|| OrdF64::new(self.moments.sum()).expect("finite sum"))
	}

	fn is_empty(&self) -> bool {
		self.moments.is_empty()
	}
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct MinAccumulator {
	values: Multiset<OrdF64>,
}

impl WindowAccumulator for MinAccumulator {
	type Contribution = OrdF64;
	type Output = OrdF64;

	fn add(&mut self, contribution: &OrdF64) {
		self.values.add(*contribution);
	}

	fn remove(&mut self, contribution: &OrdF64) {
		self.values.remove(contribution);
	}

	fn finalize(&self) -> Option<OrdF64> {
		self.values.min().copied()
	}

	fn is_empty(&self) -> bool {
		self.values.is_empty()
	}
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct LastAccumulator {
	retained: RetainedMap<u64, i64>,
}

impl WindowAccumulator for LastAccumulator {
	type Contribution = (u64, i64);
	type Output = i64;

	fn add(&mut self, contribution: &(u64, i64)) {
		self.retained.insert(contribution.0, contribution.1);
	}

	fn remove(&mut self, contribution: &(u64, i64)) {
		self.retained.remove(&contribution.0);
	}

	fn finalize(&self) -> Option<i64> {
		self.retained.entries().last_key_value().map(|(_, v)| *v)
	}

	fn is_empty(&self) -> bool {
		self.retained.is_empty()
	}
}

fn of64(v: f64) -> OrdF64 {
	OrdF64::new(v).expect("not nan")
}

fn assert_add_remove_is_inverse<A: WindowAccumulator>(initial: &[A::Contribution], probe: A::Contribution) {
	let mut accumulator = A::default();
	for c in initial {
		accumulator.add(c);
	}
	let before = accumulator.finalize();
	accumulator.add(&probe);
	accumulator.remove(&probe);
	assert_eq!(accumulator.finalize(), before, "add then remove must restore finalize()");
}

fn assert_order_independent<A>(contributions: &[A::Contribution])
where
	A: WindowAccumulator,
{
	let mut forward = A::default();
	for c in contributions {
		forward.add(c);
	}
	let mut backward = A::default();
	for c in contributions.iter().rev() {
		backward.add(c);
	}
	assert_eq!(forward.finalize(), backward.finalize(), "finalize() must be order-independent");
}

#[test]
fn sum_add_remove_is_inverse() {
	assert_add_remove_is_inverse::<SumAccumulator>(&[1.0, 2.0, 3.0], 7.0);
}

#[test]
fn min_add_remove_is_inverse_even_when_probe_is_new_minimum() {
	assert_add_remove_is_inverse::<MinAccumulator>(&[of64(5.0), of64(8.0), of64(6.0)], of64(1.0));
}

#[test]
fn min_add_remove_is_inverse_for_duplicate_value() {
	assert_add_remove_is_inverse::<MinAccumulator>(&[of64(5.0), of64(5.0), of64(8.0)], of64(5.0));
}

#[test]
fn retained_add_remove_is_inverse_for_fresh_key() {
	assert_add_remove_is_inverse::<LastAccumulator>(&[(1u64, 10i64), (2, 20)], (3u64, 30i64));
}

#[test]
fn sum_is_order_independent() {
	assert_order_independent::<SumAccumulator>(&[1.0, 2.0, 4.0, 8.0]);
}

#[test]
fn min_is_order_independent() {
	assert_order_independent::<MinAccumulator>(&[of64(3.0), of64(1.0), of64(4.0), of64(1.0), of64(5.0)]);
}

#[test]
fn retained_is_order_independent_for_distinct_keys() {
	assert_order_independent::<LastAccumulator>(&[(1u64, 10i64), (2, 20), (3, 30)]);
}

#[test]
fn retained_add_over_existing_key_then_remove_deletes() {
	let mut accumulator = LastAccumulator::default();
	accumulator.add(&(1u64, 10i64));
	accumulator.add(&(1u64, 99i64));
	accumulator.remove(&(1u64, 99i64));
	assert!(accumulator.is_empty());
	assert_eq!(accumulator.finalize(), None);
}

#[test]
fn moments_drains_to_exact_zero() {
	let mut m = Moments::default();
	m.add(0.1);
	m.add(0.2);
	m.remove(0.1);
	m.remove(0.2);
	assert_eq!(m.count(), 0);
	assert_eq!(m.sum(), 0.0, "fully drained accumulator resets sum to exact zero");
	assert!(m.is_empty());
	assert_eq!(m.mean(), None);
	assert_eq!(m.variance_pop(), None);
}

#[test]
fn moments_mean_and_variance() {
	let mut m = Moments::default();
	for x in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
		m.add(x);
	}
	assert_eq!(m.count(), 8);
	assert_eq!(m.mean(), Some(5.0));
	assert_eq!(m.variance_pop(), Some(4.0));
	assert_eq!(m.stddev_pop(), Some(2.0));
}

#[test]
fn multiset_min_max_distinct_total() {
	let mut ms: Multiset<u64> = Multiset::default();
	for v in [5u64, 1, 5, 9, 1] {
		ms.add(v);
	}
	assert_eq!(ms.min(), Some(&1));
	assert_eq!(ms.max(), Some(&9));
	assert_eq!(ms.distinct(), 3);
	assert_eq!(ms.total(), 5);

	ms.remove(&1);
	assert_eq!(ms.min(), Some(&1), "one occurrence of 1 remains");
	assert_eq!(ms.distinct(), 3);
	ms.remove(&1);
	assert_eq!(ms.min(), Some(&5), "last occurrence of 1 removed, min rises");
	assert_eq!(ms.distinct(), 2);
}

#[test]
fn multiset_quantile_and_median_nearest_rank() {
	let mut ms: Multiset<u64> = Multiset::default();
	for v in [1u64, 2, 3, 4, 5] {
		ms.add(v);
	}
	assert_eq!(ms.quantile(0.0), Some(&1));
	assert_eq!(ms.median(), Some(&3));
	assert_eq!(ms.quantile(1.0), Some(&5));
	assert_eq!(ms.quantile(0.5), Some(&3));
}

#[test]
fn multiset_mode_breaks_ties_to_smallest_value() {
	let mut ms: Multiset<u64> = Multiset::default();
	for v in [7u64, 7, 3, 3, 9] {
		ms.add(v);
	}
	assert_eq!(ms.mode(), Some(&3), "3 and 7 tie at count 2; smallest wins deterministically");
}

#[test]
fn ordf64_total_order_and_nan_rejection() {
	assert!(OrdF64::new(f64::NAN).is_none());
	assert!(of64(-1.0) < of64(0.0));
	assert!(of64(0.0) < of64(1.0));
	let mut ms: Multiset<OrdF64> = Multiset::default();
	ms.add(of64(2.5));
	ms.add(of64(-3.0));
	ms.add(of64(2.5));
	assert_eq!(ms.min(), Some(&of64(-3.0)));
	assert_eq!(ms.max(), Some(&of64(2.5)));
	assert_eq!(ms.total(), 3);
}

#[test]
fn moments_postcard_roundtrip() {
	let mut m = Moments::default();
	m.add(1.5);
	m.add(2.5);
	let bytes = to_allocvec(&m).expect("serialize");
	let restored: Moments = from_bytes(&bytes).expect("deserialize");
	assert_eq!(restored, m);
}

#[test]
fn multiset_postcard_roundtrip() {
	let mut ms: Multiset<OrdF64> = Multiset::default();
	ms.add(of64(1.0));
	ms.add(of64(1.0));
	ms.add(of64(2.0));
	let bytes = to_allocvec(&ms).expect("serialize");
	let restored: Multiset<OrdF64> = from_bytes(&bytes).expect("deserialize");
	assert_eq!(restored, ms);
	assert_eq!(restored.min(), Some(&of64(1.0)));
	assert_eq!(restored.total(), 3);
}

#[test]
fn retained_map_postcard_roundtrip() {
	let mut rm: RetainedMap<u64, i64> = RetainedMap::default();
	rm.insert(1, 10);
	rm.insert(2, 20);
	let bytes = to_allocvec(&rm).expect("serialize");
	let restored: RetainedMap<u64, i64> = from_bytes(&bytes).expect("deserialize");
	assert_eq!(restored, rm);
	assert_eq!(restored.len(), 2);
}

#[test]
fn last_value_is_last_write_wins() {
	let mut lv: LastValue<i64> = LastValue::default();
	assert!(lv.is_empty());
	lv.add(&10);
	lv.add(&20);
	assert_eq!(lv.finalize(), Some(20));
	lv.remove(&20);
	assert!(lv.is_empty());
	assert_eq!(lv.finalize(), None);
}

#[test]
fn endpoint_by_coord_tracks_both_ends_and_is_removal_safe() {
	let mut ends: EndpointByCoord<u64, i64> = EndpointByCoord::default();
	ends.observe(10, 100);
	ends.observe(30, 300);
	ends.observe(20, 200);
	assert_eq!(ends.earliest(), Some((&10, &100)));
	assert_eq!(ends.earliest_coord(), Some(&10));
	assert_eq!(ends.latest(), Some((&30, &300)));
	assert_eq!(ends.latest_coord(), Some(&30));

	ends.forget(&10);
	assert_eq!(ends.earliest(), Some((&20, &200)), "forgetting the min reveals the prior min");
	ends.forget(&30);
	assert_eq!(ends.latest(), Some((&20, &200)), "forgetting the max reveals the prior max");

	ends.observe(20, 999);
	assert_eq!(ends.earliest_value(), Some(&999));
	assert_eq!(ends.latest_value(), Some(&999));

	ends.forget(&20);
	assert!(ends.is_empty());
	assert_eq!(ends.earliest(), None);
	assert_eq!(ends.latest(), None);
}

#[test]
fn retained_acc_add_remove_is_inverse_for_fresh_key() {
	assert_add_remove_is_inverse::<RetainedAccumulator<u64, i64>>(&[(1u64, 10i64), (2, 20)], (3u64, 30i64));
}

#[test]
fn retained_acc_is_order_independent_for_distinct_keys() {
	assert_order_independent::<RetainedAccumulator<u64, i64>>(&[(1u64, 10i64), (2, 20), (3, 30)]);
}

#[test]
fn retained_acc_finalize_returns_whole_map() {
	let mut accumulator: RetainedAccumulator<u64, i64> = RetainedAccumulator::default();
	assert!(accumulator.is_empty());
	assert_eq!(accumulator.finalize(), None);
	accumulator.add(&(2, 20));
	accumulator.add(&(1, 10));
	let map = accumulator.finalize().expect("non-empty");
	assert_eq!(map.len(), 2);
	assert_eq!(map.get(&1), Some(&10));
	assert_eq!(map.get(&2), Some(&20));
}

#[test]
fn retained_acc_add_over_existing_key_then_remove_deletes() {
	let mut accumulator: RetainedAccumulator<u64, i64> = RetainedAccumulator::default();
	accumulator.add(&(1, 10));
	accumulator.add(&(1, 99));
	accumulator.remove(&(1, 99));
	assert!(accumulator.is_empty());
	assert_eq!(accumulator.finalize(), None);
}

#[test]
fn retained_acc_postcard_roundtrip() {
	let mut accumulator: RetainedAccumulator<u64, i64> = RetainedAccumulator::default();
	accumulator.add(&(1, 10));
	accumulator.add(&(2, 20));
	let bytes = to_allocvec(&accumulator).expect("serialize");
	let restored: RetainedAccumulator<u64, i64> = from_bytes(&bytes).expect("deserialize");
	assert_eq!(restored, accumulator);
}

#[test]
fn keyed_invertible_routes_per_key_and_drops_empty_keys() {
	let mut accumulator: KeyedInvertibleAccumulator<u64, Moments> = KeyedInvertibleAccumulator::default();
	assert!(accumulator.is_empty());
	assert_eq!(accumulator.finalize(), None);

	accumulator.add(&(1, 10.0));
	accumulator.add(&(1, 20.0));
	accumulator.add(&(2, 5.0));
	let out = accumulator.finalize().expect("non-empty");
	assert_eq!(out.len(), 2);
	assert_eq!(out.get(&1).map(|m| m.sum()), Some(30.0));
	assert_eq!(out.get(&2).map(|m| m.sum()), Some(5.0));

	accumulator.remove(&(2, 5.0));
	let out = accumulator.finalize().expect("non-empty");
	assert_eq!(out.len(), 1, "key 2 drained to empty and was dropped");
	assert!(out.get(&2).is_none());
}

#[test]
fn keyed_invertible_add_remove_is_inverse() {
	assert_add_remove_is_inverse::<KeyedInvertibleAccumulator<u64, Moments>>(
		&[(1u64, 10.0f64), (2, 20.0), (1, 30.0)],
		(3u64, 7.0f64),
	);
}

#[test]
fn keyed_invertible_postcard_roundtrip() {
	let mut accumulator: KeyedInvertibleAccumulator<u64, Moments> = KeyedInvertibleAccumulator::default();
	accumulator.add(&(1, 10.0));
	accumulator.add(&(2, 20.0));
	let bytes = to_allocvec(&accumulator).expect("serialize");
	let restored: KeyedInvertibleAccumulator<u64, Moments> = from_bytes(&bytes).expect("deserialize");
	assert_eq!(restored, accumulator);
}

#[test]
fn sealing_max_seals_aged_and_keeps_recent_tail_removal_safe() {
	let mut accumulator: SealingMax<u64, i64> = SealingMax::with_lateness(10);
	accumulator.add(&(0, 5));
	accumulator.add(&(5, 8));
	accumulator.add(&(12, 3));
	assert_eq!(accumulator.max(), Some(8));

	accumulator.remove(&(0, 5));
	assert_eq!(accumulator.max(), Some(8), "aged removal does not disturb the sealed max");

	accumulator.remove(&(5, 8));
	assert_eq!(accumulator.max(), Some(5), "tail max 8 removed; falls back to sealed 5");
}

#[test]
fn sealing_min_seals_aged_extreme() {
	let mut accumulator: SealingMin<u64, i64> = SealingMin::with_lateness(10);
	accumulator.add(&(0, 2));
	accumulator.add(&(5, 9));
	accumulator.add(&(12, 7));
	assert_eq!(accumulator.min(), Some(2));
	accumulator.remove(&(5, 9));
	assert_eq!(accumulator.min(), Some(2), "sealed min 2 survives removal of a live event");
}

#[test]
fn sealing_max_default_never_seals_and_is_fully_invertible() {
	assert_add_remove_is_inverse::<SealingMax<u64, i64>>(&[(1u64, 10i64), (2, 20)], (3u64, 30i64));
	let mut accumulator: SealingMax<u64, i64> = SealingMax::default();
	accumulator.add(&(0, 5));
	accumulator.add(&(100, 8));
	accumulator.remove(&(100, 8));
	assert_eq!(accumulator.max(), Some(5), "removing the max reveals the prior max (no sealing)");
}

#[test]
fn sealing_endpoint_freezes_open_and_tracks_live_close() {
	let mut accumulator: SealingEndpoint<u64, i64> = SealingEndpoint::with_lateness(10);
	accumulator.add(&(0, 100));
	accumulator.add(&(5, 200));
	accumulator.add(&(12, 300));
	assert_eq!(accumulator.open(), Some(&100), "open frozen to the earliest observation");
	assert_eq!(accumulator.close(), Some(&300), "close is the latest live observation");

	accumulator.remove(&(0, 100));
	assert_eq!(accumulator.open(), Some(&100), "aged open removal is a dropped no-op (frozen)");

	accumulator.remove(&(12, 300));
	assert_eq!(accumulator.close(), Some(&200), "removing the latest reveals the prior latest in the tail");

	accumulator.add(&(20, 400));
	assert_eq!(accumulator.open(), Some(&100));
	assert_eq!(accumulator.close(), Some(&400));
}

#[test]
fn sealing_endpoint_default_is_fully_invertible() {
	assert_add_remove_is_inverse::<SealingEndpoint<u64, i64>>(&[(1u64, 10i64), (3, 30)], (2u64, 20i64));
}

#[test]
fn sealing_primitives_postcard_roundtrip() {
	let mut mx: SealingMax<u64, i64> = SealingMax::with_lateness(10);
	mx.add(&(0, 5));
	mx.add(&(12, 8));
	let bytes = to_allocvec(&mx).expect("serialize");
	let restored: SealingMax<u64, i64> = from_bytes(&bytes).expect("deserialize");
	assert_eq!(restored, mx);

	let mut ep: SealingEndpoint<u64, i64> = SealingEndpoint::with_lateness(10);
	ep.add(&(0, 100));
	ep.add(&(12, 300));
	let bytes = to_allocvec(&ep).expect("serialize");
	let restored: SealingEndpoint<u64, i64> = from_bytes(&bytes).expect("deserialize");
	assert_eq!(restored, ep);
}

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
fn sealing_fold_no_lateness_sums_all_adjacent_steps() {
	let mut accumulator: SealingFold<u64, AbsPathFold> = SealingFold::default();
	accumulator.add(&(0, 10.0));
	accumulator.add(&(1, 20.0));
	accumulator.add(&(2, 15.0));

	assert_eq!(accumulator.finalize(), Some(15.0));
}

#[test]
fn sealing_fold_seals_aged_prefix_exactly_for_forward_data() {
	let mut accumulator: SealingFold<u64, AbsPathFold> = SealingFold::with_lateness(1);
	accumulator.add(&(0, 10.0));
	accumulator.add(&(1, 20.0));
	accumulator.add(&(2, 15.0));
	assert_eq!(accumulator.finalize(), Some(15.0), "sealed prefix preserves the full path exactly");
}

#[test]
fn sealing_fold_aged_removal_is_dropped_no_op_but_live_removal_is_safe() {
	let mut accumulator: SealingFold<u64, AbsPathFold> = SealingFold::with_lateness(1);
	accumulator.add(&(0, 10.0));
	accumulator.add(&(1, 20.0));
	accumulator.add(&(2, 15.0));

	accumulator.remove(&(0, 10.0));
	assert_eq!(accumulator.finalize(), Some(15.0), "aged removal does not disturb the sealed path");

	accumulator.remove(&(2, 15.0));
	assert_eq!(accumulator.finalize(), Some(10.0), "live removal recomputes the path");
}

#[test]
fn sealing_fold_default_add_remove_is_inverse() {
	assert_add_remove_is_inverse::<SealingFold<u64, AbsPathFold>>(&[(0u64, 10.0f64), (1, 20.0)], (2u64, 30.0f64));
}

#[test]
fn sealing_fold_postcard_roundtrip() {
	let mut accumulator: SealingFold<u64, AbsPathFold> = SealingFold::with_lateness(1);
	accumulator.add(&(0, 10.0));
	accumulator.add(&(1, 20.0));
	accumulator.add(&(2, 15.0));
	let bytes = to_allocvec(&accumulator).expect("serialize");
	let restored: SealingFold<u64, AbsPathFold> = from_bytes(&bytes).expect("deserialize");
	assert_eq!(restored.finalize(), accumulator.finalize());
}

#[test]
fn sealing_tail_drops_aged_keeps_recent() {
	let mut tail: SealingTail<u64, i64> = SealingTail::with_lateness(10);
	tail.add(0, 1);
	tail.add(5, 2);
	tail.add(12, 3);
	let keys: Vec<u64> = tail.tail().keys().copied().collect();
	assert_eq!(keys, vec![5, 12], "aged prefix dropped, recent tail kept in order");
	tail.remove(&5);
	let keys: Vec<u64> = tail.tail().keys().copied().collect();
	assert_eq!(keys, vec![12], "live tail entry removable");
}

#[test]
fn sealing_tail_default_never_drops() {
	let mut tail: SealingTail<u64, i64> = SealingTail::default();
	tail.add(0, 1);
	tail.add(100, 2);
	assert_eq!(tail.tail().len(), 2, "with no lateness bound nothing is dropped");
}

#[test]
fn sealing_tail_postcard_roundtrip() {
	let mut tail: SealingTail<u64, i64> = SealingTail::with_lateness(10);
	tail.add(0, 1);
	tail.add(12, 3);
	let bytes = to_allocvec(&tail).expect("serialize");
	let restored: SealingTail<u64, i64> = from_bytes(&bytes).expect("deserialize");
	assert_eq!(restored, tail);
}

#[test]
fn tail_acc_no_lateness_retains_whole_window_like_retained_acc() {
	let mut accumulator: TailAccumulator<u64, i64> = TailAccumulator::default();
	accumulator.add(&(0, 10));
	accumulator.add(&(100, 20));
	let map = accumulator.finalize().expect("non-empty");
	assert_eq!(map.len(), 2);
	assert_eq!(map.get(&0), Some(&10));
	assert_eq!(map.get(&100), Some(&20));
}

#[test]
fn tail_acc_default_add_remove_is_inverse() {
	assert_add_remove_is_inverse::<TailAccumulator<u64, i64>>(&[(0u64, 10i64), (1, 20)], (2u64, 30i64));
}

#[test]
fn tail_acc_with_lateness_drops_aged_from_finalize() {
	let mut accumulator: TailAccumulator<u64, i64> = TailAccumulator::with_lateness(10);
	accumulator.add(&(0, 10));
	accumulator.add(&(5, 20));
	accumulator.add(&(12, 30));
	let map = accumulator.finalize().expect("non-empty");
	assert_eq!(map.keys().copied().collect::<Vec<_>>(), vec![5, 12], "aged prefix dropped from the emitted map");
}

#[test]
fn tail_acc_postcard_roundtrip() {
	let mut accumulator: TailAccumulator<u64, i64> = TailAccumulator::with_lateness(10);
	accumulator.add(&(0, 1));
	accumulator.add(&(12, 3));
	let bytes = to_allocvec(&accumulator).expect("serialize");
	let restored: TailAccumulator<u64, i64> = from_bytes(&bytes).expect("deserialize");
	assert_eq!(restored, accumulator);
}
