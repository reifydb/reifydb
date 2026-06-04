// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Primitive-level gap tests for the public accumulators. The in-crate
//! `#[cfg(test)]` suite already covers the common contracts (invertibility,
//! order-independence, sealing/lateness, keyed routing, most postcard
//! roundtrips); these add the cases that suite does not exercise. Each test
//! pins a contractual invariant, not just observed output, so it fails if the
//! implementation regresses.

use reifydb_core::window::accumulator::{
	WindowAccumulator,
	invertible::{EndpointByCoord, KeyedInvertibleAccumulator, LastValue, Moments, Multiset, OrdF64},
	sealing::{SealingEndpoint, SealingMin},
};

use super::common::{assert_add_remove_is_inverse, assert_order_independent};

fn of(v: f64) -> OrdF64 {
	OrdF64::new(v).expect("not nan")
}

#[test]
fn endpoint_by_coord_postcard_roundtrip() {
	let mut ends: EndpointByCoord<u64, i64> = EndpointByCoord::default();
	ends.observe(10, 100);
	ends.observe(30, 300);
	ends.observe(20, 200);
	let bytes = postcard::to_allocvec(&ends).expect("serialize");
	let restored: EndpointByCoord<u64, i64> = postcard::from_bytes(&bytes).expect("deserialize");
	assert_eq!(restored, ends);
	assert_eq!(restored.earliest(), Some((&10, &100)), "earliest endpoint survives roundtrip");
	assert_eq!(restored.latest(), Some((&30, &300)), "latest endpoint survives roundtrip");
}

#[test]
fn last_value_postcard_roundtrip() {
	let mut lv: LastValue<i64> = LastValue::default();
	lv.add(&42);
	let bytes = postcard::to_allocvec(&lv).expect("serialize");
	let restored: LastValue<i64> = postcard::from_bytes(&bytes).expect("deserialize");
	assert_eq!(restored, lv);
	assert_eq!(restored.finalize(), Some(42), "retained value survives roundtrip");
}

#[test]
fn sealing_min_default_is_fully_invertible() {
	// Symmetric to the in-crate SealingMax default-invertibility test: with
	// no lateness bound nothing seals, so add/remove is a pure inverse even
	// when the probe is a new minimum.
	assert_add_remove_is_inverse::<SealingMin<u64, i64>>(&[(1u64, 10i64), (2, 20), (3, 30)], (4u64, -5i64));

	let mut accumulator: SealingMin<u64, i64> = SealingMin::default();
	accumulator.add(&(0, 5));
	accumulator.add(&(100, 1));
	accumulator.remove(&(100, 1));
	assert_eq!(accumulator.finalize(), Some(5), "removing the min reveals the prior min when nothing has sealed");
}

#[test]
fn sealing_endpoint_late_earlier_arrival_updates_open() {
	// An observation that arrives after a seal but is *earlier* than the
	// sealed open must become the new open: open is the earliest
	// observation overall, not merely the first to seal.
	let mut accumulator: SealingEndpoint<u64, i64> = SealingEndpoint::with_lateness(10);
	accumulator.add(&(5, 50));
	accumulator.add(&(20, 200)); // hw=20; coord 5 ages (20-5=15>10) -> sealed_open=(5,50)
	assert_eq!(accumulator.open(), Some(&50), "open frozen to the earliest seen so far");

	accumulator.add(&(2, 999)); // hw=20; coord 2 ages immediately (18>10) and 2 < 5
	assert_eq!(accumulator.open(), Some(&999), "a genuinely earlier late arrival reclaims open");
	assert_eq!(accumulator.close(), Some(&200), "close unchanged by the earlier arrival");
}

#[test]
fn sealing_endpoint_late_middle_arrival_keeps_open() {
	// Counterpart to the above: a late arrival whose coord is later than the
	// sealed open must NOT move open.
	let mut accumulator: SealingEndpoint<u64, i64> = SealingEndpoint::with_lateness(10);
	accumulator.add(&(2, 999));
	accumulator.add(&(20, 200)); // hw=20; coord 2 ages -> sealed_open=(2,999)
	assert_eq!(accumulator.open(), Some(&999));

	accumulator.add(&(5, 50)); // coord 5 ages (15>10) but 5 > 2, so open stays (2,999)
	assert_eq!(accumulator.open(), Some(&999), "a later late arrival does not displace the earlier open");
}

#[test]
fn moments_single_element_has_zero_variance() {
	let mut m = Moments::default();
	m.add(5.0);
	assert_eq!(m.count(), 1);
	assert_eq!(m.mean(), Some(5.0));
	assert_eq!(m.variance_pop(), Some(0.0), "a single observation has zero population variance");
	assert_eq!(m.stddev_pop(), Some(0.0));
}

#[test]
fn moments_variance_is_never_negative_under_cancellation() {
	// Identical large values produce a true variance of 0; floating-point
	// cancellation can push the raw sum_sq/n - mean^2 slightly negative, so
	// the accumulator must clamp. The invariant: variance is never negative.
	let mut m = Moments::default();
	for _ in 0..5 {
		m.add(1.0e8);
	}
	let var = m.variance_pop().expect("non-empty");
	assert!(var >= 0.0, "population variance must never be negative, got {var}");
	assert!(var < 1.0, "variance of identical values must be ~0, got {var}");
}

#[test]
fn multiset_quantile_clamps_out_of_range_probes() {
	let mut ms: Multiset<u64> = Multiset::default();
	for v in [10u64, 20, 30] {
		ms.add(v);
	}
	assert_eq!(ms.quantile(-1.0), Some(&10), "q below 0 clamps to the minimum");
	assert_eq!(ms.quantile(2.0), Some(&30), "q above 1 clamps to the maximum");
}

#[test]
fn multiset_empty_accessors_are_none() {
	let ms: Multiset<u64> = Multiset::default();
	assert!(ms.is_empty());
	assert_eq!(ms.min(), None);
	assert_eq!(ms.max(), None);
	assert_eq!(ms.mode(), None);
	assert_eq!(ms.median(), None);
	assert_eq!(ms.quantile(0.5), None);
}

#[test]
fn keyed_invertible_is_order_independent_for_exact_sums() {
	// Per-key Moments over small integer-valued contributions: the sums are
	// exact in f64, so reordering must not change finalize(). Uses distinct
	// and colliding keys to cover the per-key routing.
	assert_order_independent::<KeyedInvertibleAccumulator<u64, Moments>>(&[
		(1u64, 10.0f64),
		(2, 20.0),
		(1, 5.0),
		(3, 7.0),
		(2, 3.0),
	]);
}

#[test]
fn ordf64_total_order_separates_signed_zero_and_infinities() {
	// total_cmp orders -0.0 strictly below +0.0 and they are distinct keys
	// (bitwise), which is what lets a Multiset<OrdF64> treat them as two
	// values rather than collapsing them.
	let neg_zero = of(-0.0);
	let pos_zero = of(0.0);
	assert!(neg_zero < pos_zero, "-0.0 must sort below +0.0 under total order");
	assert_ne!(neg_zero, pos_zero, "-0.0 and +0.0 must be distinct keys");

	let neg_inf = of(f64::NEG_INFINITY);
	let pos_inf = of(f64::INFINITY);
	assert!(neg_inf < neg_zero);
	assert!(pos_zero < pos_inf);
	assert!(neg_inf < pos_inf);

	assert!(OrdF64::new(f64::NAN).is_none(), "NaN is rejected");
}
