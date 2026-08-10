// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Owned t-digest state, mirroring the `tdigest` crate (Apache-2.0) so it converts to and from
//! `tdigest::TDigest` losslessly while exposing its centroids, which is what lets it be persisted
//! structurally rather than as an opaque blob. `min`/`max` are `Option` instead of `NaN`-on-empty
//! and `sum`/`count` are [`OrderedF64`], so the whole type stays `Eq`/`Ord` and free of `NaN`.

use serde::{Deserialize, Serialize};

use crate::value::ordered_f64::OrderedF64;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Centroid {
	mean: OrderedF64,
	weight: OrderedF64,
}

impl Centroid {
	pub fn new(mean: OrderedF64, weight: OrderedF64) -> Self {
		Self {
			mean,
			weight,
		}
	}

	pub fn mean(&self) -> OrderedF64 {
		self.mean
	}

	pub fn weight(&self) -> OrderedF64 {
		self.weight
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Percentiles {
	centroids: Vec<Centroid>,
	max_size: usize,
	sum: OrderedF64,
	count: OrderedF64,
	min: Option<OrderedF64>,
	max: Option<OrderedF64>,
}

impl Percentiles {
	pub fn empty(max_size: usize) -> Self {
		Self {
			centroids: Vec::new(),
			max_size,
			sum: OrderedF64::zero(),
			count: OrderedF64::zero(),
			min: None,
			max: None,
		}
	}

	pub fn new(
		centroids: Vec<Centroid>,
		sum: OrderedF64,
		count: OrderedF64,
		min: Option<OrderedF64>,
		max: Option<OrderedF64>,
		max_size: usize,
	) -> Self {
		Self {
			centroids,
			max_size,
			sum,
			count,
			min,
			max,
		}
	}

	pub fn centroids(&self) -> &[Centroid] {
		&self.centroids
	}

	pub fn max_size(&self) -> usize {
		self.max_size
	}

	pub fn sum(&self) -> OrderedF64 {
		self.sum
	}

	pub fn count(&self) -> OrderedF64 {
		self.count
	}

	pub fn min(&self) -> Option<OrderedF64> {
		self.min
	}

	pub fn max(&self) -> Option<OrderedF64> {
		self.max
	}

	pub fn is_empty(&self) -> bool {
		self.centroids.is_empty()
	}

	pub fn mean(&self) -> Option<OrderedF64> {
		let count = self.count.value();
		if count > 0.0 {
			OrderedF64::try_from(self.sum.value() / count).ok()
		} else {
			None
		}
	}
}

impl Default for Percentiles {
	fn default() -> Self {
		Self::empty(DEFAULT_MAX_SIZE)
	}
}

pub const DEFAULT_MAX_SIZE: usize = 100;

#[cfg(test)]
mod tests {
	use postcard::{from_bytes, to_allocvec};

	use crate::value::{
		ordered_f64::OrderedF64,
		percentile::{Centroid, Percentiles},
	};

	fn f(v: f64) -> OrderedF64 {
		OrderedF64::try_from(v).expect("finite test constant")
	}

	fn sample() -> Percentiles {
		Percentiles::new(
			vec![Centroid::new(f(1.0), f(2.0)), Centroid::new(f(5.5), f(3.0))],
			f(19.5),
			f(5.0),
			Some(f(1.0)),
			Some(f(5.5)),
			100,
		)
	}

	#[test]
	fn centroids_are_readable() {
		// This accessor is the entire reason the type exists: tdigest::TDigest keeps its
		// centroids private, which forces callers to persist it as an opaque blob.
		let p = sample();
		let centroids = p.centroids();

		assert_eq!(centroids.len(), 2);
		assert_eq!(centroids[0].mean(), f(1.0));
		assert_eq!(centroids[0].weight(), f(2.0));
		assert_eq!(centroids[1].mean(), f(5.5));
		assert_eq!(centroids[1].weight(), f(3.0));
	}

	#[test]
	fn round_trips_through_the_persisted_form() {
		// The digest persists structurally, so every field including the centroids must survive.
		let p = sample();

		let bytes = to_allocvec(&p).expect("encode");
		let restored: Percentiles = from_bytes(&bytes).expect("decode");

		assert_eq!(restored, p);
	}

	#[test]
	fn an_empty_digest_reports_absent_bounds_not_a_sentinel() {
		// tdigest encodes "no observations" as NaN min/max. OrderedF64 rejects NaN, so
		// empty must be represented as an absent value; a 0.0 sentinel would be
		// indistinguishable from a genuine observation of zero.
		let p = Percentiles::empty(100);

		assert!(p.is_empty());
		assert_eq!(p.min(), None);
		assert_eq!(p.max(), None);
		assert_eq!(p.count(), OrderedF64::zero());
		assert_eq!(p.sum(), OrderedF64::zero());
		assert_eq!(p.mean(), None, "mean of no observations is absent, not NaN or zero");
	}

	#[test]
	fn an_empty_digest_round_trips() {
		// Every bucket starts empty, so a decode failure here would break the first write
		// to every new group rather than showing up under load.
		let p = Percentiles::empty(64);

		let bytes = to_allocvec(&p).expect("encode");
		let restored: Percentiles = from_bytes(&bytes).expect("decode");

		assert_eq!(restored, p);
		assert_eq!(restored.max_size(), 64, "max_size must survive; it bounds later merges");
	}

	#[test]
	fn mean_divides_sum_by_count() {
		let p = sample();
		assert_eq!(p.mean(), Some(f(19.5 / 5.0)));
	}

	#[test]
	fn truncated_bytes_are_rejected() {
		// The centroid length prefix must outrun the buffer, never yield a garbage digest.
		let p = sample();
		let bytes = to_allocvec(&p).expect("encode");
		let truncated = &bytes[..bytes.len() / 2];

		assert!(from_bytes::<Percentiles>(truncated).is_err());
	}
}
