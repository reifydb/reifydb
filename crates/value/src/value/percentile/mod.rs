// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Owned t-digest state.
//!
//! The representation mirrors the `tdigest` crate (Apache-2.0) so a [`Percentiles`] can be
//! converted to and from `tdigest::TDigest` losslessly at an operator boundary while the
//! estimation algorithms are still borrowed from that crate. Unlike `TDigest`, the centroids
//! are readable ([`Percentiles::centroids`]), which is what lets this type be persisted
//! structurally instead of as an opaque serialized blob.
//!
//! Two deliberate departures from `TDigest`:
//!
//! - `min`/`max` are `Option` rather than `NaN`-on-empty. [`OrderedF64`] rejects `NaN`, and an explicit absent value
//!   states "no observations yet" instead of encoding it in a float.
//! - `sum`/`count` are [`OrderedF64`], so the whole type is `Eq`/`Ord` and can be compared and hashed without float
//!   caveats.

use rkyv::{Archive as RkyvArchive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use crate::value::ordered_f64::OrderedF64;

#[derive(
	Debug,
	Copy,
	Clone,
	PartialEq,
	Eq,
	PartialOrd,
	Ord,
	Hash,
	Serialize,
	Deserialize,
	RkyvArchive,
	RkyvSerialize,
	RkyvDeserialize,
)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, RkyvArchive, RkyvSerialize, RkyvDeserialize)]
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
	use rkyv::{access, deserialize, rancor::Error, to_bytes};

	use crate::value::{
		ordered_f64::OrderedF64,
		percentile::{ArchivedPercentiles, Centroid, Percentiles},
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
	fn round_trips_through_the_archived_form() {
		// Persistence goes through rkyv directly rather than a serialized blob, so every
		// field including the centroid vector must survive archive -> access -> deserialize.
		let p = sample();

		let bytes = to_bytes::<Error>(&p).expect("archive");
		let archived = access::<ArchivedPercentiles, Error>(&bytes).expect("access");
		let restored: Percentiles = deserialize::<Percentiles, Error>(archived).expect("deserialize");

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

		let bytes = to_bytes::<Error>(&p).expect("archive");
		let archived = access::<ArchivedPercentiles, Error>(&bytes).expect("access");
		let restored: Percentiles = deserialize::<Percentiles, Error>(archived).expect("deserialize");

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
		// bytecheck must reject a short buffer rather than hand back a garbage digest.
		let p = sample();
		let bytes = to_bytes::<Error>(&p).expect("archive");
		let truncated = &bytes[..bytes.len() / 2];

		assert!(access::<ArchivedPercentiles, Error>(truncated).is_err());
	}
}
