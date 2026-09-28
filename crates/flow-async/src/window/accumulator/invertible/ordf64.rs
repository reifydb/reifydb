// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cmp::Ordering,
	hash::{Hash, Hasher},
};

use reifydb_core::metrics::heap::HeapSize;
use reifydb_macro::operator_state;

#[operator_state]
#[derive(Debug, Clone, Copy)]
pub struct OrdF64(f64);

impl OrdF64 {
	#[inline]
	pub fn new(value: f64) -> Option<Self> {
		(!value.is_nan()).then_some(Self(value))
	}

	#[inline]
	pub fn get(self) -> f64 {
		self.0
	}
}

impl PartialEq for OrdF64 {
	#[inline]
	fn eq(&self, other: &Self) -> bool {
		self.0.total_cmp(&other.0) == Ordering::Equal
	}
}

impl Eq for OrdF64 {}

impl PartialOrd for OrdF64 {
	#[inline]
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for OrdF64 {
	#[inline]
	fn cmp(&self, other: &Self) -> Ordering {
		self.0.total_cmp(&other.0)
	}
}

impl Hash for OrdF64 {
	#[inline]
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.0.to_bits().hash(state);
	}
}

impl HeapSize for OrdF64 {
	fn heap_size(&self) -> usize {
		0
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::window::accumulator::{invertible::multiset::Multiset, testkit::of64};

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
}
