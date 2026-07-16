// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::atomic::{AtomicU64, Ordering};

use reifydb_value::byte_size::ByteSize;

pub struct MemoryBudget {
	used: AtomicU64,
	limit: AtomicU64,
}

impl MemoryBudget {
	pub fn new(limit: ByteSize) -> Self {
		Self {
			used: AtomicU64::new(0),
			limit: AtomicU64::new(limit.as_bytes()),
		}
	}

	pub fn charge(&self, bytes: ByteSize) {
		self.used.fetch_add(bytes.as_bytes(), Ordering::Relaxed);
	}

	pub fn try_charge(&self, bytes: ByteSize) -> bool {
		let amount = bytes.as_bytes();
		let limit = self.limit.load(Ordering::Relaxed);
		let mut current = self.used.load(Ordering::Relaxed);
		loop {
			let next = current.saturating_add(amount);
			if next > limit {
				return false;
			}
			match self.used.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
				Ok(_) => return true,
				Err(observed) => current = observed,
			}
		}
	}

	pub fn release(&self, bytes: ByteSize) {
		let amount = bytes.as_bytes();
		let mut current = self.used.load(Ordering::Relaxed);
		loop {
			let next = current.saturating_sub(amount);
			match self.used.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
				Ok(_) => return,
				Err(observed) => current = observed,
			}
		}
	}

	pub fn over_budget(&self) -> bool {
		self.used.load(Ordering::Relaxed) > self.limit.load(Ordering::Relaxed)
	}

	pub fn used(&self) -> ByteSize {
		ByteSize::from_bytes(self.used.load(Ordering::Relaxed))
	}

	pub fn limit(&self) -> ByteSize {
		ByteSize::from_bytes(self.limit.load(Ordering::Relaxed))
	}

	pub fn reset(&self) {
		self.used.store(0, Ordering::Relaxed);
	}
}

#[cfg(test)]
mod tests {
	use std::{sync::Arc, thread};

	use reifydb_value::byte_size::ByteSize;

	use super::MemoryBudget;

	#[test]
	fn charge_and_release_track_used() {
		let budget = MemoryBudget::new(ByteSize::from_kib(4));
		budget.charge(ByteSize::from_kib(1));
		budget.charge(ByteSize::from_kib(2));
		assert_eq!(budget.used(), ByteSize::from_kib(3));
		budget.release(ByteSize::from_kib(2));
		assert_eq!(budget.used(), ByteSize::from_kib(1));
	}

	#[test]
	fn over_budget_trips_only_above_limit() {
		let budget = MemoryBudget::new(ByteSize::from_kib(2));
		budget.charge(ByteSize::from_kib(2));
		assert!(!budget.over_budget(), "used == limit is within budget");
		budget.charge(ByteSize::from_bytes(1));
		assert!(budget.over_budget(), "one byte over the limit trips it");
	}

	#[test]
	fn release_saturates_at_zero() {
		let budget = MemoryBudget::new(ByteSize::from_kib(4));
		budget.charge(ByteSize::from_kib(1));
		budget.release(ByteSize::from_kib(5));
		assert_eq!(budget.used(), ByteSize::ZERO, "release never underflows the counter");
	}

	#[test]
	fn try_charge_commits_only_when_it_fits() {
		let budget = MemoryBudget::new(ByteSize::from_kib(4));
		assert!(budget.try_charge(ByteSize::from_kib(3)), "charge within limit succeeds");
		assert_eq!(budget.used(), ByteSize::from_kib(3));
		assert!(budget.try_charge(ByteSize::from_kib(1)), "charge up to exactly the limit succeeds");
		assert_eq!(budget.used(), ByteSize::from_kib(4));
	}

	#[test]
	fn try_charge_rejects_and_leaves_used_unchanged() {
		let budget = MemoryBudget::new(ByteSize::from_kib(4));
		budget.charge(ByteSize::from_kib(3));
		assert!(!budget.try_charge(ByteSize::from_kib(2)), "a charge that would exceed the limit is rejected");
		assert_eq!(budget.used(), ByteSize::from_kib(3), "a rejected charge must not mutate used");
	}

	#[test]
	fn reset_zeroes_used_and_keeps_limit() {
		let budget = MemoryBudget::new(ByteSize::from_kib(4));
		budget.charge(ByteSize::from_kib(3));
		budget.reset();
		assert_eq!(budget.used(), ByteSize::ZERO);
		assert_eq!(budget.limit(), ByteSize::from_kib(4));
	}

	#[test]
	fn concurrent_try_charge_never_exceeds_limit() {
		let budget = Arc::new(MemoryBudget::new(ByteSize::from_bytes(10_000)));
		let threads = 16;
		let attempts_per_thread = 2_000;
		let charge_amount = 7u64;

		let handles: Vec<_> = (0..threads)
			.map(|_| {
				let budget = budget.clone();
				thread::spawn(move || {
					(0..attempts_per_thread)
						.filter(|_| budget.try_charge(ByteSize::from_bytes(charge_amount)))
						.count()
				})
			})
			.collect();

		let total_successes: u64 = handles.into_iter().map(|h| h.join().unwrap() as u64).sum();

		assert!(
			budget.used().as_bytes() <= budget.limit().as_bytes(),
			"the CAS retry loop must never let concurrent charges overshoot the limit"
		);
		assert_eq!(
			budget.used().as_bytes(),
			total_successes * charge_amount,
			"used must equal exactly the sum of successful charges, with no lost or duplicated updates"
		);
	}
}
