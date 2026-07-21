// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashMap,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::{byte_size::ByteSize, count::Count, reifydb_assertions};

use crate::{
	interface::catalog::flow::FlowNodeId, metrics::heap::StateMemory,
	window::engine::config::DEFAULT_OPERATOR_STATE_BUDGET,
};

pub const LEASE_FLOOR: ByteSize = ByteSize::from_bytes(8 * 1024 * 1024);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LeaseGrant(ByteSize);

impl LeaseGrant {
	pub fn bytes(&self) -> ByteSize {
		self.0
	}
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LeaseReport {
	pub state: StateMemory,
	pub row_numbers: StateMemory,
}

impl LeaseReport {
	pub fn total_bytes(&self) -> ByteSize {
		ByteSize::from_bytes(self.state.bytes.as_bytes().saturating_add(self.row_numbers.bytes.as_bytes()))
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseHealth {
	Reporting,
	Silent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OperatorLease {
	pub node: FlowNodeId,
	pub grant: LeaseGrant,
	pub last: LeaseReport,
	pub health: LeaseHealth,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OperatorStateBudgetSnapshot {
	pub budget: ByteSize,
	pub resident: ByteSize,
	pub dirty: ByteSize,
	pub in_flight: ByteSize,
	pub leased: ByteSize,
}

impl OperatorStateBudgetSnapshot {
	pub fn total(&self) -> ByteSize {
		ByteSize::from_bytes(
			self.resident
				.as_bytes()
				.saturating_add(self.dirty.as_bytes())
				.saturating_add(self.in_flight.as_bytes())
				.saturating_add(self.leased.as_bytes()),
		)
	}

	pub fn overage(&self) -> ByteSize {
		ByteSize::from_bytes(self.total().as_bytes().saturating_sub(self.budget.as_bytes()))
	}
}

struct LeaseState {
	grant: u64,
	reported: Option<u64>,
}

impl LeaseState {
	fn charged(&self) -> u64 {
		self.grant.max(self.reported.unwrap_or(0))
	}
}

pub struct OperatorStateBudget {
	budget: AtomicU64,
	clean: AtomicU64,
	dirty: AtomicU64,
	in_flight: AtomicU64,
	leased: AtomicU64,
	evictions: AtomicU64,
	leases: Mutex<HashMap<FlowNodeId, LeaseState>>,
}

#[derive(Clone)]
pub struct OperatorStateBudgetHandle(Arc<OperatorStateBudget>);

impl Default for OperatorStateBudgetHandle {
	fn default() -> Self {
		Self::new(DEFAULT_OPERATOR_STATE_BUDGET)
	}
}

impl OperatorStateBudgetHandle {
	pub fn new(budget: ByteSize) -> Self {
		Self(Arc::new(OperatorStateBudget {
			budget: AtomicU64::new(budget.as_bytes()),
			clean: AtomicU64::new(0),
			dirty: AtomicU64::new(0),
			in_flight: AtomicU64::new(0),
			leased: AtomicU64::new(0),
			evictions: AtomicU64::new(0),
			leases: Mutex::new(HashMap::new()),
		}))
	}

	pub fn set_budget(&self, budget: ByteSize) {
		self.0.budget.store(budget.as_bytes(), Ordering::Relaxed);
	}

	pub fn charge_clean(&self, bytes: ByteSize) {
		self.0.clean.fetch_add(bytes.as_bytes(), Ordering::Relaxed);
	}

	pub fn release_clean(&self, bytes: ByteSize) {
		release(&self.0.clean, bytes.as_bytes());
	}

	pub fn charge_dirty(&self, bytes: ByteSize) {
		self.0.dirty.fetch_add(bytes.as_bytes(), Ordering::Relaxed);
	}

	pub fn release_dirty(&self, bytes: ByteSize) {
		release(&self.0.dirty, bytes.as_bytes());
	}

	pub fn charge_in_flight(&self, bytes: ByteSize) {
		self.0.in_flight.fetch_add(bytes.as_bytes(), Ordering::Relaxed);
	}

	pub fn release_in_flight(&self, bytes: ByteSize) {
		release(&self.0.in_flight, bytes.as_bytes());
	}

	pub fn record_eviction(&self, bytes: ByteSize) {
		let _ = bytes;
		self.0.evictions.fetch_add(1, Ordering::Relaxed);
	}

	pub fn evictions(&self) -> Count {
		Count::new(self.0.evictions.load(Ordering::Relaxed))
	}

	pub fn over_budget(&self) -> bool {
		self.snapshot().overage().as_bytes() > 0
	}

	pub fn snapshot(&self) -> OperatorStateBudgetSnapshot {
		OperatorStateBudgetSnapshot {
			budget: ByteSize::from_bytes(self.0.budget.load(Ordering::Relaxed)),
			resident: ByteSize::from_bytes(self.0.clean.load(Ordering::Relaxed)),
			dirty: ByteSize::from_bytes(self.0.dirty.load(Ordering::Relaxed)),
			in_flight: ByteSize::from_bytes(self.0.in_flight.load(Ordering::Relaxed)),
			leased: ByteSize::from_bytes(self.0.leased.load(Ordering::Relaxed)),
		}
	}

	pub fn grant_lease(&self, node: FlowNodeId, requested: ByteSize) -> LeaseGrant {
		let mut leases = self.0.leases.lock();
		let snapshot = self.snapshot();
		let used = snapshot.total().as_bytes().saturating_sub(leases.get(&node).map_or(0, |l| l.charged()));
		let headroom = snapshot.budget.as_bytes().saturating_sub(used);
		let granted = requested.as_bytes().min(headroom).max(LEASE_FLOOR.as_bytes());
		leases.insert(
			node,
			LeaseState {
				grant: granted,
				reported: None,
			},
		);
		Self::recompute_leased(&self.0, &leases);
		LeaseGrant(ByteSize::from_bytes(granted))
	}

	pub fn resize_lease(&self, node: FlowNodeId, grant: ByteSize) {
		let mut leases = self.0.leases.lock();
		if let Some(lease) = leases.get_mut(&node) {
			lease.grant = grant.as_bytes().max(LEASE_FLOOR.as_bytes());
			Self::recompute_leased(&self.0, &leases);
		}
	}

	pub fn resize_lease_to_demand(&self, node: FlowNodeId, demand: ByteSize) {
		let mut leases = self.0.leases.lock();
		let snapshot = self.snapshot();
		if let Some(lease) = leases.get_mut(&node) {
			let used = snapshot.total().as_bytes().saturating_sub(lease.charged());
			let headroom = snapshot.budget.as_bytes().saturating_sub(used);
			lease.grant = demand.as_bytes().min(headroom).max(LEASE_FLOOR.as_bytes());
			Self::recompute_leased(&self.0, &leases);
		}
	}

	pub fn report_lease(&self, node: FlowNodeId, report: LeaseReport) {
		let mut leases = self.0.leases.lock();
		if let Some(lease) = leases.get_mut(&node) {
			lease.reported = Some(report.total_bytes().as_bytes());
			Self::recompute_leased(&self.0, &leases);
		}
	}

	pub fn report_lease_none(&self, node: FlowNodeId) {
		let mut leases = self.0.leases.lock();
		if let Some(lease) = leases.get_mut(&node) {
			lease.reported = None;
			Self::recompute_leased(&self.0, &leases);
		}
	}

	pub fn silent_leases(&self) -> Count {
		let leases = self.0.leases.lock();
		Count::new(leases.values().filter(|lease| lease.reported.is_none()).count() as u64)
	}

	pub fn release_lease(&self, node: FlowNodeId) {
		let mut leases = self.0.leases.lock();
		leases.remove(&node);
		Self::recompute_leased(&self.0, &leases);
	}

	pub fn current_lease(&self, node: FlowNodeId) -> Option<OperatorLease> {
		let leases = self.0.leases.lock();
		leases.get(&node).map(|lease| OperatorLease {
			node,
			grant: LeaseGrant(ByteSize::from_bytes(lease.grant)),
			last: LeaseReport {
				state: StateMemory::new(
					Count::new(0),
					ByteSize::from_bytes(lease.reported.unwrap_or(0)),
				),
				row_numbers: StateMemory::default(),
			},
			health: match lease.reported {
				Some(_) => LeaseHealth::Reporting,
				None => LeaseHealth::Silent,
			},
		})
	}

	fn recompute_leased(budget: &OperatorStateBudget, leases: &HashMap<FlowNodeId, LeaseState>) {
		let total: u64 = leases.values().map(|l| l.charged()).sum();
		budget.leased.store(total, Ordering::Relaxed);
	}
}

fn release(counter: &AtomicU64, bytes: u64) {
	reifydb_assertions! {
		assert!(
			counter.load(Ordering::Relaxed) >= bytes,
			"state budget counter released below zero: held={} released={}",
			counter.load(Ordering::Relaxed),
			bytes
		);
	}
	let mut current = counter.load(Ordering::Relaxed);
	loop {
		let next = current.saturating_sub(bytes);
		match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
			Ok(_) => return,
			Err(observed) => current = observed,
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::{byte_size::ByteSize, count::Count};

	use super::{LEASE_FLOOR, LeaseHealth, LeaseReport, OperatorStateBudgetHandle};
	use crate::{interface::catalog::flow::FlowNodeId, metrics::heap::StateMemory};

	fn mb(n: u64) -> ByteSize {
		ByteSize::from_bytes(n * 1024 * 1024)
	}

	#[test]
	fn test_charge_release_symmetry() {
		// The pool is the single accounting authority: every charge
		// must be exactly reversible, and totals must decompose into
		// the class counters with nothing lost.
		let pool = OperatorStateBudgetHandle::new(mb(100));
		pool.charge_clean(mb(10));
		pool.charge_dirty(mb(5));
		pool.charge_in_flight(mb(2));

		let snapshot = pool.snapshot();
		assert_eq!(snapshot.resident, mb(10));
		assert_eq!(snapshot.dirty, mb(5));
		assert_eq!(snapshot.in_flight, mb(2));
		assert_eq!(snapshot.total(), mb(17));
		assert_eq!(snapshot.overage(), ByteSize::ZERO);

		pool.release_clean(mb(10));
		pool.release_dirty(mb(5));
		pool.release_in_flight(mb(2));
		assert_eq!(pool.snapshot().total(), ByteSize::ZERO);
	}

	#[test]
	fn test_overage_is_derived_not_stored() {
		let pool = OperatorStateBudgetHandle::new(mb(10));
		pool.charge_dirty(mb(25));
		assert_eq!(pool.snapshot().overage(), mb(15));
		assert!(pool.over_budget());
		pool.release_dirty(mb(20));
		assert_eq!(pool.snapshot().overage(), ByteSize::ZERO);
		assert!(!pool.over_budget());
	}

	#[test]
	fn test_lease_charges_max_of_grant_and_reported() {
		// A lease violator's excess bytes must count into the bound;
		// charging only the grant would hide real memory exactly the
		// way dark_bytes hides it today.
		let pool = OperatorStateBudgetHandle::new(mb(100));
		let node = FlowNodeId(7);
		let grant = pool.grant_lease(node, mb(20));
		assert_eq!(grant.bytes(), mb(20));
		assert_eq!(pool.snapshot().leased, mb(20));

		pool.report_lease(
			node,
			LeaseReport {
				state: StateMemory::new(Count::new(10), mb(50)),
				row_numbers: StateMemory::new(Count::new(1), mb(5)),
			},
		);
		assert_eq!(pool.snapshot().leased, mb(55));

		pool.report_lease(
			node,
			LeaseReport {
				state: StateMemory::new(Count::new(10), mb(1)),
				row_numbers: StateMemory::default(),
			},
		);
		assert_eq!(pool.snapshot().leased, mb(20), "reported below grant charges the grant");

		pool.release_lease(node);
		assert_eq!(pool.snapshot().leased, ByteSize::ZERO);
	}

	#[test]
	fn test_grant_clamps_to_headroom_but_never_below_floor() {
		let pool = OperatorStateBudgetHandle::new(mb(100));
		pool.charge_clean(mb(95));
		let grant = pool.grant_lease(FlowNodeId(1), mb(64));
		assert_eq!(grant.bytes(), LEASE_FLOOR, "5 MiB headroom is below the floor, so the floor wins");

		let pool = OperatorStateBudgetHandle::new(mb(100));
		pool.charge_clean(mb(50));
		let grant = pool.grant_lease(FlowNodeId(2), mb(64));
		assert_eq!(grant.bytes(), mb(50), "grant clamps to available headroom");
	}

	#[test]
	fn test_silence_is_a_state_not_a_fault() {
		// An operator that declines to sample is legitimate, not broken:
		// it reports Silent forever without escalating, and it stays
		// charged at its full grant so silence can never under-account.
		let pool = OperatorStateBudgetHandle::new(mb(100));
		let node = FlowNodeId(3);
		pool.grant_lease(node, mb(16));

		assert_eq!(
			pool.current_lease(node).unwrap().health,
			LeaseHealth::Silent,
			"a lease that has not yet reported is silent"
		);

		for _ in 0..10 {
			pool.report_lease_none(node);
		}
		assert_eq!(pool.current_lease(node).unwrap().health, LeaseHealth::Silent);
		assert_eq!(pool.snapshot().leased, mb(16), "silence charges the full grant");
		assert_eq!(pool.silent_leases(), Count::new(1));
	}

	#[test]
	fn test_silence_and_reporting_are_reversible() {
		// Health tracks the latest sample only. An operator that reports
		// once and then goes quiet must fall back to Silent, otherwise a
		// stale byte count would keep being charged as if it were fresh.
		let pool = OperatorStateBudgetHandle::new(mb(100));
		let node = FlowNodeId(5);
		pool.grant_lease(node, mb(16));

		pool.report_lease(
			node,
			LeaseReport {
				state: StateMemory::new(Count::new(1), mb(40)),
				row_numbers: StateMemory::default(),
			},
		);
		assert_eq!(pool.current_lease(node).unwrap().health, LeaseHealth::Reporting);
		assert_eq!(pool.snapshot().leased, mb(40));
		assert_eq!(pool.silent_leases(), Count::new(0));

		pool.report_lease_none(node);
		assert_eq!(pool.current_lease(node).unwrap().health, LeaseHealth::Silent);
		assert_eq!(pool.snapshot().leased, mb(16), "a stale report is dropped, not carried forward");
	}

	#[test]
	fn test_resize_respects_floor() {
		let pool = OperatorStateBudgetHandle::new(mb(100));
		let node = FlowNodeId(4);
		pool.grant_lease(node, mb(64));
		pool.resize_lease(node, mb(1));
		assert_eq!(pool.current_lease(node).unwrap().grant.bytes(), LEASE_FLOOR);
	}

	#[test]
	fn test_resize_lease_to_demand_follows_demand_within_headroom() {
		// Demand-driven leases (decision D1): a grant must track the
		// operator's reported demand so busy guests grow and idle
		// guests shrink, instead of pinning the creation-time grant
		// forever.
		let pool = OperatorStateBudgetHandle::new(mb(100));
		let node = FlowNodeId(1);
		pool.grant_lease(node, mb(10));

		pool.resize_lease_to_demand(node, mb(25));
		assert_eq!(pool.current_lease(node).unwrap().grant.bytes(), mb(25));

		pool.resize_lease_to_demand(node, mb(12));
		assert_eq!(pool.current_lease(node).unwrap().grant.bytes(), mb(12));
	}

	#[test]
	fn test_resize_lease_to_demand_clamps_to_available_headroom() {
		// FCFS headroom (decision D4): a demand resize may only grow
		// into budget that is actually free; granting past headroom
		// would push the shared pool over budget on behalf of a single
		// operator.
		let pool = OperatorStateBudgetHandle::new(mb(100));
		let first = FlowNodeId(1);
		let second = FlowNodeId(2);
		pool.grant_lease(first, mb(80));
		pool.grant_lease(second, mb(10));

		pool.resize_lease_to_demand(second, mb(50));

		assert_eq!(pool.current_lease(second).unwrap().grant.bytes(), mb(20));
	}

	#[test]
	fn test_resize_lease_to_demand_excludes_own_charge_from_headroom() {
		// The operator's own current charge must not count against its
		// own headroom, otherwise a fully granted pool could never
		// regrow any lease and demand could only ratchet downward.
		let pool = OperatorStateBudgetHandle::new(mb(100));
		let node = FlowNodeId(1);
		pool.grant_lease(node, mb(100));

		pool.resize_lease_to_demand(node, mb(90));

		assert_eq!(pool.current_lease(node).unwrap().grant.bytes(), mb(90));
	}

	#[test]
	fn test_resize_lease_to_demand_never_drops_below_floor() {
		// An idle operator keeps the lease floor so it can restart its
		// cache without renegotiating a grant from zero.
		let pool = OperatorStateBudgetHandle::new(mb(100));
		let node = FlowNodeId(1);
		pool.grant_lease(node, mb(64));

		pool.resize_lease_to_demand(node, ByteSize::ZERO);

		assert_eq!(pool.current_lease(node).unwrap().grant.bytes(), LEASE_FLOOR);
	}

	#[test]
	fn test_resize_lease_to_demand_ignores_released_node() {
		// Operator teardown races the sampling tick; a demand resize
		// arriving after release_lease must not resurrect the lease.
		let pool = OperatorStateBudgetHandle::new(mb(100));
		let node = FlowNodeId(9);
		pool.grant_lease(node, mb(10));
		pool.release_lease(node);

		pool.resize_lease_to_demand(node, mb(10));

		assert!(pool.current_lease(node).is_none());
		assert_eq!(pool.snapshot().leased, ByteSize::ZERO);
	}
}
