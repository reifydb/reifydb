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

use crate::{interface::catalog::flow::FlowNodeId, metrics::heap::StateMemory};

pub const DEFAULT_OPERATOR_STATE_BUDGET: ByteSize = ByteSize::from_bytes(2 * 1024 * 1024 * 1024);

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

#[derive(Clone, Copy)]
enum Reported {
	Never,
	Cacheless,
	Bytes(ByteSize),
}

struct LeaseState {
	grant: ByteSize,
	reported: Reported,
}

impl LeaseState {
	fn charged(&self) -> ByteSize {
		match self.reported {
			Reported::Never => self.grant,
			Reported::Cacheless => ByteSize::ZERO,
			Reported::Bytes(bytes) => self.grant.max(bytes),
		}
	}

	fn reported_bytes(&self) -> ByteSize {
		match self.reported {
			Reported::Bytes(bytes) => bytes,
			Reported::Never | Reported::Cacheless => ByteSize::ZERO,
		}
	}

	fn health(&self) -> LeaseHealth {
		match self.reported {
			Reported::Bytes(_) => LeaseHealth::Reporting,
			Reported::Never | Reported::Cacheless => LeaseHealth::Silent,
		}
	}

	fn is_reporting(&self) -> bool {
		matches!(self.reported, Reported::Bytes(_))
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
		let used = snapshot.total().saturating_sub(leases.get(&node).map_or(ByteSize::ZERO, |l| l.charged()));
		let headroom = snapshot.budget.saturating_sub(used);
		let granted = requested.min(headroom).max(LEASE_FLOOR);
		leases.insert(
			node,
			LeaseState {
				grant: granted,
				reported: Reported::Never,
			},
		);
		Self::recompute_leased(&self.0, &leases);
		LeaseGrant(granted)
	}

	pub fn resize_lease(&self, node: FlowNodeId, grant: ByteSize) {
		let mut leases = self.0.leases.lock();
		if let Some(lease) = leases.get_mut(&node) {
			lease.grant = grant.max(LEASE_FLOOR);
			Self::recompute_leased(&self.0, &leases);
		}
	}

	pub fn resize_lease_to_demand(&self, node: FlowNodeId, demand: ByteSize) {
		let mut leases = self.0.leases.lock();
		let snapshot = self.snapshot();
		if let Some(lease) = leases.get_mut(&node) {
			let used = snapshot.total().saturating_sub(lease.charged());
			let headroom = snapshot.budget.saturating_sub(used);
			lease.grant = demand.min(headroom).max(LEASE_FLOOR);
			Self::recompute_leased(&self.0, &leases);
		}
	}

	pub fn report_lease(&self, node: FlowNodeId, report: LeaseReport) {
		let mut leases = self.0.leases.lock();
		if let Some(lease) = leases.get_mut(&node) {
			lease.reported = Reported::Bytes(report.total_bytes());
			Self::recompute_leased(&self.0, &leases);
		}
	}

	pub fn report_lease_none(&self, node: FlowNodeId) {
		let mut leases = self.0.leases.lock();
		if let Some(lease) = leases.get_mut(&node) {
			lease.reported = Reported::Cacheless;
			Self::recompute_leased(&self.0, &leases);
		}
	}

	pub fn silent_leases(&self) -> Count {
		let leases = self.0.leases.lock();
		Count::new(leases.values().filter(|lease| !lease.is_reporting()).count() as u64)
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
			grant: LeaseGrant(lease.grant),
			last: LeaseReport {
				state: StateMemory::new(Count::new(0), lease.reported_bytes()),
				row_numbers: StateMemory::default(),
			},
			health: lease.health(),
		})
	}

	fn recompute_leased(budget: &OperatorStateBudget, leases: &HashMap<FlowNodeId, LeaseState>) {
		let total = leases.values().fold(ByteSize::ZERO, |acc, lease| acc.saturating_add(lease.charged()));
		budget.leased.store(total.as_bytes(), Ordering::Relaxed);
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
		// An operator that declines to sample is legitimate, not broken: it
		// stays Silent forever without escalating. A fresh lease reserves its
		// full grant (cold start, before we know its footprint), but once the
		// operator reports it holds no cache, it charges nothing: a cacheless
		// operator must not pin pool budget it will never use.
		let pool = OperatorStateBudgetHandle::new(mb(100));
		let node = FlowNodeId(3);
		pool.grant_lease(node, mb(16));

		assert_eq!(
			pool.current_lease(node).unwrap().health,
			LeaseHealth::Silent,
			"a lease that has not yet reported is silent"
		);
		assert_eq!(pool.snapshot().leased, mb(16), "a fresh lease reserves its cold-start grant");

		for _ in 0..10 {
			pool.report_lease_none(node);
		}
		assert_eq!(pool.current_lease(node).unwrap().health, LeaseHealth::Silent);
		assert_eq!(pool.snapshot().leased, ByteSize::ZERO, "a cacheless operator charges nothing");
		assert_eq!(pool.silent_leases(), Count::new(1));
	}

	#[test]
	fn test_silence_and_reporting_are_reversible() {
		// Health tracks the latest sample only. An operator that reports once
		// and then reports it holds no cache must fall back to Silent and
		// release its charge: the stale byte count is dropped, and a cacheless
		// report reserves nothing rather than falling back to the grant.
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
		assert_eq!(
			pool.snapshot().leased,
			ByteSize::ZERO,
			"a stale report is dropped and a cacheless report reserves nothing"
		);
	}

	#[test]
	fn test_cacheless_report_frees_the_grant_while_fresh_lease_holds_it() {
		// The flat-operator waste fix. A KeyedStateful/SingleStateful operator
		// runs no state cache, so at every flush it reports no usage. That
		// report must release its share of the pool. A freshly granted lease
		// that has not yet spoken is different: it still reserves its cold-start
		// grant, because we do not yet know whether it will fill a cache. Without
		// distinguishing the two, every cacheless operator would pin its grant
		// forever for memory it never holds.
		let pool = OperatorStateBudgetHandle::new(mb(100));
		let fresh = FlowNodeId(1);
		let cacheless = FlowNodeId(2);

		pool.grant_lease(fresh, mb(16));
		pool.grant_lease(cacheless, mb(16));
		assert_eq!(pool.snapshot().leased, mb(32), "two fresh leases each reserve their grant");

		pool.report_lease_none(cacheless);
		assert_eq!(
			pool.snapshot().leased,
			mb(16),
			"the cacheless operator frees its grant; only the still-fresh lease reserves"
		);
		assert_eq!(
			pool.current_lease(cacheless).unwrap().health,
			LeaseHealth::Silent,
			"a cacheless operator is silent, not faulted"
		);
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
