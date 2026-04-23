// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{sync::Arc, time::Duration};

use reifydb_catalog::materialized::MaterializedCatalog;
use reifydb_core::{
	common::CommitVersion,
	encoded::{key::EncodedKey, row::EncodedRow},
	event::EventBus,
};
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::{
		clock::{Clock, MockClock},
		rng::Rng,
	},
	pool::Pools,
};
use reifydb_store_multi::MultiStore;
use reifydb_store_single::SingleStore;
use reifydb_transaction::{
	interceptor::interceptors::Interceptors,
	multi::transaction::MultiTransaction,
	single::SingleTransaction,
	transaction::{command::CommandTransaction, query::QueryTransaction},
};
use reifydb_type::{Result, util::cowvec::CowVec, value::identity::IdentityId};

use crate::consume::host::CdcHost;

/// In-memory `CdcHost` for tests. Owns its own `MaterializedCatalog`, `EventBus` and a
/// `Clock::Mock` (cloned `MockClock` accessible via the public `mock` field).
#[derive(Clone)]
pub struct TestCdcHost {
	multi: MultiTransaction,
	single: SingleTransaction,
	pub event_bus: EventBus,
	pub materialized_catalog: MaterializedCatalog,
	pub clock: Clock,
	pub mock: MockClock,
}

impl TestCdcHost {
	/// Build a fresh host with the mock clock initialised to `initial_nanos`.
	pub fn with_clock(initial_nanos: u64) -> Self {
		let multi_store = MultiStore::testing_memory();
		let single_store = SingleStore::testing_memory();
		let actor_system = ActorSystem::new(Pools::default(), Clock::Real);
		let event_bus = EventBus::new(&actor_system);
		let single = SingleTransaction::new(single_store, event_bus.clone());
		let materialized_catalog = MaterializedCatalog::new();
		let mock = MockClock::new(initial_nanos);
		let clock = Clock::Mock(mock.clone());
		let multi = MultiTransaction::new(
			multi_store,
			single.clone(),
			event_bus.clone(),
			actor_system,
			clock.clone(),
			Rng::seeded(42),
			Arc::new(materialized_catalog.clone()),
		)
		.unwrap();
		Self {
			multi,
			single,
			event_bus,
			materialized_catalog,
			clock,
			mock,
		}
	}

	/// Build a host with the mock clock initialized to 1 s past the epoch.
	pub fn new() -> Self {
		Self::with_clock(1_000_000_000)
	}
}

impl Default for TestCdcHost {
	fn default() -> Self {
		Self::new()
	}
}

impl CdcHost for TestCdcHost {
	fn begin_command(&self) -> Result<CommandTransaction> {
		CommandTransaction::new(
			self.multi.clone(),
			self.single.clone(),
			self.event_bus.clone(),
			Interceptors::new(),
			IdentityId::system(),
			self.clock.clone(),
		)
	}

	fn begin_query(&self) -> Result<QueryTransaction> {
		Ok(QueryTransaction::new(self.multi.begin_query()?, self.single.clone(), IdentityId::system()))
	}

	fn current_version(&self) -> Result<CommitVersion> {
		Ok(CommitVersion(1))
	}

	fn done_until(&self) -> CommitVersion {
		CommitVersion(1)
	}

	fn wait_for_mark_timeout(&self, _version: CommitVersion, _timeout: Duration) -> bool {
		true
	}

	fn materialized_catalog(&self) -> &MaterializedCatalog {
		&self.materialized_catalog
	}
}

/// Convenience: build an `EncodedKey` from a string.
pub fn make_key(s: &str) -> EncodedKey {
	EncodedKey(CowVec::new(s.as_bytes().to_vec()))
}

/// Convenience: build an `EncodedRow` from a string.
pub fn make_row(s: &str) -> EncodedRow {
	EncodedRow(CowVec::new(s.as_bytes().to_vec()))
}
