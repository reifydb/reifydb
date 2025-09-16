// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	event::EventBus,
	interface::{CdcTransaction, UnversionedTransaction, VersionedStorage},
};
use reifydb_engine::StandardCdcTransaction;
use reifydb_storage::{
	memory::Memory,
	sqlite::{Sqlite, SqliteConfig},
};
use reifydb_transaction::{
	mvcc::transaction::{optimistic::Optimistic, serializable::Serializable},
	svl::SingleVersionLock,
};

pub mod embedded;

#[cfg(feature = "sub_server")]
pub mod server;

/// Convenience function to create in-memory storage
pub fn memory() -> (Memory, SingleVersionLock<Memory>, StandardCdcTransaction<Memory>, EventBus) {
	let eventbus = EventBus::new();
	let memory = Memory::default();
	(
		memory.clone(),
		SingleVersionLock::new(Memory::new(), eventbus.clone()),
		StandardCdcTransaction::new(memory),
		eventbus,
	)
}

/// Convenience function to create SQLite storage
pub fn sqlite(config: SqliteConfig) -> (Sqlite, SingleVersionLock<Sqlite>, StandardCdcTransaction<Sqlite>, EventBus) {
	let eventbus = EventBus::new();
	let result = Sqlite::new(config);
	(
		result.clone(),
		SingleVersionLock::new(result.clone(), eventbus.clone()),
		StandardCdcTransaction::new(result),
		eventbus,
	)
}

/// Convenience function to create an optimistic transaction layer
pub fn optimistic<VS, UT, C>(input: (VS, UT, C, EventBus)) -> (Optimistic<VS, UT>, UT, C, EventBus)
where
	VS: VersionedStorage,
	UT: UnversionedTransaction,
	C: CdcTransaction,
{
	(Optimistic::new(input.0, input.1.clone(), input.3.clone()), input.1, input.2, input.3)
}

/// Convenience function to create a serializable transaction layer
pub fn serializable<VS, UT, C>(input: (VS, UT, C, EventBus)) -> (Serializable<VS, UT>, UT, C, EventBus)
where
	VS: VersionedStorage,
	UT: UnversionedTransaction,
	C: CdcTransaction,
{
	(Serializable::new(input.0, input.1.clone(), input.3.clone()), input.1, input.2, input.3)
}
