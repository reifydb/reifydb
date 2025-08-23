// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::path::Path;

use reifydb_core::interface::CdcTransaction;
use reifydb_engine::StandardCdcTransaction;
use reifydb_core::{
    hook::Hooks,
    interface::{
        UnversionedTransaction, VersionedStorage,
    },
};
use reifydb_storage::{
    lmdb::Lmdb,
    memory::Memory,
    sqlite::{Sqlite, SqliteConfig},
};
use reifydb_transaction::{
    mvcc::transaction::{
        optimistic::Optimistic, serializable::Serializable,
    },
    svl::SingleVersionLock,
};

pub mod sync;

#[cfg(feature = "async")]
pub mod async_;

#[cfg(any(feature = "sub_grpc", feature = "sub_ws"))]
pub mod server;

/// Convenience function to create in-memory storage
pub fn memory()
-> (Memory, SingleVersionLock<Memory>, StandardCdcTransaction<Memory>, Hooks) {
	let hooks = Hooks::new();
	let memory = Memory::default();
	(
		memory.clone(),
		SingleVersionLock::new(Memory::new(), hooks.clone()),
		StandardCdcTransaction::new(memory),
		hooks,
	)
}

/// Convenience function to create LMDB storage
pub fn lmdb(
	path: &Path,
) -> (Lmdb, SingleVersionLock<Lmdb>, StandardCdcTransaction<Lmdb>, Hooks) {
	let hooks = Hooks::new();
	let result = Lmdb::new(path);
	(
		result.clone(),
		SingleVersionLock::new(result.clone(), hooks.clone()),
		StandardCdcTransaction::new(result),
		hooks,
	)
}

/// Convenience function to create SQLite storage
pub fn sqlite(
	config: SqliteConfig,
) -> (Sqlite, SingleVersionLock<Sqlite>, StandardCdcTransaction<Sqlite>, Hooks)
{
	let hooks = Hooks::new();
	let result = Sqlite::new(config);
	(
		result.clone(),
		SingleVersionLock::new(result.clone(), hooks.clone()),
		StandardCdcTransaction::new(result),
		hooks,
	)
}

/// Convenience function to create an optimistic transaction layer
pub fn optimistic<VS, UT, C>(
	input: (VS, UT, C, Hooks),
) -> (Optimistic<VS, UT>, UT, C, Hooks)
where
	VS: VersionedStorage,
	UT: UnversionedTransaction,
	C: CdcTransaction,
{
	(
		Optimistic::new(input.0, input.1.clone(), input.3.clone()),
		input.1,
		input.2,
		input.3,
	)
}

/// Convenience function to create a serializable transaction layer
pub fn serializable<VS, UT, C>(
	input: (VS, UT, C, Hooks),
) -> (Serializable<VS, UT>, UT, C, Hooks)
where
	VS: VersionedStorage,
	UT: UnversionedTransaction,
	C: CdcTransaction,
{
	(
		Serializable::new(input.0, input.1.clone(), input.3.clone()),
		input.1,
		input.2,
		input.3,
	)
}
