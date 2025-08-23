// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::transaction::StandardCdcTransaction;
use crate::{StandardCommandTransaction, StandardTransaction};
use reifydb_core::catalog::MaterializedCatalog;
use reifydb_core::{hook::Hooks, interceptor::Interceptors};
use reifydb_storage::memory::Memory;
use reifydb_transaction::mvcc::transaction::serializable::Serializable;
use reifydb_transaction::svl::SingleVersionLock;

pub fn create_test_command_transaction() -> StandardCommandTransaction<
	StandardTransaction<
		Serializable<Memory, SingleVersionLock<Memory>>,
		SingleVersionLock<Memory>,
		StandardCdcTransaction<Memory>,
	>,
> {
	let memory = Memory::new();
	let hooks = Hooks::new();
	let unversioned = SingleVersionLock::new(memory.clone(), hooks.clone());
	let cdc = StandardCdcTransaction::new(memory.clone());
	StandardCommandTransaction::new(
		Serializable::new(memory, unversioned.clone(), hooks.clone())
			.begin_command()
			.unwrap(),
		unversioned,
		cdc,
		hooks,
		Interceptors::new(),
		MaterializedCatalog::new(),
	)
}
