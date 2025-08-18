// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	hook::Hooks,
	interceptor::Interceptors,
	interface::{
		CommandTransaction, StandardCdcTransaction, StandardTransaction,
	},
};
use reifydb_storage::memory::Memory;

use crate::{
	mvcc::transaction::serializable::Serializable, svl::SingleVersionLock,
};

pub fn create_test_command_transaction() -> CommandTransaction<
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
	CommandTransaction::new(
		Serializable::new(memory, unversioned.clone(), hooks.clone())
			.begin_command()
			.unwrap(),
		unversioned,
		cdc,
		hooks,
		Interceptors::new(),
	)
}
