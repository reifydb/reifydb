// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::mvcc::transaction::serializable::Serializable;
use crate::svl::SingleVersionLock;
use reifydb_core::hook::Hooks;
use reifydb_core::interface::ActiveWriteTransaction;
use reifydb_storage::memory::Memory;

pub fn create_test_write_transaction() -> ActiveWriteTransaction<
    Serializable<Memory, SingleVersionLock<Memory>>,
    SingleVersionLock<Memory>,
> {
    let memory = Memory::new();
    let hooks = Hooks::new();
    let unversioned = SingleVersionLock::new(memory.clone(), hooks.clone());
    ActiveWriteTransaction::new(
        Serializable::new(memory, unversioned.clone(), hooks).begin_write().unwrap(),
        unversioned,
    )
}
