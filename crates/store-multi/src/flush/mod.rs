// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod engine;

use reifydb_core::interface::catalog::storage::StorageId;

pub trait ObjectPersistence: Send + Sync + 'static {
	fn is_persistent(&self, storage: StorageId) -> bool;
}
