// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

pub mod backend;
pub mod config;
mod memory;
pub mod partition;
pub mod statistics;
mod store;

// New public exports
pub use backend::{Backend, ColumnBackend, MemoryColumnBackend};
pub use config::{BackendConfig, ColumnStoreConfig, CompressionConfig, RetentionConfig};
// Backward compatibility - alias the old MemoryColumnStore to the new backend
pub use memory::MemoryColumnStore;
pub use partition::{Partition, PartitionKey};
pub use store::StandardColumnStore;

// Convenience re-exports for backend modules
pub mod memory_backend {
	pub use crate::backend::memory::MemoryColumnBackend;
}

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};

pub struct StoreColumnVersion;

impl HasVersion for StoreColumnVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "store-column".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Column-oriented storage for analytical queries".to_string(),
			r#type: ComponentType::Module,
		}
	}
}
