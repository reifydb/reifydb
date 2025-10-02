// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

pub mod memory;
pub mod partition;
pub mod statistics;

pub use memory::MemoryColumnStore;
pub use partition::{Partition, PartitionKey};
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
