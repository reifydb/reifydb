// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};
pub use reifydb_type::Result;

pub mod backend;
pub(crate) mod cdc;
pub mod config;
mod multi;
mod single;
mod store;

pub use config::{BackendConfig, MergeConfig, RetentionConfig, TransactionStoreConfig};
pub use multi::*;
pub use single::*;
pub use store::StandardTransactionStore;

pub mod memory {
	pub use crate::backend::memory::MemoryBackend;
}
pub mod sqlite {
	pub use crate::backend::sqlite::{SqliteBackend, SqliteConfig};
}

pub struct TransactionStoreVersion;

impl HasVersion for TransactionStoreVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "store-transaction".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Transaction storage for OLTP operations and recent data".to_string(),
			r#type: ComponentType::Module,
		}
	}
}
