// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

pub mod backend;
pub(crate) mod cdc;
pub mod config;
mod store;

pub use backend::{Backend, MultiVersionRowBackend};
pub use config::{BackendConfig, MergeConfig, RetentionConfig, RowStoreConfig};
use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};
pub use reifydb_type::Result;
pub use store::StandardRowStore;

pub mod memory {
	pub use crate::backend::memory::Memory;
}
pub mod sqlite {
	pub use crate::backend::sqlite::{Sqlite, SqliteConfig};
}

pub struct RowStoreVersion;

impl HasVersion for RowStoreVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "store-row".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Row-oriented storage for OLTP operations and recent data".to_string(),
			r#type: ComponentType::Module,
		}
	}
}
