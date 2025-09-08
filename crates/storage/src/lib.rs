// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

pub(crate) mod cdc;
mod diagnostic;
pub mod memory;
pub mod sqlite;

use reifydb_core::interface::version::{
	ComponentKind, HasVersion, SystemVersion,
};
pub use reifydb_type::Result;

pub struct StorageVersion;

impl HasVersion for StorageVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "storage".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Storage layer and persistence module"
				.to_string(),
			kind: ComponentKind::Module,
		}
	}
}
