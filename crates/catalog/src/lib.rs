// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

pub use reifydb_core::Result;
use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};

mod materialized;
pub mod resolve;
pub mod store;
pub mod system;
pub mod table_virtual;
pub mod test_utils;
pub mod transaction;

pub use materialized::{MaterializedCatalog, load::MaterializedCatalogLoader};
// Re-export moved modules for backward compatibility
pub use store::column;
pub use store::{column_policy, namespace, primary_key, primitive, ringbuffer, sequence, table, view};
pub use transaction::{
	CatalogCommandTransaction, CatalogDictionaryCommandOperations, CatalogDictionaryQueryOperations,
	CatalogNamespaceCommandOperations, CatalogNamespaceQueryOperations, CatalogPrimitiveQueryOperations,
	CatalogQueryTransaction, CatalogTableCommandOperations, CatalogTableQueryOperations,
	CatalogTableVirtualUserQueryOperations, CatalogTrackChangeOperations, CatalogViewCommandOperations,
	CatalogViewQueryOperations,
};

pub struct CatalogStore;

pub struct CatalogVersion;

impl HasVersion for CatalogVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "catalog".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Database catalog and metadata management module".to_string(),
			r#type: ComponentType::Module,
		}
	}
}
