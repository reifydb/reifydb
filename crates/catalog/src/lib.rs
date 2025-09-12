// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

pub use reifydb_core::Result;
use reifydb_core::interface::version::{
	ComponentType, HasVersion, SystemVersion,
};

pub mod column;
pub mod column_policy;
mod materialized;
pub mod namespace;
pub mod primary_key;
pub mod sequence;
pub mod source;
pub mod system;
pub mod table;
pub mod table_virtual;
pub mod test_utils;
pub mod transaction;
pub mod view;

pub use materialized::{MaterializedCatalog, load::MaterializedCatalogLoader};
pub use transaction::{
	CatalogCommandTransaction, CatalogNamespaceCommandOperations,
	CatalogNamespaceQueryOperations, CatalogQueryTransaction,
	CatalogSourceQueryOperations, CatalogTableCommandOperations,
	CatalogTableQueryOperations, CatalogTrackChangeOperations,
	CatalogViewCommandOperations, CatalogViewQueryOperations,
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
