// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb_core::interface::{
	catalog::{id::SubscriptionId, subscription::SubscriptionDef},
	version::{ComponentType, HasVersion, SystemVersion},
};
use reifydb_transaction::standard::IntoStandardTransaction;

pub mod catalog;
pub mod materialized;
pub mod schema;
pub(crate) mod store;
pub mod system;
pub mod test_utils;
pub mod vtable;
/// Result type alias for this crate
pub type Result<T> = reifydb_type::Result<T>;

pub(crate) struct CatalogStore;

/// Find a subscription by ID directly from storage.
///
/// This is a low-level function that bypasses the MaterializedCatalog cache.
/// For most use cases, prefer using `Catalog::find_subscription` instead.
pub fn find_subscription(
	txn: &mut impl IntoStandardTransaction,
	id: SubscriptionId,
) -> Result<Option<SubscriptionDef>> {
	CatalogStore::find_subscription(txn, id)
}

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
