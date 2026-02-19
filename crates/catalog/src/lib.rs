// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb_core::interface::{
	catalog::{
		id::{NamespaceId, SubscriptionId},
		subscription::SubscriptionDef,
	},
	version::{ComponentType, HasVersion, SystemVersion},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

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
pub fn find_subscription(txn: &mut Transaction<'_>, id: SubscriptionId) -> Result<Option<SubscriptionDef>> {
	CatalogStore::find_subscription(txn, id)
}

/// Delete a subscription and all its associated data (columns, rows, metadata).
///
/// This is a low-level function that performs complete cleanup of a subscription.
/// Use this when cleaning up subscriptions after a WebSocket connection closes.
pub fn delete_subscription(txn: &mut AdminTransaction, id: SubscriptionId) -> Result<()> {
	CatalogStore::delete_subscription(txn, id)
}

/// Delete a flow by its name within a namespace.
///
/// This is useful for cleaning up flows associated with subscriptions,
/// where the flow name is derived from the subscription ID.
pub fn delete_flow_by_name(txn: &mut AdminTransaction, namespace: NamespaceId, name: &str) -> Result<()> {
	CatalogStore::delete_flow_by_name(txn, namespace, name)
}

pub struct CatalogVersion;

impl HasVersion for CatalogVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: env!("CARGO_PKG_NAME")
				.strip_prefix("reifydb-")
				.unwrap_or(env!("CARGO_PKG_NAME"))
				.to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Database catalog and metadata management module".to_string(),
			r#type: ComponentType::Module,
		}
	}
}
