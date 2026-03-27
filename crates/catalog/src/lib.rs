// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb_core::interface::{
	catalog::{
		id::{NamespaceId, SubscriptionId},
		subscription::Subscription,
		token::{Token, TokenId},
	},
	version::{ComponentType, HasVersion, SystemVersion},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::{datetime::DateTime, identity::IdentityId};
pub mod bootstrap;
pub mod catalog;
pub mod error;
pub mod function;
pub mod materialized;
pub mod procedure;
pub mod schema;
pub mod store;
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
pub fn find_subscription(txn: &mut Transaction<'_>, id: SubscriptionId) -> Result<Option<Subscription>> {
	CatalogStore::find_subscription(txn, id)
}

/// Drop a subscription and all its associated data (columns, rows, metadata).
///
/// This is a low-level function that performs complete cleanup of a subscription.
/// Use this when cleaning up subscriptions after a WebSocket connection closes.
pub fn drop_subscription(txn: &mut AdminTransaction, id: SubscriptionId) -> Result<()> {
	CatalogStore::drop_subscription(txn, id)
}

/// Drop a flow by its name within a namespace.
///
/// This is useful for cleaning up flows associated with subscriptions,
/// where the flow name is derived from the subscription ID.
pub fn drop_flow_by_name(txn: &mut AdminTransaction, namespace: NamespaceId, name: &str) -> Result<()> {
	CatalogStore::drop_flow_by_name(txn, namespace, name)
}

/// Create a new token in storage.
pub fn create_token(
	txn: &mut AdminTransaction,
	token: &str,
	identity: IdentityId,
	expires_at: Option<DateTime>,
	created_at: DateTime,
) -> Result<Token> {
	CatalogStore::create_token(txn, token, identity, expires_at, created_at)
}

/// Find a token by its value (constant-time comparison).
pub fn find_token_by_value(txn: &mut Transaction<'_>, value: &str) -> Result<Option<Token>> {
	CatalogStore::find_token_by_value(txn, value)
}

/// Drop a single token by ID.
pub fn drop_token(txn: &mut AdminTransaction, id: TokenId) -> Result<()> {
	CatalogStore::drop_token(txn, id)
}

/// Drop all tokens for a given identity.
pub fn drop_tokens_by_identity(txn: &mut AdminTransaction, identity: IdentityId) -> Result<()> {
	CatalogStore::drop_tokens_by_identity(txn, identity)
}

/// Drop all expired tokens.
pub fn drop_expired_tokens(txn: &mut AdminTransaction, now: DateTime) -> Result<()> {
	CatalogStore::drop_expired_tokens(txn, now)
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
