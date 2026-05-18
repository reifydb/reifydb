// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Concrete implementation of the catalog object hierarchy declared by `core::interface::catalog`. Owns the on-disk
//! representation of namespaces, tables, views, flows, identities, policies, sequences, tokens, tests, and the system
//! objects ReifyDB self-hosts; resolves names through the resolved-name machinery; and provides the materialized
//! views that the engine reads to plan and execute queries.
//!
//! Catalog reads ride on a regular transaction; catalog writes go through the admin transaction so DDL and identity
//! mutations are isolated from concurrent OLTP traffic and emit their own change records. Bootstrap installs the
//! system namespace and the seed identities the rest of the system depends on; vtable exposes catalog state to RQL
//! as queryable virtual tables.
//!
//! Invariant: persisted catalog ids (namespace id, table id, identity id, etc.) are stable across reboots; ephemeral
//! per-boot ids exist for in-memory resolution but never round-trip through storage. Mixing the two leaves dangling
//! references when the process restarts.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

use reifydb_core::interface::{
	catalog::{
		id::NamespaceId,
		token::{Token, TokenId},
	},
	version::{ComponentType, HasVersion, SystemVersion},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::{datetime::DateTime, identity::IdentityId};
pub mod bootstrap;
pub mod cache;
pub mod catalog;
pub mod change;
pub mod error;
pub mod interceptor;
pub mod shape;
pub mod store;
pub mod system;
pub mod test_utils;
pub mod vtable;

pub type Result<T> = reifydb_type::Result<T>;

pub(crate) struct CatalogStore;

pub fn drop_flow_by_name(txn: &mut AdminTransaction, namespace: NamespaceId, name: &str) -> Result<()> {
	CatalogStore::drop_flow_by_name(txn, namespace, name)
}

pub fn create_token(
	txn: &mut AdminTransaction,
	token: &str,
	identity: IdentityId,
	expires_at: Option<DateTime>,
	created_at: DateTime,
) -> Result<Token> {
	CatalogStore::create_token(txn, token, identity, expires_at, created_at)
}

pub fn find_token_by_value(txn: &mut Transaction<'_>, value: &str) -> Result<Option<Token>> {
	CatalogStore::find_token_by_value(txn, value)
}

pub fn drop_token(txn: &mut AdminTransaction, id: TokenId) -> Result<()> {
	CatalogStore::drop_token(txn, id)
}

pub fn drop_tokens_by_identity(txn: &mut AdminTransaction, identity: IdentityId) -> Result<()> {
	CatalogStore::drop_tokens_by_identity(txn, identity)
}

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
