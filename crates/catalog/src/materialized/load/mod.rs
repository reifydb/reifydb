// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod dictionary;
pub mod flow;
pub mod namespace;
pub mod operator_retention_policy;
pub mod policy;
pub mod primary_key;
pub mod primitive_retention_policy;
pub mod ringbuffer;
pub mod role;
pub mod subscription;
pub mod sumtype;
pub mod table;
pub mod user;
pub mod user_role;
pub mod view;

use dictionary::load_dictionaries;
use flow::load_flows;
use namespace::load_namespaces;
use operator_retention_policy::load_operator_retention_policies;
use policy::load_security_policies;
use primary_key::load_primary_keys;
use primitive_retention_policy::load_source_retention_policies;
use reifydb_transaction::transaction::Transaction;
use ringbuffer::load_ringbuffers;
use role::load_roles;
use subscription::load_subscriptions;
use sumtype::load_sumtypes;
use table::load_tables;
use user::load_users;
use user_role::load_user_roles;
use view::load_views;

use super::MaterializedCatalog;

/// Loads catalog data from storage and populates a MaterializedCatalog
pub struct MaterializedCatalogLoader;

impl MaterializedCatalogLoader {
	/// Load all catalog data from storage into the MaterializedCatalog
	pub fn load_all(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> crate::Result<()> {
		load_namespaces(rx, catalog)?;
		// Load primary keys first so they're available when loading
		// tables/views
		load_primary_keys(rx, catalog)?;

		load_tables(rx, catalog)?;
		load_views(rx, catalog)?;
		load_flows(rx, catalog)?;
		load_ringbuffers(rx, catalog)?;

		// Load retention policies
		load_source_retention_policies(rx, catalog)?;
		load_operator_retention_policies(rx, catalog)?;

		// Load dictionaries and sumtypes
		load_dictionaries(rx, catalog)?;
		load_sumtypes(rx, catalog)?;

		// Load subscriptions
		load_subscriptions(rx, catalog)?;

		// Load auth entities
		load_users(rx, catalog)?;
		load_roles(rx, catalog)?;
		load_user_roles(rx, catalog)?;
		load_security_policies(rx, catalog)?;

		Ok(())
	}
}
