// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod dictionary;
pub mod flow;
pub mod namespace;
pub mod operator_retention_policy;
pub mod primary_key;
pub mod primitive_retention_policy;
pub mod ringbuffer;
pub mod subscription;
pub mod sumtype;
pub mod table;
pub mod view;

use dictionary::load_dictionaries;
use flow::load_flows;
use namespace::load_namespaces;
use operator_retention_policy::load_operator_retention_policies;
use primary_key::load_primary_keys;
use primitive_retention_policy::load_source_retention_policies;
use reifydb_transaction::transaction::AsTransaction;
use ringbuffer::load_ringbuffers;
use subscription::load_subscriptions;
use sumtype::load_sumtypes;
use table::load_tables;
use view::load_views;

use super::MaterializedCatalog;

/// Loads catalog data from storage and populates a MaterializedCatalog
pub struct MaterializedCatalogLoader;

impl MaterializedCatalogLoader {
	/// Load all catalog data from storage into the MaterializedCatalog
	pub fn load_all(rx: &mut impl AsTransaction, catalog: &MaterializedCatalog) -> crate::Result<()> {
		let mut txn = rx.as_transaction();
		load_namespaces(&mut txn, catalog)?;
		// Load primary keys first so they're available when loading
		// tables/views
		load_primary_keys(&mut txn, catalog)?;

		load_tables(&mut txn, catalog)?;
		load_views(&mut txn, catalog)?;
		load_flows(&mut txn, catalog)?;
		load_ringbuffers(&mut txn, catalog)?;

		// Load retention policies
		load_source_retention_policies(&mut txn, catalog)?;
		load_operator_retention_policies(&mut txn, catalog)?;

		load_dictionaries(&mut txn, catalog)?;
		load_sumtypes(&mut txn, catalog)?;

		load_subscriptions(&mut txn, catalog)?;

		Ok(())
	}
}
