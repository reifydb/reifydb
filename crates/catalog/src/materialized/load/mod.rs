// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod dictionary;
pub mod flow;
pub mod namespace;
pub mod operator_retention_policy;
pub mod primary_key;
pub mod primitive_retention_policy;
pub mod table;
pub mod view;

pub(crate) use dictionary::load_dictionaries;
pub(crate) use flow::load_flows;
pub(crate) use namespace::load_namespaces;
pub(crate) use operator_retention_policy::load_operator_retention_policies;
pub(crate) use primary_key::load_primary_keys;
pub(crate) use primitive_retention_policy::load_source_retention_policies;
use reifydb_transaction::IntoStandardTransaction;
pub(crate) use table::load_tables;
pub(crate) use view::load_views;

use crate::MaterializedCatalog;

/// Loads catalog data from storage and populates a MaterializedCatalog
pub struct MaterializedCatalogLoader;

impl MaterializedCatalogLoader {
	/// Load all catalog data from storage into the MaterializedCatalog
	pub async fn load_all(
		rx: &mut impl IntoStandardTransaction,
		catalog: &MaterializedCatalog,
	) -> crate::Result<()> {
		let mut txn = rx.into_standard_transaction();
		load_namespaces(&mut txn, catalog).await?;
		// Load primary keys first so they're available when loading
		// tables/views
		load_primary_keys(&mut txn, catalog).await?;

		load_tables(&mut txn, catalog).await?;
		load_views(&mut txn, catalog).await?;
		load_flows(&mut txn, catalog).await?;

		// Load retention policies
		load_source_retention_policies(&mut txn, catalog).await?;
		load_operator_retention_policies(&mut txn, catalog).await?;

		// Load dictionaries
		load_dictionaries(&mut txn, catalog).await?;

		Ok(())
	}
}
