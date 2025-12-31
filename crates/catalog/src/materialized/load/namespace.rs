// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{NamespaceKey, QueryTransaction};

use crate::{MaterializedCatalog, store::namespace};

/// Load all namespaces from storage
pub(crate) async fn load_namespaces(
	tx: &mut impl QueryTransaction,
	catalog: &MaterializedCatalog,
) -> crate::Result<()> {
	let range = NamespaceKey::full_scan();
	let batch = tx.range(range).await?;

	for multi in batch.items {
		let version = multi.version;
		let namespace_def = namespace::convert_namespace(multi);
		catalog.set_namespace(namespace_def.id, version, Some(namespace_def));
	}

	Ok(())
}
