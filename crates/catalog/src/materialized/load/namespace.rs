// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{MultiVersionQueryTransaction, NamespaceKey};

use crate::{MaterializedCatalog, store::namespace};

/// Load all namespaces from storage
pub(crate) async fn load_namespaces(
	tx: &mut impl MultiVersionQueryTransaction,
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
