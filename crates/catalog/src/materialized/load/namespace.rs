// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::key::namespace::NamespaceKey;
use reifydb_transaction::transaction::Transaction;

use super::MaterializedCatalog;
use crate::store::namespace;

/// Load all namespaces from storage
pub(crate) fn load_namespaces(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> crate::Result<()> {
	let range = NamespaceKey::full_scan();
	let mut stream = rx.range(range, 1024)?;

	while let Some(entry) = stream.next() {
		let multi = entry?;
		let version = multi.version;
		let namespace_def = namespace::convert_namespace(multi);
		catalog.set_namespace(namespace_def.id, version, Some(namespace_def));
	}

	Ok(())
}
