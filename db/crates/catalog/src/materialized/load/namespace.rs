// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{NamespaceKey, VersionedQueryTransaction};

use crate::{MaterializedCatalog, namespace};

/// Load all namespaces from storage
pub(crate) fn load_namespaces(
	tx: &mut impl VersionedQueryTransaction,
	catalog: &MaterializedCatalog,
) -> crate::Result<()> {
	let range = NamespaceKey::full_scan();
	let namespaces = tx.range(range)?;

	for versioned in namespaces {
		let version = versioned.version;
		let namespace_def = namespace::convert_namespace(versioned);
		catalog.set_namespace(namespace_def.id, version, Some(namespace_def));
	}

	Ok(())
}
