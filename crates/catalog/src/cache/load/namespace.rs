// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::key::namespace::NamespaceKey;
use reifydb_transaction::transaction::Transaction;

use super::CatalogCache;
use crate::{Result, store::namespace};

pub(crate) fn load_namespaces(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = NamespaceKey::full_scan();
	let stream = rx.range(range, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;
		let namespace = namespace::convert_namespace(multi);
		catalog.set_namespace(namespace.id(), version, Some(namespace));
	}

	Ok(())
}
