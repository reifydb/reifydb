// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::key::namespace::NamespaceKey;
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::error::ValueError;

use super::CatalogCache;
use crate::store::namespace;

pub(crate) fn load_namespaces(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<(), ValueError> {
	let range = NamespaceKey::full_scan();
	let stream = rx.range(range, RangeScope::All, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;
		let namespace = namespace::convert_namespace(multi)?;
		catalog.set_namespace(namespace.id(), version, Some(namespace));
	}

	Ok(())
}
