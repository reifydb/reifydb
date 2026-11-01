// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::namespace::Namespace,
	key::{any::TaggedKey, namespace::NamespaceKey},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use crate::{CatalogStore, Result, store::namespace::convert_namespace};

impl CatalogStore {
	pub(crate) fn list_namespaces_all(rx: &mut Transaction<'_>) -> Result<Vec<Namespace>> {
		let mut result = Vec::new();

		let namespace_range = NamespaceKey::full_scan();

		let stream = rx.range(namespace_range, RangeScope::All, 1024)?;

		for entry in stream {
			let entry = entry?;
			if let TaggedKey::Namespace(_) = &entry.key {
				result.push(convert_namespace(entry)?);
			}
		}

		Ok(result)
	}
}
