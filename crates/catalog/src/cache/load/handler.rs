// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::key::{Key, handler::HandlerKey};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use super::CatalogCache;
use crate::{CatalogStore, Result};

pub(crate) fn load_handlers(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let mut entries = Vec::new();
	{
		let stream = rx.range(HandlerKey::full_scan(), RangeScope::All, 1024)?;
		for entry in stream {
			let entry = entry?;
			let version = entry.version;
			if let Some(k) = HandlerKey::decode(&entry.key) {
				entries.push((k.handler, version));
			}
		}
	}

	for (id, version) in entries {
		if let Some(handler) = CatalogStore::find_handler(rx, id)? {
			catalog.set_handler(id, version, Some(handler));
		}
	}

	Ok(())
}

#[cfg(test)]
mod tests {
	use reifydb_core::common::CommitVersion;
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_value::value::sumtype::{SumTypeId, VariantRef};

	use super::*;
	use crate::{
		cache::load::CatalogCacheLoader,
		test_utils::{create_handler, create_namespace, ensure_test_namespace},
	};

	#[test]
	fn handlers_in_the_store_are_rebuilt_into_a_fresh_cache() {
		// set_handler is the only writer of the handler indexes, so a boot loader that skips
		// handlers leaves them gone for the life of the process. Driving load_all rather than
		// load_handlers is the point: the wiring is what must be covered.
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);
		create_namespace(&mut txn, "namespace_one");

		let variant = VariantRef {
			sumtype_id: SumTypeId(0),
			variant_tag: 0,
		};
		let created = create_handler(&mut txn, "namespace_one", "on_order_placed", variant, "");

		let catalog = CatalogCache::new();
		CatalogCacheLoader::load_all(&mut Transaction::Admin(&mut txn), &catalog).unwrap();

		let indexed = catalog.list_handlers_for_variant_at(variant, CommitVersion(u64::MAX));
		assert_eq!(
			indexed.len(),
			1,
			"a handler committed to the store was not rebuilt into the cache at boot, so it will never \
			 fire again for the life of the process"
		);
		assert_eq!(indexed[0].id, created.id);
		assert_eq!(indexed[0].name, "on_order_placed");

		assert_eq!(
			catalog.find_handler_by_name_at(created.namespace, "on_order_placed", CommitVersion(u64::MAX))
				.map(|h| h.id),
			Some(created.id),
			"the name index must be rebuilt too, not just the variant index"
		);
	}
}
