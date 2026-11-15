// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::key::{any::TaggedKey, catalog::BindingKey};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::error::ValueError;

use crate::{cache::CatalogCache, store::binding::find::decode_binding};

pub(crate) fn load_bindings(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<(), ValueError> {
	let stream = rx.range(BindingKey::full_scan(), RangeScope::All, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;
		if let TaggedKey::Binding(k) = &multi.key {
			let binding = decode_binding(EncodedCatalogRow::view(&multi.bytes));
			catalog.set_binding(k.binding, version, Some(binding));
		}
	}

	Ok(())
}
