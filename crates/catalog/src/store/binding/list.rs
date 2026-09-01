// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{
	interface::catalog::binding::Binding,
	key::{Key, binding::BindingKey},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use super::find::decode_binding;
use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn list_bindings_all(rx: &mut Transaction<'_>) -> Result<Vec<Binding>> {
		let mut out = Vec::new();
		let stream = rx.range(BindingKey::full_scan(), RangeScope::All, 1024)?;
		for entry in stream {
			let entry = entry?;
			if BindingKey::decode(&entry.key).is_some() {
				out.push(decode_binding(EncodedCatalogRow::view(&entry.bytes)));
			}
		}
		Ok(out)
	}
}
