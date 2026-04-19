// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::binding::Binding,
	key::{Key, binding::BindingKey},
};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result};

use super::find::decode_binding;

impl CatalogStore {
	pub(crate) fn list_bindings_all(rx: &mut Transaction<'_>) -> Result<Vec<Binding>> {
		let mut out = Vec::new();
		let stream = rx.range(BindingKey::full_scan(), 1024)?;
		for entry in stream {
			let entry = entry?;
			if let Some(Key::Binding(_)) = Key::decode(&entry.key) {
				out.push(decode_binding(&entry.row));
			}
		}
		Ok(out)
	}
}
