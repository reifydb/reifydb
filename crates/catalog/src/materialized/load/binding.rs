// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::key::{EncodableKey, binding::BindingKey};
use reifydb_transaction::transaction::Transaction;

use crate::{Result, materialized::MaterializedCatalog, store::binding::find::decode_binding};

pub(crate) fn load_bindings(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> Result<()> {
	let stream = rx.range(BindingKey::full_scan(), 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;
		if let Some(k) = BindingKey::decode(&multi.key) {
			let binding = decode_binding(&multi.row);
			catalog.set_binding(k.binding, version, Some(binding));
		}
	}

	Ok(())
}
