// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::key::{Key, procedure::ProcedureKey};
use reifydb_transaction::transaction::Transaction;

use super::MaterializedCatalog;
use crate::{CatalogStore, Result};

pub(crate) fn load_procedures(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> Result<()> {
	// Collect procedure ids + commit versions in a first pass so the storage stream is
	// dropped before we fan out to per-procedure reads (which need to re-borrow rx).
	let mut entries = Vec::new();
	{
		let stream = rx.range(ProcedureKey::full_scan(), 1024)?;
		for entry in stream {
			let entry = entry?;
			let version = entry.version;
			if let Some(Key::Procedure(k)) = Key::decode(&entry.key) {
				entries.push((k.procedure, version));
			}
		}
	}

	for (id, version) in entries {
		if let Some(procedure) = CatalogStore::find_procedure(rx, id)? {
			catalog.set_procedure(id, version, Some(procedure));
		}
	}

	Ok(())
}
