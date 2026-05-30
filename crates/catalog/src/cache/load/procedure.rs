// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::key::{Key, procedure::ProcedureKey};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use super::CatalogCache;
use crate::{CatalogStore, Result};

pub(crate) fn load_procedures(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let mut entries = Vec::new();
	{
		let stream = rx.range(ProcedureKey::full_scan(), RangeScope::All, 1024)?;
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
