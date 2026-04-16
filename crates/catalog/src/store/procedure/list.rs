// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::procedure::Procedure,
	key::{Key, procedure::ProcedureKey},
};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn list_procedures_all(rx: &mut Transaction<'_>) -> Result<Vec<Procedure>> {
		let mut ids = Vec::new();
		{
			let stream = rx.range(ProcedureKey::full_scan(), 1024)?;
			for entry in stream {
				let entry = entry?;
				if let Some(Key::Procedure(k)) = Key::decode(&entry.key) {
					ids.push(k.procedure);
				}
			}
		}
		let mut out = Vec::with_capacity(ids.len());
		for id in ids {
			if let Some(p) = Self::find_procedure(rx, id)? {
				out.push(p);
			}
		}
		Ok(out)
	}
}
