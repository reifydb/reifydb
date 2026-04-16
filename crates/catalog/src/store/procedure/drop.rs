// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::id::ProcedureId,
	key::{
		EncodableKey, Key, namespace_procedure::NamespaceProcedureKey, procedure::ProcedureKey,
		procedure_param::ProcedureParamKey,
	},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn drop_procedure(txn: &mut AdminTransaction, procedure: ProcedureId) -> Result<()> {
		// Look up namespace via procedure row to delete the secondary index entry.
		if let Some(p) = Self::find_procedure(&mut Transaction::Admin(&mut *txn), procedure)? {
			txn.remove(&NamespaceProcedureKey::encoded(p.namespace(), procedure))?;
		}

		// Collect all param keys for this procedure and remove them.
		let mut param_keys: Vec<ProcedureParamKey> = Vec::new();
		{
			let stream = txn.range(ProcedureParamKey::full_scan(procedure), 1024)?;
			for entry in stream {
				let entry = entry?;
				if let Some(Key::ProcedureParam(k)) = Key::decode(&entry.key) {
					param_keys.push(k);
				}
			}
		}
		for key in param_keys {
			txn.remove(&key.encode())?;
		}

		// Remove the procedure row itself.
		txn.remove(&ProcedureKey::encoded(procedure))?;

		Ok(())
	}
}
