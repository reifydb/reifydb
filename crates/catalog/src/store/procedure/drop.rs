// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::id::ProcedureId,
	key::{
		any::AnyKey,
		namespace::NamespaceProcedureKey,
		procedure::{ProcedureKey, ProcedureParamKey},
	},
};
use reifydb_transaction::{
	multi::RangeScope,
	transaction::{Transaction, admin::AdminTransaction},
};

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn drop_procedure(txn: &mut AdminTransaction, procedure: ProcedureId) -> Result<()> {
		if let Some(p) = Self::find_procedure(&mut Transaction::Admin(&mut *txn), procedure)? {
			txn.remove(&NamespaceProcedureKey::new(p.namespace(), procedure))?;
		}

		let mut param_keys: Vec<ProcedureParamKey> = Vec::new();
		{
			let stream =
				txn.range(ProcedureParamKey::full_scan(procedure).encode(), RangeScope::All, 1024)?;
			for entry in stream {
				let entry = entry?;
				if let AnyKey::ProcedureParam(k) = entry.key {
					param_keys.push(k);
				}
			}
		}
		for key in param_keys {
			txn.remove(&key)?;
		}

		txn.remove(&ProcedureKey::new(procedure))?;

		Ok(())
	}
}
