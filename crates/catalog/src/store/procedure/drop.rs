// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::id::ProcedureId,
	key::{
		any::TaggedKey,
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
			txn.remove(vec![(NamespaceProcedureKey::new(p.namespace(), procedure).into(), None)])?;
		}

		let mut param_keys: Vec<ProcedureParamKey> = Vec::new();
		{
			let stream = txn.range(ProcedureParamKey::full_scan(procedure), RangeScope::All, 1024)?;
			for entry in stream {
				let entry = entry?;
				if let TaggedKey::ProcedureParam(k) = entry.key {
					param_keys.push(k);
				}
			}
		}
		for key in param_keys {
			txn.remove(vec![(key.clone().into(), None)])?;
		}

		txn.remove(vec![(ProcedureKey::new(procedure).into(), None)])?;

		Ok(())
	}
}
