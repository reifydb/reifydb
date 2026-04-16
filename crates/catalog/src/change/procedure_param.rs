// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	key::{EncodableKey, procedure::ProcedureKey, procedure_param::ProcedureParamKey},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{
	Result,
	catalog::Catalog,
	store::procedure::find::{decode_procedure, load_params},
};

pub(super) struct ProcedureParamApplier;

impl CatalogChangeApplier for ProcedureParamApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		reload_parent_procedure(catalog, txn, key)
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		reload_parent_procedure(catalog, txn, key)
	}
}

fn reload_parent_procedure(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
	let procedure_id = match ProcedureParamKey::decode(key) {
		Some(k) => k.procedure,
		None => return Ok(()),
	};

	let Some(entry) = txn.get(&ProcedureKey::encoded(procedure_id))? else {
		return Ok(());
	};

	let params = load_params(txn, procedure_id)?;
	let procedure = decode_procedure(&entry.row, params);
	catalog.materialized.set_procedure(procedure_id, txn.version(), Some(procedure));
	Ok(())
}
