// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	key::{EncodableKey, kind::KeyKind, procedure::ProcedureKey},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{
	Result,
	catalog::Catalog,
	error::CatalogChangeError,
	store::procedure::find::{decode_procedure, load_params},
};

pub(super) struct ProcedureApplier;

impl CatalogChangeApplier for ProcedureApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		let id = ProcedureKey::decode(key).map(|k| k.procedure).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::Procedure,
		})?;
		let params = load_params(txn, id)?;
		let procedure = decode_procedure(row, params);
		catalog.cache.set_procedure(id, txn.version(), Some(procedure));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = ProcedureKey::decode(key).map(|k| k.procedure).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::Procedure,
		})?;
		catalog.cache.set_procedure(id, txn.version(), None);
		Ok(())
	}
}
