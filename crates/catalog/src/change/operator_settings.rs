// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	key::{EncodableKey, operator_settings::OperatorSettingsKey},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{Result, catalog::Catalog, store::operator_settings::decode_operator_settings};

pub(super) struct OperatorSettingsApplier;

impl CatalogChangeApplier for OperatorSettingsApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		if let Some(k) = OperatorSettingsKey::decode(key)
			&& let Some(config) = decode_operator_settings(row)
		{
			catalog.cache.set_operator_settings(k.operator, txn.version(), Some(config));
		}
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		if let Some(k) = OperatorSettingsKey::decode(key) {
			catalog.cache.set_operator_settings(k.operator, txn.version(), None);
		}
		Ok(())
	}
}
