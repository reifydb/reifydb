// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	key::{EncodableKey, row_settings::RowSettingsKey},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{Result, catalog::Catalog, store::row_settings::decode_row_settings};

pub(super) struct RowSettingsApplier;

impl CatalogChangeApplier for RowSettingsApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		if let Some(k) = RowSettingsKey::decode(key)
			&& let Some(config) = decode_row_settings(row)
		{
			catalog.cache.set_row_settings(k.shape, txn.version(), Some(config));
		}
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		if let Some(k) = RowSettingsKey::decode(key) {
			catalog.cache.set_row_settings(k.shape, txn.version(), None);
		}
		Ok(())
	}
}
