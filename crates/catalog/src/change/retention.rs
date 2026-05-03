// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	key::{
		EncodableKey,
		retention_strategy::{OperatorRetentionStrategyKey, ShapeRetentionStrategyKey},
	},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{Result, catalog::Catalog, store::retention_strategy::decode_retention_strategy};

pub(super) struct ShapeRetentionStrategyApplier;

impl CatalogChangeApplier for ShapeRetentionStrategyApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		if let Some(k) = ShapeRetentionStrategyKey::decode(key)
			&& let Some(policy) = decode_retention_strategy(row)
		{
			catalog.cache.set_shape_retention_strategy(k.shape, txn.version(), Some(policy));
		}
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		if let Some(k) = ShapeRetentionStrategyKey::decode(key) {
			catalog.cache.set_shape_retention_strategy(k.shape, txn.version(), None);
		}
		Ok(())
	}
}

pub(super) struct OperatorRetentionStrategyApplier;

impl CatalogChangeApplier for OperatorRetentionStrategyApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		if let Some(k) = OperatorRetentionStrategyKey::decode(key)
			&& let Some(policy) = decode_retention_strategy(row)
		{
			catalog.cache.set_operator_retention_strategy(k.operator, txn.version(), Some(policy));
		}
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		if let Some(k) = OperatorRetentionStrategyKey::decode(key) {
			catalog.cache.set_operator_retention_strategy(k.operator, txn.version(), None);
		}
		Ok(())
	}
}
