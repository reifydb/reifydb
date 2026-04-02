// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	key::{
		EncodableKey,
		retention_policy::{OperatorRetentionPolicyKey, ShapeRetentionPolicyKey},
	},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{Result, catalog::Catalog, store::retention_policy::decode_retention_policy};

pub(super) struct ShapeRetentionPolicyApplier;

impl CatalogChangeApplier for ShapeRetentionPolicyApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		if let Some(k) = ShapeRetentionPolicyKey::decode(key)
			&& let Some(policy) = decode_retention_policy(row)
		{
			catalog.materialized.set_shape_retention_policy(k.shape, txn.version(), Some(policy));
		}
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		if let Some(k) = ShapeRetentionPolicyKey::decode(key) {
			catalog.materialized.set_shape_retention_policy(k.shape, txn.version(), None);
		}
		Ok(())
	}
}

pub(super) struct OperatorRetentionPolicyApplier;

impl CatalogChangeApplier for OperatorRetentionPolicyApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		if let Some(k) = OperatorRetentionPolicyKey::decode(key)
			&& let Some(policy) = decode_retention_policy(row)
		{
			catalog.materialized.set_operator_retention_policy(k.operator, txn.version(), Some(policy));
		}
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		if let Some(k) = OperatorRetentionPolicyKey::decode(key) {
			catalog.materialized.set_operator_retention_policy(k.operator, txn.version(), None);
		}
		Ok(())
	}
}
