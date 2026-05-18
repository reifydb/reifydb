// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	key::{EncodableKey, operator_ttl::OperatorTtlKey},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{Result, catalog::Catalog, store::ttl::decode_ttl_config};

pub(super) struct OperatorTtlApplier;

impl CatalogChangeApplier for OperatorTtlApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		if let Some(k) = OperatorTtlKey::decode(key)
			&& let Some(config) = decode_ttl_config(row)
		{
			catalog.cache.set_operator_ttl(k.node, txn.version(), Some(config));
		}
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		if let Some(k) = OperatorTtlKey::decode(key) {
			catalog.cache.set_operator_ttl(k.node, txn.version(), None);
		}
		Ok(())
	}
}
