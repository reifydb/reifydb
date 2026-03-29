// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Series has no materialized catalog representation. Write to the store only.

use reifydb_core::encoded::{key::EncodedKey, row::EncodedRow};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{Result, catalog::Catalog};

pub(super) struct SeriesApplier;

impl CatalogChangeApplier for SeriesApplier {
	fn set(_catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())
	}

	fn remove(_catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)
	}
}
