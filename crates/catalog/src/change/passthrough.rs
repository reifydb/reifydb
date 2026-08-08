// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{encoded::bytes::EncodedBytes, key::encoded::EncodedKey};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{Result, catalog::Catalog};

pub(super) struct PassthroughApplier;

impl CatalogChangeApplier for PassthroughApplier {
	fn set(_catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, bytes: &EncodedBytes) -> Result<()> {
		txn.set(key, bytes.clone())
	}

	fn remove(_catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)
	}
}
