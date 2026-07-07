// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::id::RelationshipId, key::relationship::RelationshipKey};
use reifydb_transaction::transaction::admin::AdminTransaction;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn drop_relationship(txn: &mut AdminTransaction, id: RelationshipId) -> Result<()> {
		txn.remove(&RelationshipKey::encoded(id))?;
		Ok(())
	}
}
