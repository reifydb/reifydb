// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::id::RelationshipId, key::catalog::RelationshipKey};
use reifydb_transaction::transaction::admin::AdminTransaction;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn drop_relationship(txn: &mut AdminTransaction, id: RelationshipId) -> Result<()> {
		txn.remove(&RelationshipKey::encoded(id))?;
		Ok(())
	}
}
