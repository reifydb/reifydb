// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::id::RelationshipId, key::catalog::RelationshipKey};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_value::error::ValueError;

use crate::CatalogStore;

impl CatalogStore {
	pub(crate) fn drop_relationship(txn: &mut AdminTransaction, id: RelationshipId) -> Result<(), ValueError> {
		txn.remove(vec![(RelationshipKey::new(id).into(), None)])?;
		Ok(())
	}
}
