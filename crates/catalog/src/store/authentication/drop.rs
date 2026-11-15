// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::authentication::AuthenticationId, key::identity::AuthenticationKey};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_value::error::ValueError;

use crate::CatalogStore;

impl CatalogStore {
	pub(crate) fn drop_authentication(txn: &mut AdminTransaction, id: AuthenticationId) -> Result<(), ValueError> {
		txn.remove(vec![(AuthenticationKey::new(id).into(), None)])?;
		Ok(())
	}
}
