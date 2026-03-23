// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::authentication::AuthenticationId, key::authentication::AuthenticationKey};
use reifydb_transaction::transaction::admin::AdminTransaction;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn drop_authentication(txn: &mut AdminTransaction, id: AuthenticationId) -> Result<()> {
		txn.remove(&AuthenticationKey::encoded(id))?;
		Ok(())
	}
}
