// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::user_authentication::UserAuthenticationId, key::user_authentication::UserAuthenticationKey,
};
use reifydb_transaction::transaction::admin::AdminTransaction;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn drop_user_authentication(txn: &mut AdminTransaction, id: UserAuthenticationId) -> Result<()> {
		txn.remove(&UserAuthenticationKey::encoded(id))?;
		Ok(())
	}
}
