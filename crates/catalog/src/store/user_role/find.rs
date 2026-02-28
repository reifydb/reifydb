// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::user::{UserId, UserRoleDef},
	key::user_role::UserRoleKey,
};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result, store::user_role::convert_user_role};

impl CatalogStore {
	#[allow(dead_code)]
	pub(crate) fn find_roles_for_user(rx: &mut Transaction<'_>, user: UserId) -> Result<Vec<UserRoleDef>> {
		let mut result = Vec::new();
		let range = UserRoleKey::user_scan(user);
		let mut stream = rx.range(range, 1024)?;

		while let Some(entry) = stream.next() {
			let multi = entry?;
			result.push(convert_user_role(multi));
		}

		Ok(result)
	}
}
