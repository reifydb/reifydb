// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::key::user::UserKey;
use reifydb_transaction::transaction::Transaction;

use super::MaterializedCatalog;
use crate::store::user::convert_user;

pub(crate) fn load_users(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> crate::Result<()> {
	let range = UserKey::full_scan();
	let mut stream = rx.range(range, 1024)?;

	while let Some(entry) = stream.next() {
		let multi = entry?;
		let version = multi.version;
		let user_def = convert_user(multi);
		catalog.set_user(user_def.id, version, Some(user_def));
	}

	Ok(())
}
