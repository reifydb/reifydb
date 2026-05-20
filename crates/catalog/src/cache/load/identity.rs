// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::key::identity::IdentityKey;
use reifydb_transaction::transaction::Transaction;

use super::CatalogCache;
use crate::{Result, store::identity::convert_identity};

pub(crate) fn load_identities(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = IdentityKey::full_scan();
	let stream = rx.range(range, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;
		let identity = convert_identity(multi);
		catalog.set_identity(identity.id, version, Some(identity));
	}

	Ok(())
}
