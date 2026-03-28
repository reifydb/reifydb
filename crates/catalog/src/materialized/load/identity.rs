// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::key::identity::IdentityKey;
use reifydb_transaction::transaction::Transaction;

use super::MaterializedCatalog;
use crate::{Result, store::identity::convert_identity};

pub(crate) fn load_identities(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> Result<()> {
	let range = IdentityKey::full_scan();
	let mut stream = rx.range(range, 1024)?;

	while let Some(entry) = stream.next() {
		let multi = entry?;
		let version = multi.version;
		let identity = convert_identity(multi);
		catalog.set_identity(identity.id, version, Some(identity));
	}

	Ok(())
}
