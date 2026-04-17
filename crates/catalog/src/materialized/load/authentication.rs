// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::key::authentication::AuthenticationKey;
use reifydb_transaction::transaction::Transaction;

use super::MaterializedCatalog;
use crate::{Result, store::authentication::convert_authentication};

pub(crate) fn load_authentications(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> Result<()> {
	let range = AuthenticationKey::full_scan();
	let stream = rx.range(range, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;
		let auth = convert_authentication(multi);
		catalog.set_authentication(auth.id, version, Some(auth));
	}

	Ok(())
}
