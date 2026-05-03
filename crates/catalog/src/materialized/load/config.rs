// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::key::config::ConfigStorageKey;
use reifydb_transaction::transaction::Transaction;

use super::MaterializedCatalog;
use crate::{Result, store::config};

pub(crate) fn load_configs(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> Result<()> {
	let version = rx.version();
	let range = ConfigStorageKey::full_scan();
	let stream = rx.range(range, 1024)?;

	for entry in stream {
		let multi = entry?;
		let (key, value) = config::convert_config(multi);
		catalog.set_config(key, version, value)?;
	}

	Ok(())
}
