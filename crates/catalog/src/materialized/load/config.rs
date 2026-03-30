// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::key::config::ConfigKey;
use reifydb_transaction::transaction::Transaction;

use super::MaterializedCatalog;
use crate::{Result, store::config};

/// Load all persisted config overrides from storage and apply them to the registry.
///
/// This must run FIRST in `load_all` so that subsystems reading config during their
/// own bootstrap phase already see the correct (persisted) values.
pub(crate) fn load_configs(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> Result<()> {
	let version = rx.version();
	let range = ConfigKey::full_scan();
	let stream = rx.range(range, 1024)?;

	for entry in stream {
		let multi = entry?;
		let (key, value) = config::convert_config(multi);
		if !key.is_empty() {
			catalog.system_config().apply_persisted(&key, version, value);
		}
	}

	Ok(())
}
