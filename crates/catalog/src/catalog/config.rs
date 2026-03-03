// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::config::ConfigDef;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use crate::{CatalogStore, Result, catalog::Catalog, error::CatalogError};

impl Catalog {
	pub fn set_config(&self, txn: &mut AdminTransaction, key: &str, value: Value) -> Result<()> {
		if self.materialized.system_config().get(key).is_none() {
			return Err(CatalogError::ConfigKeyNotFound(key.to_string()).into());
		}
		CatalogStore::set_config(txn, key, &value)?;
		txn.changes.add_config_change(key.to_string(), value);
		Ok(())
	}

	pub fn list_configs(&self) -> Vec<ConfigDef> {
		self.materialized.list_configs()
	}
}
