// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackSystemConfigChangeOperations,
	config::{SystemConfig, SystemConfigKey},
};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use super::Catalog;
use crate::{CatalogStore, Result};

impl Catalog {
	pub fn set_system_config(&self, txn: &mut AdminTransaction, key: SystemConfigKey, value: Value) -> Result<()> {
		let pre_value = self.materialized.get_system_config(key);
		let pre = SystemConfig {
			key,
			value: pre_value,
			default_value: key.default_value(),
			description: key.description(),
			requires_restart: key.requires_restart(),
		};

		CatalogStore::set_system_config(txn, key, &value)?;

		let post = SystemConfig {
			key,
			value: value.clone(),
			default_value: key.default_value(),
			description: key.description(),
			requires_restart: key.requires_restart(),
		};

		txn.track_system_config_set(pre, post)?;

		Ok(())
	}
}
