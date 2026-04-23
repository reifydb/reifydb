// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackConfigChangeOperations,
	config::{Config, ConfigKey},
};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use super::Catalog;
use crate::{CatalogStore, Result, materialized::check_config_value};

impl Catalog {
	pub fn set_config(&self, txn: &mut AdminTransaction, key: ConfigKey, value: Value) -> Result<()> {
		check_config_value(key, &value)?;

		let pre_value = self.materialized.get_config(key);
		let pre = Config {
			key,
			value: pre_value,
			default_value: key.default_value(),
			description: key.description(),
			requires_restart: key.requires_restart(),
		};

		CatalogStore::set_config(txn, key, &value)?;

		let post = Config {
			key,
			value: value.clone(),
			default_value: key.default_value(),
			description: key.description(),
			requires_restart: key.requires_restart(),
		};

		txn.track_config_set(pre, post)?;

		Ok(())
	}
}
