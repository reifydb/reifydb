// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::key::config::ConfigKey;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use crate::{CatalogStore, Result, store::config::schema::config};

impl CatalogStore {
	pub(crate) fn set_config(txn: &mut AdminTransaction, key: &str, value: &Value) -> Result<()> {
		let mut row = config::SCHEMA.allocate();
		config::SCHEMA.set_value(&mut row, config::VALUE, &Value::any(value.clone()));
		txn.set(&ConfigKey::for_key(key), row)?;
		Ok(())
	}
}
