// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::config::ConfigKey, key::config::ConfigStorageKey};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use crate::{CatalogStore, Result, store::config::shape::config};

impl CatalogStore {
	pub(crate) fn set_config(txn: &mut AdminTransaction, key: ConfigKey, value: &Value) -> Result<()> {
		let mut row = config::SHAPE.allocate();
		config::SHAPE.set_value(&mut row, config::VALUE, &Value::any(value.clone()));
		txn.set(&ConfigStorageKey::for_key(key), row)?;
		Ok(())
	}
}
