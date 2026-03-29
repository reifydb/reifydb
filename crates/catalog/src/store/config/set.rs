// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::key::config::ConfigKey;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use crate::{CatalogStore, Result, store::config::shape::config};

impl CatalogStore {
	pub(crate) fn set_config(txn: &mut AdminTransaction, key: &str, value: &Value) -> Result<()> {
		let mut row = config::SHAPE.allocate();
		config::SHAPE.set_value(&mut row, config::VALUE, &Value::any(value.clone()));
		txn.set(&ConfigKey::for_key(key), row)?;
		Ok(())
	}
}
