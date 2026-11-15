// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::config::ConfigKey, key::config::ConfigStorageKey};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_value::{error::ValueError, value::Value};

use crate::{CatalogStore, store::config::shape::config};

impl CatalogStore {
	pub(crate) fn set_config(txn: &mut AdminTransaction, key: ConfigKey, value: &Value) -> Result<(), ValueError> {
		let mut row = config::allocate();
		config::set_value(&mut row, &Value::any(value.clone()));
		txn.set(vec![(ConfigStorageKey::new(key).into(), row.freeze().into())])?;
		Ok(())
	}
}
