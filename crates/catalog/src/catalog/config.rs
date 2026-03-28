// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::config::Config;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::{Value, r#type::Type};

use crate::{CatalogStore, Result, catalog::Catalog, error::CatalogError};

impl Catalog {
	pub fn set_config(&self, txn: &mut AdminTransaction, key: &str, value: Value) -> Result<()> {
		if self.materialized.system_config().get(key).is_none() {
			return Err(CatalogError::ConfigKeyNotFound(key.to_string()).into());
		}
		if matches!(value, Value::None { .. }) {
			return Err(CatalogError::ConfigValueInvalid(key.to_string()).into());
		}
		let expected = self
			.materialized
			.system_config()
			.get_default(key)
			.expect("key exists, default must be present")
			.get_type();
		let actual = value.get_type();
		if !types_compatible(&expected, &actual) {
			return Err(CatalogError::ConfigTypeMismatch {
				key: key.to_string(),
				expected,
				actual,
			}
			.into());
		}
		CatalogStore::set_config(txn, key, &value)?;
		txn.changes.add_config_change(key.to_string(), value);
		Ok(())
	}

	pub fn list_configs(&self) -> Vec<Config> {
		self.materialized.list_configs()
	}
}

fn types_compatible(expected: &Type, actual: &Type) -> bool {
	if expected == actual {
		return true;
	}
	is_numeric(expected) && is_numeric(actual)
}

fn is_numeric(ty: &Type) -> bool {
	matches!(
		ty,
		Type::Int1
			| Type::Int2 | Type::Int4
			| Type::Int8 | Type::Int16
			| Type::Uint1 | Type::Uint2
			| Type::Uint4 | Type::Uint8
			| Type::Uint16 | Type::Float4
			| Type::Float8 | Type::Int
			| Type::Uint | Type::Decimal
	)
}
