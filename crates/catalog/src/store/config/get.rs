// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::key::config::ConfigKey;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::Value;

use crate::{CatalogStore, Result, store::config};

impl CatalogStore {
	pub(crate) fn get_config(rx: &mut Transaction<'_>, key: &str) -> Result<Option<Value>> {
		Ok(rx.get(&ConfigKey::for_key(key))?.map(|multi| {
			let (_, value) = config::convert_config(multi);
			value
		}))
	}
}
