// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	key::{EncodableKey, config::ConfigKey},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::Value;

use super::CatalogChangeApplier;
use crate::{
	Result,
	catalog::Catalog,
	store::config::shape::config::{SHAPE, VALUE},
};

pub(super) struct ConfigApplier;

impl CatalogChangeApplier for ConfigApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		apply_config(catalog, key, row, txn.version());
		Ok(())
	}

	fn remove(_catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)
	}
}

use reifydb_core::common::CommitVersion;

fn apply_config(catalog: &Catalog, key: &EncodedKey, row: &EncodedRow, version: CommitVersion) {
	let Some(config_key) = ConfigKey::decode(key).map(|k| k.key) else {
		return;
	};
	let value = match SHAPE.get_value(row, VALUE) {
		Value::Any(inner) => *inner,
		other => other,
	};
	catalog.materialized.set_system_config(config_key, version, value);
}
