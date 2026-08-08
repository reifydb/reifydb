// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::key::{EncodableKey, config::ConfigStorageKey};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::Value;

use super::CatalogChangeApplier;
use crate::{
	Result,
	catalog::Catalog,
	store::config::shape::config::{SHAPE, VALUE},
};

pub(super) struct ConfigApplier;

impl CatalogChangeApplier for ConfigApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, bytes: &EncodedBytes) -> Result<()> {
		txn.set(key, bytes.clone())?;
		apply_config(catalog, key, bytes, txn.version())?;
		Ok(())
	}

	fn remove(_catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)
	}
}

use reifydb_core::common::CommitVersion;

fn apply_config(catalog: &Catalog, key: &EncodedKey, bytes: &EncodedBytes, version: CommitVersion) -> Result<()> {
	let Some(config_key) = ConfigStorageKey::decode(key).map(|k| k.key) else {
		return Ok(());
	};
	let value = match SHAPE.get_value(bytes, VALUE) {
		Value::Any(inner) => *inner,
		other => other,
	};
	catalog.cache.set_config(config_key, version, value)?;
	Ok(())
}
