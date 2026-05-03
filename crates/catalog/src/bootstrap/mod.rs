// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_core::{
	common::CommitVersion,
	event::EventBus,
	interface::catalog::{config::ConfigKey, id::NamespaceId},
	key::config::ConfigStorageKey,
};
use reifydb_runtime::context::clock::Clock;
use reifydb_store_multi::{
	buffer::storage::BufferStorage, persistent::PersistentStorage, store::multi::scan_tiers_latest,
};
use reifydb_transaction::{
	interceptor::interceptors::Interceptors,
	multi::transaction::MultiTransaction,
	single::SingleTransaction,
	transaction::{Transaction, admin::AdminTransaction, query::QueryTransaction},
};
use reifydb_type::value::{Value, identity::IdentityId};
use tracing::{info, warn};

use crate::{
	Result,
	cache::{
		CatalogCache,
		load::{CatalogCacheLoader, config::load_configs},
	},
	catalog::{Catalog, namespace::NamespaceToCreate},
	store::config::convert_config,
};

pub mod binding;
pub mod identity;
pub mod metric;
pub mod procedure;

pub fn bootstrap_system_objects(
	multi: &MultiTransaction,
	single: &SingleTransaction,
	catalog: &CatalogCache,
	eventbus: &EventBus,
) -> Result<()> {
	identity::bootstrap_root_identity(multi, single, catalog, eventbus)?;
	procedure::bootstrap_system_procedures(multi, single, catalog, eventbus)?;
	binding::bootstrap_system_bindings(multi, single, catalog, eventbus)?;
	metric::bootstrap_metric_ringbuffers(multi, single, catalog, eventbus)?;
	Ok(())
}

pub fn apply_bootstrap_configs(
	multi: &MultiTransaction,
	single: &SingleTransaction,
	catalog: &CatalogCache,
	eventbus: &EventBus,
	configs: &[(ConfigKey, Value)],
) -> Result<()> {
	if configs.is_empty() {
		return Ok(());
	}

	let mut admin = AdminTransaction::new(
		multi.clone(),
		single.clone(),
		eventbus.clone(),
		Interceptors::default(),
		IdentityId::system(),
		Clock::Real,
	)?;

	let catalog_api = Catalog::new(catalog.clone());
	for (key, value) in configs {
		catalog_api.set_config(&mut admin, *key, value.clone())?;
	}
	admin.commit()?;

	let mut qt = QueryTransaction::new(multi.begin_query()?, single.clone(), IdentityId::system());
	load_configs(&mut Transaction::Query(&mut qt), catalog)?;

	Ok(())
}

pub fn load_catalog_cache(multi: &MultiTransaction, single: &SingleTransaction, catalog: &CatalogCache) -> Result<()> {
	let mut qt = QueryTransaction::new(multi.begin_query()?, single.clone(), IdentityId::system());
	CatalogCacheLoader::load_all(&mut Transaction::Query(&mut qt), catalog)?;
	Ok(())
}

pub fn read_configs(
	buffer: Option<&BufferStorage>,
	persistent: Option<&PersistentStorage>,
	keys: &[ConfigKey],
) -> Result<HashMap<ConfigKey, Value>> {
	let mut found: HashMap<ConfigKey, Value> = HashMap::new();

	let range = ConfigStorageKey::full_scan();
	let batch = scan_tiers_latest(buffer, persistent, range, CommitVersion(u64::MAX), 1024)?;

	for multi in batch.items {
		let (key, value) = convert_config(multi);
		if !keys.contains(&key) {
			continue;
		}
		match key.accept(value) {
			Ok(canonical) => {
				found.insert(key, canonical);
			}
			Err(e) => {
				warn!("ignoring invalid persisted value for {key}: {e}; falling back to default");
			}
		}
	}

	let mut out: HashMap<ConfigKey, Value> = HashMap::with_capacity(keys.len());
	for key in keys {
		let value = found.remove(key).unwrap_or_else(|| key.default_value());
		out.insert(*key, value);
	}
	Ok(out)
}

#[cfg(test)]
mod read_configs_tests {
	use std::collections::HashMap;

	use reifydb_core::{
		common::CommitVersion,
		interface::{catalog::config::ConfigKey, store::EntryKind},
		key::config::ConfigStorageKey,
	};
	use reifydb_store_multi::{buffer::storage::BufferStorage, tier::TierStorage};
	use reifydb_type::value::Value;

	use super::read_configs;
	use crate::store::config::shape::config::{SHAPE, VALUE};

	fn write_config(buffer: &BufferStorage, key: ConfigKey, value: Value, version: CommitVersion) {
		let mut row = SHAPE.allocate();
		SHAPE.set_value(&mut row, VALUE, &Value::any(value));
		let key_bytes = ConfigStorageKey::for_key(key);
		let mut batches = HashMap::new();
		batches.insert(EntryKind::Multi, vec![(key_bytes.0, Some(row.0))]);
		buffer.set(version, batches).unwrap();
	}

	fn delete_config(buffer: &BufferStorage, key: ConfigKey, version: CommitVersion) {
		let key_bytes = ConfigStorageKey::for_key(key);
		let mut batches = HashMap::new();
		batches.insert(EntryKind::Multi, vec![(key_bytes.0, None)]);
		buffer.set(version, batches).unwrap();
	}

	#[test]
	fn returns_defaults_when_no_tiers_configured() {
		let out = read_configs(
			None,
			None,
			&[ConfigKey::ThreadsAsync, ConfigKey::ThreadsSystem, ConfigKey::ThreadsQuery],
		)
		.unwrap();
		assert_eq!(out[&ConfigKey::ThreadsAsync], Value::Uint2(1));
		assert_eq!(out[&ConfigKey::ThreadsSystem], Value::Uint2(2));
		assert_eq!(out[&ConfigKey::ThreadsQuery], Value::Uint2(1));
	}

	#[test]
	fn returns_defaults_when_buffer_is_empty() {
		let buffer = BufferStorage::memory();
		let out = read_configs(
			Some(&buffer),
			None,
			&[ConfigKey::ThreadsAsync, ConfigKey::ThreadsSystem, ConfigKey::ThreadsQuery],
		)
		.unwrap();
		assert_eq!(out[&ConfigKey::ThreadsAsync], Value::Uint2(1));
		assert_eq!(out[&ConfigKey::ThreadsSystem], Value::Uint2(2));
		assert_eq!(out[&ConfigKey::ThreadsQuery], Value::Uint2(1));
	}

	#[test]
	fn reads_persisted_value_from_buffer() {
		let buffer = BufferStorage::memory();
		write_config(&buffer, ConfigKey::ThreadsQuery, Value::Uint2(8), CommitVersion(1));

		let out =
			read_configs(Some(&buffer), None, &[ConfigKey::ThreadsQuery, ConfigKey::ThreadsAsync]).unwrap();

		assert_eq!(out[&ConfigKey::ThreadsQuery], Value::Uint2(8));
		assert_eq!(out[&ConfigKey::ThreadsAsync], Value::Uint2(1));
	}

	#[test]
	fn latest_version_wins() {
		let buffer = BufferStorage::memory();
		write_config(&buffer, ConfigKey::ThreadsSystem, Value::Uint2(4), CommitVersion(1));
		write_config(&buffer, ConfigKey::ThreadsSystem, Value::Uint2(16), CommitVersion(5));
		write_config(&buffer, ConfigKey::ThreadsSystem, Value::Uint2(8), CommitVersion(3));

		let out = read_configs(Some(&buffer), None, &[ConfigKey::ThreadsSystem]).unwrap();

		assert_eq!(out[&ConfigKey::ThreadsSystem], Value::Uint2(16));
	}

	#[test]
	fn tombstone_returns_default() {
		let buffer = BufferStorage::memory();
		write_config(&buffer, ConfigKey::ThreadsQuery, Value::Uint2(12), CommitVersion(1));
		delete_config(&buffer, ConfigKey::ThreadsQuery, CommitVersion(2));

		let out = read_configs(Some(&buffer), None, &[ConfigKey::ThreadsQuery]).unwrap();

		assert_eq!(out[&ConfigKey::ThreadsQuery], Value::Uint2(1));
	}

	#[test]
	fn rejects_invalid_persisted_value_and_falls_back_to_default() {
		let buffer = BufferStorage::memory();
		write_config(&buffer, ConfigKey::ThreadsAsync, Value::Uint2(0), CommitVersion(1));

		let out = read_configs(Some(&buffer), None, &[ConfigKey::ThreadsAsync]).unwrap();

		assert_eq!(out[&ConfigKey::ThreadsAsync], Value::Uint2(1));
	}

	#[test]
	fn unrequested_keys_are_ignored() {
		let buffer = BufferStorage::memory();
		write_config(&buffer, ConfigKey::ThreadsQuery, Value::Uint2(8), CommitVersion(1));
		write_config(&buffer, ConfigKey::OracleWindowSize, Value::Uint8(999), CommitVersion(1));

		let out = read_configs(Some(&buffer), None, &[ConfigKey::ThreadsQuery]).unwrap();

		assert_eq!(out.len(), 1);
		assert_eq!(out[&ConfigKey::ThreadsQuery], Value::Uint2(8));
		assert!(!out.contains_key(&ConfigKey::OracleWindowSize));
	}

	#[test]
	fn shape_stays_in_sync_with_set_config_path() {
		let buffer = BufferStorage::memory();
		let mut row = SHAPE.allocate();
		SHAPE.set_value(&mut row, VALUE, &Value::any(Value::Uint2(5)));

		let key_bytes = ConfigStorageKey::for_key(ConfigKey::ThreadsSystem);
		let mut batches = HashMap::new();
		batches.insert(EntryKind::Multi, vec![(key_bytes.0, Some(row.0))]);
		buffer.set(CommitVersion(1), batches).unwrap();

		let out = read_configs(Some(&buffer), None, &[ConfigKey::ThreadsSystem]).unwrap();
		assert_eq!(out[&ConfigKey::ThreadsSystem], Value::Uint2(5));
	}
}

pub(crate) fn ensure_namespace(
	catalog_api: &Catalog,
	admin: &mut AdminTransaction,
	id: NamespaceId,
	path: &str,
	local_name: &str,
	parent_id: NamespaceId,
) -> Result<NamespaceId> {
	if let Some(ns) = catalog_api.find_namespace_by_path(&mut Transaction::Admin(admin), path)? {
		return Ok(ns.id());
	}

	let ns = catalog_api.create_namespace_with_id(
		admin,
		id,
		NamespaceToCreate {
			namespace_fragment: None,
			name: path.to_string(),
			local_name: local_name.to_string(),
			parent_id,
			token: None,
			grpc: None,
		},
	)?;
	info!("Created {} namespace", path);
	Ok(ns.id())
}
