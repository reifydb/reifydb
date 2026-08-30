// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{HashMap, HashSet};

use reifydb_core::{
	common::CommitVersion,
	event::EventBus,
	interface::catalog::{config::ConfigKey, id::NamespaceId},
	key::config::ConfigStorageKey,
};
use reifydb_runtime::context::clock::Clock;
use reifydb_store_commit::{MultiVersionScope, store::CommitStore};
use reifydb_store_multi::{store::multi::scan_tiers_latest, tier::persistent::MultiPersistentTier};
use reifydb_transaction::{
	interceptor::interceptors::Interceptors,
	multi::transaction::MultiTransaction,
	single::SingleTransaction,
	transaction::{Transaction, admin::AdminTransaction, query::QueryTransaction},
};
use reifydb_value::{
	fragment::Fragment,
	value::{Value, constraint::TypeConstraint, identity::IdentityId, value_type::ValueType},
};
use tracing::{info, warn};

use crate::{
	Result,
	cache::{
		CatalogCache,
		load::{CatalogCacheLoader, config::load_configs},
	},
	catalog::{Catalog, namespace::NamespaceToCreate, series::SeriesColumnToCreate, table::TableColumnToCreate},
	store::config::convert_config,
};

pub mod binding;
pub mod completeness;
pub mod epoch;
pub mod flow;
pub mod identity;
pub mod instruments;
pub mod lifecycle;
pub mod metric;
pub mod policy;
pub mod proc;
pub mod procedure;
pub mod profiler;
pub mod runtime;
pub mod store;

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
	profiler::bootstrap_profiler(multi, single, catalog, eventbus)?;
	runtime::bootstrap_runtime(multi, single, catalog, eventbus)?;
	proc::bootstrap_proc(multi, single, catalog, eventbus)?;
	store::bootstrap_store(multi, single, catalog, eventbus)?;
	instruments::bootstrap_instruments(multi, single, catalog, eventbus)?;
	epoch::bootstrap_epoch(multi, single, catalog, eventbus)?;
	lifecycle::bootstrap_lifecycle(multi, single, catalog, eventbus)?;
	flow::bootstrap_flow(multi, single, catalog, eventbus)?;
	completeness::bootstrap_completeness(multi, single, catalog, eventbus)?;
	policy::bootstrap_call_policies(multi, single, eventbus)?;
	load_catalog_cache(multi, single, catalog)?;
	Ok(())
}

pub fn reject_duplicate_configs(configs: &[(ConfigKey, Value)]) {
	let mut seen: HashSet<ConfigKey> = HashSet::with_capacity(configs.len());
	for (key, _) in configs {
		assert!(seen.insert(*key), "bootstrap config {key:?} set more than once");
	}
}

pub fn seed_bootstrap_configs(
	multi: &MultiTransaction,
	catalog: &CatalogCache,
	configs: &[(ConfigKey, Value)],
) -> Result<()> {
	reject_duplicate_configs(configs);
	if !configs.is_empty() {
		let version = multi.version()?;
		for (key, value) in configs {
			catalog.set_config(*key, version, value.clone())?;
		}
	}
	#[cfg(reifydb_assertions)]
	catalog.clear_pending_config_overrides();
	Ok(())
}

pub fn apply_bootstrap_configs(
	multi: &MultiTransaction,
	single: &SingleTransaction,
	catalog: &CatalogCache,
	eventbus: &EventBus,
	configs: &[(ConfigKey, Value)],
) -> Result<()> {
	reject_duplicate_configs(configs);
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
	buffer: Option<&CommitStore>,
	persistent: Option<&MultiPersistentTier>,
	keys: &[ConfigKey],
) -> Result<HashMap<ConfigKey, Value>> {
	let mut found: HashMap<ConfigKey, Value> = HashMap::new();

	let range = ConfigStorageKey::full_scan();
	let batch = scan_tiers_latest(
		buffer,
		persistent,
		range,
		MultiVersionScope::AsOf {
			read: CommitVersion(u64::MAX),
		},
		1024,
	)?;

	for multi in batch.items {
		let Some((key, value)) = convert_config(multi) else {
			continue;
		};
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

	use reifydb_codec::row::bytes::RowBuilder;
	use reifydb_core::{
		common::CommitVersion,
		interface::{catalog::config::ConfigKey, store::EntryKind},
		key::config::ConfigStorageKey,
	};
	use reifydb_store_commit::store::CommitStore;
	use reifydb_value::value::Value;

	use super::read_configs;
	use crate::store::config::shape::config;

	fn write_config(buffer: &CommitStore, key: ConfigKey, value: Value, version: CommitVersion) {
		let mut row = config::allocate();
		config::set_value(&mut row, &Value::any(value));
		let key_bytes = ConfigStorageKey::for_key(key);
		let mut batches = HashMap::new();
		batches.insert(EntryKind::Multi, vec![(key_bytes, Some(row.freeze_bytes().0))]);
		buffer.set(version, batches).unwrap();
	}

	fn delete_config(buffer: &CommitStore, key: ConfigKey, version: CommitVersion) {
		let key_bytes = ConfigStorageKey::for_key(key);
		let mut batches = HashMap::new();
		batches.insert(EntryKind::Multi, vec![(key_bytes, None)]);
		buffer.set(version, batches).unwrap();
	}

	#[test]
	fn returns_defaults_when_no_tiers_configured() {
		// Compared against the key's own default rather than a literal: the default is profile dependent,
		// so a literal pins whichever profile the test happened to build under and not this function.
		let keys = [ConfigKey::ThreadsAsync, ConfigKey::ThreadsCoordination, ConfigKey::ThreadsTask];
		let out = read_configs(None, None, &keys).unwrap();
		for key in keys {
			assert_eq!(out[&key], key.default_value(), "{key} must fall back to its own default");
		}
	}

	#[test]
	fn returns_defaults_when_buffer_is_empty() {
		// A buffer holding nothing must read the same as no buffer at all; the values are the key's own
		// defaults rather than literals because the default is profile dependent.
		let buffer = CommitStore::new();
		let keys = [ConfigKey::ThreadsAsync, ConfigKey::ThreadsCoordination, ConfigKey::ThreadsTask];
		let out = read_configs(Some(&buffer), None, &keys).unwrap();
		for key in keys {
			assert_eq!(out[&key], key.default_value(), "{key} must fall back to its own default");
		}
	}

	#[test]
	fn reads_persisted_value_from_buffer() {
		let buffer = CommitStore::new();
		write_config(&buffer, ConfigKey::ThreadsTask, Value::Uint2(8), CommitVersion(1));

		let out =
			read_configs(Some(&buffer), None, &[ConfigKey::ThreadsTask, ConfigKey::ThreadsAsync]).unwrap();

		// The persisted key is a literal because the write above chose it; the unwritten one is named
		// through the key, because its default is profile dependent and a literal would pin the profile.
		assert_eq!(
			out[&ConfigKey::ThreadsTask],
			Value::Uint2(8),
			"a persisted value must win over the default"
		);
		assert_eq!(
			out[&ConfigKey::ThreadsAsync],
			ConfigKey::ThreadsAsync.default_value(),
			"a key nobody persisted must still read as its own default"
		);
	}

	#[test]
	fn latest_version_wins() {
		let buffer = CommitStore::new();
		write_config(&buffer, ConfigKey::ThreadsCoordination, Value::Uint2(4), CommitVersion(1));
		write_config(&buffer, ConfigKey::ThreadsCoordination, Value::Uint2(16), CommitVersion(5));
		write_config(&buffer, ConfigKey::ThreadsCoordination, Value::Uint2(8), CommitVersion(3));

		let out = read_configs(Some(&buffer), None, &[ConfigKey::ThreadsCoordination]).unwrap();

		assert_eq!(out[&ConfigKey::ThreadsCoordination], Value::Uint2(16));
	}

	#[test]
	fn tombstone_returns_default() {
		let buffer = CommitStore::new();
		write_config(&buffer, ConfigKey::ThreadsTask, Value::Uint2(12), CommitVersion(1));
		delete_config(&buffer, ConfigKey::ThreadsTask, CommitVersion(2));

		let out = read_configs(Some(&buffer), None, &[ConfigKey::ThreadsTask]).unwrap();

		// The default, not the value the tombstone buried, and named through the key so the assertion does
		// not pin a profile.
		assert_ne!(out[&ConfigKey::ThreadsTask], Value::Uint2(12), "a tombstoned value must not survive");
		assert_eq!(out[&ConfigKey::ThreadsTask], ConfigKey::ThreadsTask.default_value());
	}

	#[test]
	fn rejects_invalid_persisted_value_and_falls_back_to_default() {
		let buffer = CommitStore::new();
		write_config(&buffer, ConfigKey::ThreadsAsync, Value::Uint2(0), CommitVersion(1));

		let out = read_configs(Some(&buffer), None, &[ConfigKey::ThreadsAsync]).unwrap();

		// Zero threads is refused by the key, so the default is what is left; named through the key rather
		// than written as a literal, which would pin whichever profile the test built under.
		assert_eq!(
			out[&ConfigKey::ThreadsAsync],
			ConfigKey::ThreadsAsync.default_value(),
			"a value the key refuses must not reach the caller"
		);
	}

	#[test]
	fn unrequested_keys_are_ignored() {
		let buffer = CommitStore::new();
		write_config(&buffer, ConfigKey::ThreadsTask, Value::Uint2(8), CommitVersion(1));
		write_config(&buffer, ConfigKey::OracleWindowSize, Value::Uint8(999), CommitVersion(1));

		let out = read_configs(Some(&buffer), None, &[ConfigKey::ThreadsTask]).unwrap();

		assert_eq!(out.len(), 1);
		assert_eq!(out[&ConfigKey::ThreadsTask], Value::Uint2(8));
		assert!(!out.contains_key(&ConfigKey::OracleWindowSize));
	}

	#[test]
	fn shape_stays_in_sync_with_set_config_path() {
		let buffer = CommitStore::new();
		let mut row = config::allocate();
		config::set_value(&mut row, &Value::any(Value::Uint2(5)));

		let key_bytes = ConfigStorageKey::for_key(ConfigKey::ThreadsCoordination);
		let mut batches = HashMap::new();
		batches.insert(EntryKind::Multi, vec![(key_bytes, Some(row.freeze_bytes().0))]);
		buffer.set(CommitVersion(1), batches).unwrap();

		let out = read_configs(Some(&buffer), None, &[ConfigKey::ThreadsCoordination]).unwrap();
		assert_eq!(out[&ConfigKey::ThreadsCoordination], Value::Uint2(5));
	}
}

pub(crate) fn series_col(name: &str, ty: ValueType) -> SeriesColumnToCreate {
	SeriesColumnToCreate {
		name: Fragment::internal(name),
		fragment: Fragment::internal(name),
		constraint: TypeConstraint::unconstrained(ty),
		properties: vec![],
		auto_increment: false,
		dictionary_id: None,
	}
}

pub(crate) fn table_col(name: &str, ty: ValueType) -> TableColumnToCreate {
	TableColumnToCreate {
		name: Fragment::internal(name),
		fragment: Fragment::internal(name),
		constraint: TypeConstraint::unconstrained(ty),
		properties: vec![],
		auto_increment: false,
		dictionary_id: None,
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
