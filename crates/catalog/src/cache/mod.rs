// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! In-memory materialised view of the catalog: every catalog object the running engine has seen, indexed for fast
//! lookup by id and qualified name. The materialised view is loaded at boot, kept in sync with catalog mutations
//! through change events, and is what the engine, planner, and policy evaluator query when they need catalog state
//! on the hot path.
//!
//! Because reads here are ubiquitous, the materialised view never blocks on the storage tier - it is built from
//! storage at boot and updated incrementally afterwards. A miss here means the catalog is genuinely missing the
//! object, not that we need to fall back to storage.

pub mod authentication;
pub mod binding;
pub mod config;
pub mod dictionary;
pub mod flow;
pub mod granted_role;
pub mod handler;
pub mod identity;
pub mod load;
pub mod migration;
pub mod namespace;
pub mod operator_retention_strategy;
pub mod operator_ttl;
pub mod policy;
pub mod primary_key;
pub mod procedure;
pub mod ringbuffer;
pub mod role;
pub mod row_shape;
pub mod row_ttl;
pub mod series;
pub mod shape_retention_strategy;
pub mod sink;
pub mod source;
pub mod sumtype;
pub mod table;
pub mod test;
pub mod view;

use std::{ops, sync::Arc};

use crossbeam_skiplist::SkipMap;
use reifydb_core::{
	common::CommitVersion,
	encoded::shape::{RowShape, fingerprint::RowShapeFingerprint},
	interface::catalog::{
		authentication::{Authentication, AuthenticationId},
		binding::Binding,
		config::{Config, ConfigKey, GetConfig},
		dictionary::Dictionary,
		flow::{Flow, FlowId, FlowNodeId},
		handler::Handler,
		id::{
			BindingId, HandlerId, MigrationEventId, MigrationId, NamespaceId, PrimaryKeyId, ProcedureId,
			RingBufferId, SeriesId, SinkId, SourceId, TableId, TestId, ViewId,
		},
		identity::{GrantedRole, Identity, Role, RoleId},
		key::PrimaryKey,
		migration::{Migration, MigrationEvent},
		namespace::Namespace,
		policy::{Policy, PolicyId, PolicyOperation},
		procedure::Procedure,
		ringbuffer::RingBuffer,
		series::Series,
		shape::ShapeId,
		sink::Sink,
		source::Source,
		sumtype::SumType,
		table::Table,
		test::Test,
		view::View,
		vtable::{VTable, VTableId},
	},
	retention::RetentionStrategy,
	row::Ttl,
	util::multi::MultiVersionContainer,
};
use reifydb_type::{
	fragment::Fragment,
	value::{
		Value,
		dictionary::DictionaryId,
		identity::IdentityId,
		sumtype::{SumTypeId, VariantRef},
	},
};

use crate::{
	Result,
	error::{CatalogError, CatalogObjectKind},
};

pub type MultiVersionBinding = MultiVersionContainer<Binding>;
pub type MultiVersionNamespace = MultiVersionContainer<Namespace>;
pub type MultiVersionTable = MultiVersionContainer<Table>;
pub type MultiVersionView = MultiVersionContainer<View>;
pub type MultiVersionFlow = MultiVersionContainer<Flow>;
pub type MultiVersionPrimaryKey = MultiVersionContainer<PrimaryKey>;
pub type MultiVersionRetentionStrategy = MultiVersionContainer<RetentionStrategy>;
pub type MultiVersionDictionary = MultiVersionContainer<Dictionary>;
pub type MultiVersionHandler = MultiVersionContainer<Handler>;
pub type MultiVersionMigration = MultiVersionContainer<Migration>;
pub type MultiVersionMigrationEvent = MultiVersionContainer<MigrationEvent>;
pub type MultiVersionProcedure = MultiVersionContainer<Procedure>;
pub type MultiVersionRingBuffer = MultiVersionContainer<RingBuffer>;
pub type MultiVersionSeries = MultiVersionContainer<Series>;
pub type MultiVersionTest = MultiVersionContainer<Test>;
pub type MultiVersionSumType = MultiVersionContainer<SumType>;
pub type MultiVersionIdentity = MultiVersionContainer<Identity>;
pub type MultiVersionRole = MultiVersionContainer<Role>;
pub type MultiVersionGrantedRole = MultiVersionContainer<GrantedRole>;
pub type MultiVersionPolicy = MultiVersionContainer<Policy>;
pub type MultiVersionSource = MultiVersionContainer<Source>;
pub type MultiVersionSink = MultiVersionContainer<Sink>;
pub type MultiVersionRowTtl = MultiVersionContainer<Ttl>;
pub type MultiVersionConfig = MultiVersionContainer<Value>;
pub type MultiVersionAuthentication = MultiVersionContainer<Authentication>;

#[derive(Debug, Clone)]
pub struct CatalogCache(Arc<CatalogCacheInner>);

#[derive(Debug)]
pub struct CatalogCacheInner {
	pub(crate) bindings: SkipMap<BindingId, MultiVersionBinding>,

	pub(crate) bindings_by_procedure: SkipMap<ProcedureId, Vec<BindingId>>,

	pub(crate) bindings_by_name: SkipMap<(NamespaceId, String), BindingId>,

	pub(crate) bindings_by_grpc_name: SkipMap<String, BindingId>,

	pub(crate) bindings_by_ws_name: SkipMap<String, BindingId>,

	pub(crate) bindings_http: SkipMap<BindingId, ()>,

	pub(crate) bindings_by_http_method_path: SkipMap<(String, String), BindingId>,

	pub(crate) configs: SkipMap<ConfigKey, MultiVersionConfig>,

	pub(crate) namespaces: SkipMap<NamespaceId, MultiVersionNamespace>,

	pub(crate) namespaces_by_name: SkipMap<String, NamespaceId>,

	pub(crate) tables: SkipMap<TableId, MultiVersionTable>,

	pub(crate) tables_by_name: SkipMap<(NamespaceId, String), TableId>,

	pub(crate) views: SkipMap<ViewId, MultiVersionView>,

	pub(crate) views_by_name: SkipMap<(NamespaceId, String), ViewId>,

	pub(crate) flows: SkipMap<FlowId, MultiVersionFlow>,

	pub(crate) flows_by_name: SkipMap<(NamespaceId, String), FlowId>,

	pub(crate) procedures: SkipMap<ProcedureId, MultiVersionProcedure>,

	pub(crate) procedures_by_name: SkipMap<(NamespaceId, String), ProcedureId>,

	pub(crate) procedures_by_variant: SkipMap<VariantRef, Vec<ProcedureId>>,

	pub(crate) tests: SkipMap<TestId, MultiVersionTest>,

	pub(crate) tests_by_name: SkipMap<(NamespaceId, String), TestId>,

	pub(crate) primary_keys: SkipMap<PrimaryKeyId, MultiVersionPrimaryKey>,

	pub(crate) shape_retention_strategies: SkipMap<ShapeId, MultiVersionRetentionStrategy>,

	pub(crate) operator_retention_strategies: SkipMap<FlowNodeId, MultiVersionRetentionStrategy>,

	pub(crate) row_ttls: SkipMap<ShapeId, MultiVersionRowTtl>,

	pub(crate) operator_ttls: SkipMap<FlowNodeId, MultiVersionRowTtl>,

	pub(crate) dictionaries: SkipMap<DictionaryId, MultiVersionDictionary>,

	pub(crate) dictionaries_by_name: SkipMap<(NamespaceId, String), DictionaryId>,

	pub(crate) sumtypes: SkipMap<SumTypeId, MultiVersionSumType>,

	pub(crate) sumtypes_by_name: SkipMap<(NamespaceId, String), SumTypeId>,

	pub(crate) ringbuffers: SkipMap<RingBufferId, MultiVersionRingBuffer>,

	pub(crate) ringbuffers_by_name: SkipMap<(NamespaceId, String), RingBufferId>,

	pub(crate) series: SkipMap<SeriesId, MultiVersionSeries>,

	pub(crate) series_by_name: SkipMap<(NamespaceId, String), SeriesId>,

	pub(crate) handlers: SkipMap<HandlerId, MultiVersionHandler>,

	pub(crate) handlers_by_name: SkipMap<(NamespaceId, String), HandlerId>,

	pub(crate) handlers_by_variant: SkipMap<VariantRef, Vec<HandlerId>>,

	pub(crate) identities: SkipMap<IdentityId, MultiVersionIdentity>,

	pub(crate) identities_by_name: SkipMap<String, IdentityId>,

	pub(crate) roles: SkipMap<RoleId, MultiVersionRole>,

	pub(crate) roles_by_name: SkipMap<String, RoleId>,

	pub(crate) granted_roles: SkipMap<(IdentityId, RoleId), MultiVersionGrantedRole>,

	pub(crate) authentications: SkipMap<AuthenticationId, MultiVersionAuthentication>,

	pub(crate) authentications_by_identity_method: SkipMap<(IdentityId, String), AuthenticationId>,

	pub(crate) policies: SkipMap<PolicyId, MultiVersionPolicy>,

	pub(crate) policies_by_name: SkipMap<String, PolicyId>,

	pub(crate) policy_operations: SkipMap<PolicyId, Vec<PolicyOperation>>,

	pub(crate) migrations: SkipMap<MigrationId, MultiVersionMigration>,

	pub(crate) migrations_by_name: SkipMap<String, MigrationId>,

	pub(crate) migration_events: SkipMap<MigrationEventId, MultiVersionMigrationEvent>,

	pub(crate) sources: SkipMap<SourceId, MultiVersionSource>,

	pub(crate) sources_by_name: SkipMap<(NamespaceId, String), SourceId>,

	pub(crate) sinks: SkipMap<SinkId, MultiVersionSink>,

	pub(crate) sinks_by_name: SkipMap<(NamespaceId, String), SinkId>,

	pub(crate) vtable_user: SkipMap<VTableId, Arc<VTable>>,

	pub(crate) vtable_user_by_name: SkipMap<(NamespaceId, String), VTableId>,

	pub(crate) row_shapes: SkipMap<RowShapeFingerprint, RowShape>,
}

impl ops::Deref for CatalogCache {
	type Target = CatalogCacheInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Default for CatalogCache {
	fn default() -> Self {
		Self::new()
	}
}

impl CatalogCache {
	pub fn new() -> Self {
		let system_namespace = Namespace::system();
		let system_namespace_id = system_namespace.id();

		let namespaces = SkipMap::new();
		let container = MultiVersionContainer::new();
		container.insert(1, system_namespace);
		namespaces.insert(system_namespace_id, container);

		let default_namespace = Namespace::default_namespace();
		let default_namespace_id = default_namespace.id();
		let container = MultiVersionContainer::new();
		container.insert(1, default_namespace);
		namespaces.insert(default_namespace_id, container);

		let namespaces_by_name = SkipMap::new();
		namespaces_by_name.insert("system".to_string(), system_namespace_id);
		namespaces_by_name.insert("default".to_string(), default_namespace_id);

		let inner = CatalogCacheInner {
			bindings: SkipMap::new(),
			bindings_by_procedure: SkipMap::new(),
			bindings_by_name: SkipMap::new(),
			bindings_by_grpc_name: SkipMap::new(),
			bindings_by_ws_name: SkipMap::new(),
			bindings_http: SkipMap::new(),
			bindings_by_http_method_path: SkipMap::new(),
			configs: SkipMap::new(),
			namespaces,
			namespaces_by_name,
			procedures: SkipMap::new(),
			procedures_by_name: SkipMap::new(),
			procedures_by_variant: SkipMap::new(),
			tests: SkipMap::new(),
			tests_by_name: SkipMap::new(),
			tables: SkipMap::new(),
			tables_by_name: SkipMap::new(),
			views: SkipMap::new(),
			views_by_name: SkipMap::new(),
			flows: SkipMap::new(),
			flows_by_name: SkipMap::new(),
			primary_keys: SkipMap::new(),
			shape_retention_strategies: SkipMap::new(),
			operator_retention_strategies: SkipMap::new(),
			row_ttls: SkipMap::new(),
			operator_ttls: SkipMap::new(),
			dictionaries: SkipMap::new(),
			dictionaries_by_name: SkipMap::new(),
			sumtypes: SkipMap::new(),
			sumtypes_by_name: SkipMap::new(),
			ringbuffers: SkipMap::new(),
			ringbuffers_by_name: SkipMap::new(),
			series: SkipMap::new(),
			series_by_name: SkipMap::new(),
			handlers: SkipMap::new(),
			handlers_by_name: SkipMap::new(),
			handlers_by_variant: SkipMap::new(),
			identities: SkipMap::new(),
			identities_by_name: SkipMap::new(),
			roles: SkipMap::new(),
			roles_by_name: SkipMap::new(),
			granted_roles: SkipMap::new(),
			authentications: SkipMap::new(),
			authentications_by_identity_method: SkipMap::new(),
			policies: SkipMap::new(),
			policies_by_name: SkipMap::new(),
			policy_operations: SkipMap::new(),
			migrations: SkipMap::new(),
			migrations_by_name: SkipMap::new(),
			migration_events: SkipMap::new(),
			sources: SkipMap::new(),
			sources_by_name: SkipMap::new(),
			sinks: SkipMap::new(),
			sinks_by_name: SkipMap::new(),
			vtable_user: SkipMap::new(),
			vtable_user_by_name: SkipMap::new(),
			row_shapes: SkipMap::new(),
		};

		Self(Arc::new(inner))
	}

	pub fn register_vtable_user(&self, def: Arc<VTable>) -> Result<()> {
		let key = (def.namespace, def.name.clone());

		if self.vtable_user_by_name.contains_key(&key) {
			let ns_name = self
				.namespaces
				.get(&def.namespace)
				.map(|e| e.value().get_latest().map(|n| n.name().to_string()).unwrap_or_default())
				.unwrap_or_else(|| format!("{}", def.namespace.0));
			return Err(CatalogError::AlreadyExists {
				kind: CatalogObjectKind::VirtualTable,
				namespace: ns_name,
				name: def.name.clone(),
				fragment: Fragment::None,
			}
			.into());
		}

		self.vtable_user.insert(def.id, def.clone());
		self.vtable_user_by_name.insert(key, def.id);
		Ok(())
	}

	pub fn unregister_vtable_user(&self, namespace: NamespaceId, name: &str) -> Result<()> {
		let key = (namespace, name.to_string());

		if let Some(entry) = self.vtable_user_by_name.remove(&key) {
			self.vtable_user.remove(entry.value());
			Ok(())
		} else {
			let ns_name = self
				.namespaces
				.get(&namespace)
				.map(|e| e.value().get_latest().map(|n| n.name().to_string()).unwrap_or_default())
				.unwrap_or_else(|| format!("{}", namespace.0));
			Err(CatalogError::NotFound {
				kind: CatalogObjectKind::VirtualTable,
				namespace: ns_name,
				name: name.to_string(),
				fragment: Fragment::None,
			}
			.into())
		}
	}

	pub fn find_vtable_user_by_name(&self, namespace: NamespaceId, name: &str) -> Option<Arc<VTable>> {
		let key = (namespace, name.to_string());
		self.vtable_user_by_name
			.get(&key)
			.and_then(|entry| self.vtable_user.get(entry.value()).map(|e| e.value().clone()))
	}

	pub fn find_vtable_user(&self, id: VTableId) -> Option<Arc<VTable>> {
		self.vtable_user.get(&id).map(|e| e.value().clone())
	}

	pub fn list_vtable_user_in_namespace(&self, namespace: NamespaceId) -> Vec<Arc<VTable>> {
		self.vtable_user
			.iter()
			.filter(|e| e.value().namespace == namespace)
			.map(|e| e.value().clone())
			.collect()
	}

	pub fn list_vtable_user_all(&self) -> Vec<Arc<VTable>> {
		self.vtable_user.iter().map(|e| e.value().clone()).collect()
	}

	pub fn get_config_at(&self, key: ConfigKey, version: CommitVersion) -> Value {
		self.0.configs
			.get(&key)
			.and_then(|entry| entry.value().get(version))
			.unwrap_or_else(|| key.default_value())
	}

	pub fn get_config(&self, key: ConfigKey) -> Value {
		self.0.configs
			.get(&key)
			.and_then(|entry| entry.value().get_latest())
			.unwrap_or_else(|| key.default_value())
	}

	pub fn list_configs_at(&self, version: CommitVersion) -> Vec<Config> {
		ConfigKey::all()
			.iter()
			.map(|&key| Config {
				key,
				value: self.get_config_at(key, version),
				default_value: key.default_value(),
				description: key.description(),
				requires_restart: key.requires_restart(),
			})
			.collect()
	}

	pub fn set_config(&self, key: ConfigKey, version: CommitVersion, value: Value) -> Result<()> {
		let value = key.accept(value).map_err(|e| CatalogError::from((key, e)))?;

		let entry = self.0.configs.get_or_insert_with(key, MultiVersionContainer::new);
		entry.value().insert(version, value);
		Ok(())
	}
}

impl GetConfig for CatalogCache {
	fn get_config(&self, key: ConfigKey) -> Value {
		self.get_config(key)
	}

	fn get_config_at(&self, key: ConfigKey, version: CommitVersion) -> Value {
		self.get_config_at(key, version)
	}
}

#[cfg(test)]
mod config_validation_tests {
	use std::time::Duration as StdDuration;

	use reifydb_core::interface::catalog::config::{ConfigKey, GetConfig};
	use reifydb_type::value::{Value, duration::Duration as TypeDuration, r#type::Type};

	use super::{CatalogCache, CatalogError, CommitVersion};

	#[test]
	fn test_set_cdc_ttl_zero_is_rejected() {
		let catalog = CatalogCache::new();
		let zero = Value::Duration(TypeDuration::from_seconds(0).unwrap());

		let err = catalog.set_config(ConfigKey::CdcTtlDuration, CommitVersion(1), zero).unwrap_err();
		let msg = format!("{err}");
		assert!(msg.contains("CDC_TTL_DURATION"), "expected key in error: {msg}");
		assert!(msg.contains("greater than zero"), "expected reason in error: {msg}");

		// Default (typed-null) is preserved when set fails.
		assert!(matches!(
			catalog.get_config(ConfigKey::CdcTtlDuration),
			Value::None {
				inner: Type::Duration
			}
		));
	}

	#[test]
	fn test_set_cdc_ttl_negative_is_rejected() {
		let catalog = CatalogCache::new();
		let negative = Value::Duration(TypeDuration::from_seconds(-30).unwrap());
		let err = catalog.set_config(ConfigKey::CdcTtlDuration, CommitVersion(1), negative).unwrap_err();
		assert_eq!(err.code, "CA_053");
	}

	#[test]
	fn test_set_cdc_ttl_positive_is_accepted_and_visible() {
		let catalog = CatalogCache::new();
		let ten_sec = Value::Duration(TypeDuration::from_seconds(10).unwrap());

		catalog.set_config(ConfigKey::CdcTtlDuration, CommitVersion(1), ten_sec.clone()).unwrap();
		assert_eq!(catalog.get_config(ConfigKey::CdcTtlDuration), ten_sec);

		let opt = catalog.get_config_duration_opt(ConfigKey::CdcTtlDuration);
		assert_eq!(opt, Some(StdDuration::from_secs(10)));
	}

	#[test]
	fn test_set_cdc_ttl_to_typed_null_is_accepted() {
		// Operators can "unset" the TTL by writing Value::None - restoring forever-retention.
		let catalog = CatalogCache::new();
		catalog.set_config(
			ConfigKey::CdcTtlDuration,
			CommitVersion(1),
			Value::Duration(TypeDuration::from_seconds(30).unwrap()),
		)
		.unwrap();

		catalog.set_config(
			ConfigKey::CdcTtlDuration,
			CommitVersion(2),
			Value::None {
				inner: Type::Duration,
			},
		)
		.unwrap();

		assert_eq!(catalog.get_config_duration_opt(ConfigKey::CdcTtlDuration), None);
	}

	#[test]
	fn test_set_cdc_ttl_wrong_type_returns_type_mismatch_not_validate_error() {
		// A non-numeric, non-Duration value cannot coerce to Type::Duration
		// and must surface as ConfigTypeMismatch.
		let catalog = CatalogCache::new();
		let bad = Value::Boolean(true);
		let err = catalog.set_config(ConfigKey::CdcTtlDuration, CommitVersion(1), bad).unwrap_err();
		assert_eq!(err.code, "CA_052", "expected ConfigTypeMismatch (CA_052)");
	}

	// Sanity: keys without bespoke validation still accept zero-Duration values.
	#[test]
	fn test_row_ttl_scan_interval_accepts_zero() {
		let catalog = CatalogCache::new();
		let zero = Value::Duration(TypeDuration::from_seconds(0).unwrap());
		assert!(catalog.set_config(ConfigKey::RowTtlScanInterval, CommitVersion(1), zero).is_ok());
	}

	#[allow(dead_code)]
	fn _force_use(_: CatalogError) {}
}
