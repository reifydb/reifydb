// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

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
	row::RowTtl,
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
pub type MultiVersionRowTtl = MultiVersionContainer<RowTtl>;
pub type MultiVersionConfig = MultiVersionContainer<Value>;
pub type MultiVersionAuthentication = MultiVersionContainer<Authentication>;

/// A materialized catalog that stores multi namespace, store::table, and view
/// definitions. This provides fast O(1) lookups for catalog metadata without
/// hitting storage.
#[derive(Debug, Clone)]
pub struct MaterializedCatalog(Arc<MaterializedCatalogInner>);

#[derive(Debug)]
pub struct MaterializedCatalogInner {
	/// MultiVersion binding definitions indexed by binding ID
	pub(crate) bindings: SkipMap<BindingId, MultiVersionBinding>,
	/// Index from procedure ID to binding IDs for fast procedure->binding lookups
	pub(crate) bindings_by_procedure: SkipMap<ProcedureId, Vec<BindingId>>,
	/// Runtime configuration registry (shared with the oracle)
	pub(crate) configs: SkipMap<ConfigKey, MultiVersionConfig>,
	/// MultiVersion namespace definitions indexed by namespace ID
	pub(crate) namespaces: SkipMap<NamespaceId, MultiVersionNamespace>,
	/// Index from namespace name to namespace ID for fast name lookups
	pub(crate) namespaces_by_name: SkipMap<String, NamespaceId>,
	/// MultiVersion table definitions indexed by table ID
	pub(crate) tables: SkipMap<TableId, MultiVersionTable>,
	/// Index from (namespace_id, table_name) to table ID for fast name lookups
	pub(crate) tables_by_name: SkipMap<(NamespaceId, String), TableId>,
	/// MultiVersion view definitions indexed by view ID
	pub(crate) views: SkipMap<ViewId, MultiVersionView>,
	/// Index from (namespace_id, view_name) to view ID for fast name lookups
	pub(crate) views_by_name: SkipMap<(NamespaceId, String), ViewId>,
	/// MultiVersion flow definitions indexed by flow ID
	pub(crate) flows: SkipMap<FlowId, MultiVersionFlow>,
	/// Index from (namespace_id, flow_name) to flow ID for fast name lookups
	pub(crate) flows_by_name: SkipMap<(NamespaceId, String), FlowId>,
	/// MultiVersion procedure definitions indexed by procedure ID
	pub(crate) procedures: SkipMap<ProcedureId, MultiVersionProcedure>,
	/// Index from (namespace_id, procedure_name) to procedure ID for fast name lookups
	pub(crate) procedures_by_name: SkipMap<(NamespaceId, String), ProcedureId>,
	/// Index from variant ref to Vec<ProcedureId> for procedure dispatch
	pub(crate) procedures_by_variant: SkipMap<VariantRef, Vec<ProcedureId>>,
	/// MultiVersion test definitions indexed by test ID
	pub(crate) tests: SkipMap<TestId, MultiVersionTest>,
	/// Index from (namespace_id, test_name) to test ID for fast name lookups
	pub(crate) tests_by_name: SkipMap<(NamespaceId, String), TestId>,
	/// MultiVersion primary key definitions indexed by primary key ID
	pub(crate) primary_keys: SkipMap<PrimaryKeyId, MultiVersionPrimaryKey>,
	/// MultiVersion source retention strategies indexed by source ID
	pub(crate) shape_retention_strategies: SkipMap<ShapeId, MultiVersionRetentionStrategy>,
	/// MultiVersion operator retention strategies indexed by operator ID
	pub(crate) operator_retention_strategies: SkipMap<FlowNodeId, MultiVersionRetentionStrategy>,
	/// MultiVersion TTL configurations indexed by shape ID
	pub(crate) row_ttls: SkipMap<ShapeId, MultiVersionRowTtl>,
	/// MultiVersion dictionary definitions indexed by dictionary ID
	pub(crate) dictionaries: SkipMap<DictionaryId, MultiVersionDictionary>,
	/// Index from (namespace_id, dictionary_name) to dictionary ID for fast name lookups
	pub(crate) dictionaries_by_name: SkipMap<(NamespaceId, String), DictionaryId>,
	/// MultiVersion sum type definitions indexed by sum type ID
	pub(crate) sumtypes: SkipMap<SumTypeId, MultiVersionSumType>,
	/// Index from (namespace_id, sumtype_name) to sum type ID for fast name lookups
	pub(crate) sumtypes_by_name: SkipMap<(NamespaceId, String), SumTypeId>,
	/// MultiVersion ringbuffer definitions indexed by ringbuffer ID
	pub(crate) ringbuffers: SkipMap<RingBufferId, MultiVersionRingBuffer>,
	/// Index from (namespace_id, ringbuffer_name) to ringbuffer ID for fast name lookups
	pub(crate) ringbuffers_by_name: SkipMap<(NamespaceId, String), RingBufferId>,
	/// MultiVersion series definitions indexed by series ID
	pub(crate) series: SkipMap<SeriesId, MultiVersionSeries>,
	/// Index from (namespace_id, series_name) to series ID for fast name lookups
	pub(crate) series_by_name: SkipMap<(NamespaceId, String), SeriesId>,
	/// MultiVersion handler definitions indexed by handler ID
	pub(crate) handlers: SkipMap<HandlerId, MultiVersionHandler>,
	/// Index from (namespace_id, handler_name) to handler ID for fast name lookups
	pub(crate) handlers_by_name: SkipMap<(NamespaceId, String), HandlerId>,
	/// Index from variant ref to Vec<HandlerId> for dispatch hot-path
	pub(crate) handlers_by_variant: SkipMap<VariantRef, Vec<HandlerId>>,
	/// MultiVersion identity definitions indexed by IdentityId
	pub(crate) identities: SkipMap<IdentityId, MultiVersionIdentity>,
	/// Index from identity name to IdentityId for fast name lookups
	pub(crate) identities_by_name: SkipMap<String, IdentityId>,
	/// MultiVersion role definitions indexed by role ID
	pub(crate) roles: SkipMap<RoleId, MultiVersionRole>,
	/// Index from role name to role ID for fast name lookups
	pub(crate) roles_by_name: SkipMap<String, RoleId>,
	/// MultiVersion granted-role definitions indexed by (identity_id, role_id)
	pub(crate) granted_roles: SkipMap<(IdentityId, RoleId), MultiVersionGrantedRole>,
	/// MultiVersion authentication definitions indexed by AuthenticationId
	pub(crate) authentications: SkipMap<AuthenticationId, MultiVersionAuthentication>,
	/// Index from (identity_id, method) to AuthenticationId for fast lookups
	pub(crate) authentications_by_identity_method: SkipMap<(IdentityId, String), AuthenticationId>,
	/// MultiVersion policy definitions indexed by policy ID
	pub(crate) policies: SkipMap<PolicyId, MultiVersionPolicy>,
	/// Index from policy name to policy ID for fast name lookups
	pub(crate) policies_by_name: SkipMap<String, PolicyId>,
	/// Policy operations indexed by policy ID for fast lookups (avoids KV store scans)
	pub(crate) policy_operations: SkipMap<PolicyId, Vec<PolicyOperation>>,
	/// MultiVersion migration definitions indexed by migration ID
	pub(crate) migrations: SkipMap<MigrationId, MultiVersionMigration>,
	/// Index from migration name to migration ID for fast name lookups
	pub(crate) migrations_by_name: SkipMap<String, MigrationId>,
	/// MultiVersion migration events indexed by event ID
	pub(crate) migration_events: SkipMap<MigrationEventId, MultiVersionMigrationEvent>,
	/// MultiVersion source definitions indexed by source ID
	pub(crate) sources: SkipMap<SourceId, MultiVersionSource>,
	/// Index from (namespace_id, source_name) to source ID for fast name lookups
	pub(crate) sources_by_name: SkipMap<(NamespaceId, String), SourceId>,
	/// MultiVersion sink definitions indexed by sink ID
	pub(crate) sinks: SkipMap<SinkId, MultiVersionSink>,
	/// Index from (namespace_id, sink_name) to sink ID for fast name lookups
	pub(crate) sinks_by_name: SkipMap<(NamespaceId, String), SinkId>,
	/// User-defined virtual table definitions indexed by ID
	pub(crate) vtable_user: SkipMap<VTableId, Arc<VTable>>,
	/// Index from (namespace_id, table_name) to virtual table ID for fast name lookups
	pub(crate) vtable_user_by_name: SkipMap<(NamespaceId, String), VTableId>,
	/// Content-addressed row shapes indexed by fingerprint
	pub(crate) row_shapes: SkipMap<RowShapeFingerprint, RowShape>,
}

impl ops::Deref for MaterializedCatalog {
	type Target = MaterializedCatalogInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Default for MaterializedCatalog {
	fn default() -> Self {
		Self::new()
	}
}

impl MaterializedCatalog {
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

		let inner = MaterializedCatalogInner {
			bindings: SkipMap::new(),
			bindings_by_procedure: SkipMap::new(),
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

	/// Register a user-defined virtual table
	///
	/// Returns an error if a virtual table with the same name already exists in the namespace.
	pub fn register_vtable_user(&self, def: Arc<VTable>) -> Result<()> {
		let key = (def.namespace, def.name.clone());

		// Check if already exists
		if self.vtable_user_by_name.contains_key(&key) {
			// Get namespace name for error message
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

	/// Unregister a user-defined virtual table by namespace and name
	pub fn unregister_vtable_user(&self, namespace: NamespaceId, name: &str) -> Result<()> {
		let key = (namespace, name.to_string());

		if let Some(entry) = self.vtable_user_by_name.remove(&key) {
			self.vtable_user.remove(entry.value());
			Ok(())
		} else {
			// Get namespace name for error message
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

	/// Find a user-defined virtual table by namespace and name
	pub fn find_vtable_user_by_name(&self, namespace: NamespaceId, name: &str) -> Option<Arc<VTable>> {
		let key = (namespace, name.to_string());
		self.vtable_user_by_name
			.get(&key)
			.and_then(|entry| self.vtable_user.get(entry.value()).map(|e| e.value().clone()))
	}

	/// Find a user-defined virtual table by ID
	pub fn find_vtable_user(&self, id: VTableId) -> Option<Arc<VTable>> {
		self.vtable_user.get(&id).map(|e| e.value().clone())
	}

	/// List all user-defined virtual tables in a namespace
	pub fn list_vtable_user_in_namespace(&self, namespace: NamespaceId) -> Vec<Arc<VTable>> {
		self.vtable_user
			.iter()
			.filter(|e| e.value().namespace == namespace)
			.map(|e| e.value().clone())
			.collect()
	}

	/// List all user-defined virtual tables
	pub fn list_vtable_user_all(&self) -> Vec<Arc<VTable>> {
		self.vtable_user.iter().map(|e| e.value().clone()).collect()
	}

	/// Get a configuration value at a specific version.
	pub fn get_config_at(&self, key: ConfigKey, version: CommitVersion) -> Value {
		self.0.configs
			.get(&key)
			.and_then(|entry| entry.value().get(version))
			.unwrap_or_else(|| key.default_value())
	}

	/// Get the latest configuration value.
	pub fn get_config(&self, key: ConfigKey) -> Value {
		self.0.configs
			.get(&key)
			.and_then(|entry| entry.value().get_latest())
			.unwrap_or_else(|| key.default_value())
	}

	/// List all configurations at a specific version.
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

	/// Set a new value for a configuration at a given version.
	pub fn set_config(&self, key: ConfigKey, version: CommitVersion, value: Value) -> Result<()> {
		let expected_types = key.expected_types();
		if !expected_types.contains(&value.get_type()) {
			return Err(CatalogError::ConfigTypeMismatch {
				key: key.to_string(),
				expected: expected_types.to_vec(),
				actual: value.get_type(),
			}
			.into());
		}

		let entry = self.0.configs.get_or_insert_with(key, MultiVersionContainer::new);
		entry.value().insert(version, value);
		Ok(())
	}
}

impl GetConfig for MaterializedCatalog {
	fn get_config(&self, key: ConfigKey) -> Value {
		self.get_config(key)
	}

	fn get_config_at(&self, key: ConfigKey, version: CommitVersion) -> Value {
		self.get_config_at(key, version)
	}
}
