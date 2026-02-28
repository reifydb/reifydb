// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod dictionary;
pub mod flow;
pub mod handler;
pub mod load;
pub mod migration;
pub mod namespace;
pub mod operator_retention_policy;
pub mod policy;
pub mod primary_key;
pub mod primitive_retention_policy;
pub mod procedure;
pub mod ringbuffer;
pub mod role;
pub mod subscription;
pub mod sumtype;
pub mod table;
pub mod user;
pub mod user_role;
pub mod view;

use std::sync::Arc;

use crossbeam_skiplist::SkipMap;
use reifydb_core::{
	interface::catalog::{
		dictionary::DictionaryDef,
		flow::{FlowDef, FlowId, FlowNodeId},
		handler::HandlerDef,
		id::{
			HandlerId, MigrationEventId, MigrationId, NamespaceId, PrimaryKeyId, ProcedureId, RingBufferId,
			SubscriptionId, TableId, ViewId,
		},
		key::PrimaryKeyDef,
		migration::{MigrationDef, MigrationEvent},
		namespace::NamespaceDef,
		policy::{PolicyDef, PolicyId},
		primitive::PrimitiveId,
		procedure::ProcedureDef,
		ringbuffer::RingBufferDef,
		subscription::SubscriptionDef,
		sumtype::SumTypeDef,
		table::TableDef,
		user::{RoleDef, RoleId, UserDef, UserId, UserRoleDef},
		view::ViewDef,
		vtable::{VTableDef, VTableId},
	},
	retention::RetentionPolicy,
	util::multi::MultiVersionContainer,
};
use reifydb_type::{
	fragment::Fragment,
	value::{dictionary::DictionaryId, identity::IdentityId, sumtype::SumTypeId},
};

use crate::{
	Result,
	error::{CatalogError, CatalogObjectKind},
};

pub type MultiVersionNamespaceDef = MultiVersionContainer<NamespaceDef>;
pub type MultiVersionTableDef = MultiVersionContainer<TableDef>;
pub type MultiVersionViewDef = MultiVersionContainer<ViewDef>;
pub type MultiVersionFlowDef = MultiVersionContainer<FlowDef>;
pub type MultiVersionPrimaryKeyDef = MultiVersionContainer<PrimaryKeyDef>;
pub type MultiVersionRetentionPolicy = MultiVersionContainer<RetentionPolicy>;
pub type MultiVersionDictionaryDef = MultiVersionContainer<DictionaryDef>;
pub type MultiVersionHandlerDef = MultiVersionContainer<HandlerDef>;
pub type MultiVersionMigrationDef = MultiVersionContainer<MigrationDef>;
pub type MultiVersionMigrationEvent = MultiVersionContainer<MigrationEvent>;
pub type MultiVersionProcedureDef = MultiVersionContainer<ProcedureDef>;
pub type MultiVersionRingBufferDef = MultiVersionContainer<RingBufferDef>;
pub type MultiVersionSumTypeDef = MultiVersionContainer<SumTypeDef>;
pub type MultiVersionSubscriptionDef = MultiVersionContainer<SubscriptionDef>;
pub type MultiVersionUserDef = MultiVersionContainer<UserDef>;
pub type MultiVersionRoleDef = MultiVersionContainer<RoleDef>;
pub type MultiVersionUserRoleDef = MultiVersionContainer<UserRoleDef>;
pub type MultiVersionPolicyDef = MultiVersionContainer<PolicyDef>;

/// A materialized catalog that stores multi namespace, store::table, and view
/// definitions. This provides fast O(1) lookups for catalog metadata without
/// hitting storage.
#[derive(Debug, Clone)]
pub struct MaterializedCatalog(Arc<MaterializedCatalogInner>);

#[derive(Debug)]
pub struct MaterializedCatalogInner {
	/// MultiVersion namespace definitions indexed by namespace ID
	pub(crate) namespaces: SkipMap<NamespaceId, MultiVersionNamespaceDef>,
	/// Index from namespace name to namespace ID for fast name lookups
	pub(crate) namespaces_by_name: SkipMap<String, NamespaceId>,
	/// MultiVersion table definitions indexed by table ID
	pub(crate) tables: SkipMap<TableId, MultiVersionTableDef>,
	/// Index from (namespace_id, table_name) to table ID for fast name lookups
	pub(crate) tables_by_name: SkipMap<(NamespaceId, String), TableId>,
	/// MultiVersion view definitions indexed by view ID
	pub(crate) views: SkipMap<ViewId, MultiVersionViewDef>,
	/// Index from (namespace_id, view_name) to view ID for fast name lookups
	pub(crate) views_by_name: SkipMap<(NamespaceId, String), ViewId>,
	/// MultiVersion flow definitions indexed by flow ID
	pub(crate) flows: SkipMap<FlowId, MultiVersionFlowDef>,
	/// Index from (namespace_id, flow_name) to flow ID for fast name lookups
	pub(crate) flows_by_name: SkipMap<(NamespaceId, String), FlowId>,
	/// MultiVersion procedure definitions indexed by procedure ID
	pub(crate) procedures: SkipMap<ProcedureId, MultiVersionProcedureDef>,
	/// Index from (namespace_id, procedure_name) to procedure ID for fast name lookups
	pub(crate) procedures_by_name: SkipMap<(NamespaceId, String), ProcedureId>,
	/// Index from (sumtype_id, variant_tag) to Vec<ProcedureId> for procedure dispatch
	pub(crate) procedures_by_variant: SkipMap<(SumTypeId, u8), Vec<ProcedureId>>,
	/// MultiVersion primary key definitions indexed by primary key ID
	pub(crate) primary_keys: SkipMap<PrimaryKeyId, MultiVersionPrimaryKeyDef>,
	/// MultiVersion source retention policies indexed by source ID
	pub(crate) source_retention_policies: SkipMap<PrimitiveId, MultiVersionRetentionPolicy>,
	/// MultiVersion operator retention policies indexed by operator ID
	pub(crate) operator_retention_policies: SkipMap<FlowNodeId, MultiVersionRetentionPolicy>,
	/// MultiVersion dictionary definitions indexed by dictionary ID
	pub(crate) dictionaries: SkipMap<DictionaryId, MultiVersionDictionaryDef>,
	/// Index from (namespace_id, dictionary_name) to dictionary ID for fast name lookups
	pub(crate) dictionaries_by_name: SkipMap<(NamespaceId, String), DictionaryId>,
	/// MultiVersion sum type definitions indexed by sum type ID
	pub(crate) sumtypes: SkipMap<SumTypeId, MultiVersionSumTypeDef>,
	/// Index from (namespace_id, sumtype_name) to sum type ID for fast name lookups
	pub(crate) sumtypes_by_name: SkipMap<(NamespaceId, String), SumTypeId>,
	/// MultiVersion ringbuffer definitions indexed by ringbuffer ID
	pub(crate) ringbuffers: SkipMap<RingBufferId, MultiVersionRingBufferDef>,
	/// Index from (namespace_id, ringbuffer_name) to ringbuffer ID for fast name lookups
	pub(crate) ringbuffers_by_name: SkipMap<(NamespaceId, String), RingBufferId>,
	/// MultiVersion subscription definitions indexed by subscription ID
	/// Note: Subscriptions do NOT have names - they are identified only by ID
	pub(crate) subscriptions: SkipMap<SubscriptionId, MultiVersionSubscriptionDef>,
	/// MultiVersion handler definitions indexed by handler ID
	pub(crate) handlers: SkipMap<HandlerId, MultiVersionHandlerDef>,
	/// Index from (namespace_id, handler_name) to handler ID for fast name lookups
	pub(crate) handlers_by_name: SkipMap<(NamespaceId, String), HandlerId>,
	/// Index from (sumtype_id, variant_tag) to Vec<HandlerId> for dispatch hot-path
	pub(crate) handlers_by_variant: SkipMap<(SumTypeId, u8), Vec<HandlerId>>,
	/// MultiVersion user definitions indexed by user ID
	pub(crate) users: SkipMap<UserId, MultiVersionUserDef>,
	/// Index from user name to user ID for fast name lookups
	pub(crate) users_by_name: SkipMap<String, UserId>,
	/// Index from identity ID to user ID for fast identity lookups
	pub(crate) users_by_identity: SkipMap<IdentityId, UserId>,
	/// MultiVersion role definitions indexed by role ID
	pub(crate) roles: SkipMap<RoleId, MultiVersionRoleDef>,
	/// Index from role name to role ID for fast name lookups
	pub(crate) roles_by_name: SkipMap<String, RoleId>,
	/// MultiVersion user-role definitions indexed by (user_id, role_id)
	pub(crate) user_roles: SkipMap<(UserId, RoleId), MultiVersionUserRoleDef>,
	/// MultiVersion policy definitions indexed by policy ID
	pub(crate) policies: SkipMap<PolicyId, MultiVersionPolicyDef>,
	/// Index from policy name to policy ID for fast name lookups
	pub(crate) policies_by_name: SkipMap<String, PolicyId>,
	/// MultiVersion migration definitions indexed by migration ID
	pub(crate) migrations: SkipMap<MigrationId, MultiVersionMigrationDef>,
	/// Index from migration name to migration ID for fast name lookups
	pub(crate) migrations_by_name: SkipMap<String, MigrationId>,
	/// MultiVersion migration events indexed by event ID
	pub(crate) migration_events: SkipMap<MigrationEventId, MultiVersionMigrationEvent>,
	/// User-defined virtual table definitions indexed by ID
	pub(crate) vtable_user: SkipMap<VTableId, Arc<VTableDef>>,
	/// Index from (namespace_id, table_name) to virtual table ID for fast name lookups
	pub(crate) vtable_user_by_name: SkipMap<(NamespaceId, String), VTableId>,
}

impl std::ops::Deref for MaterializedCatalog {
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
		let system_namespace = NamespaceDef::system();
		let system_namespace_id = system_namespace.id;

		let namespaces = SkipMap::new();
		let container = MultiVersionContainer::new();
		container.insert(1, system_namespace);
		namespaces.insert(system_namespace_id, container);

		let default_namespace = NamespaceDef::default_namespace();
		let default_namespace_id = default_namespace.id;
		let default_container = MultiVersionContainer::new();
		default_container.insert(1, default_namespace);
		namespaces.insert(default_namespace_id, default_container);

		let namespaces_by_name = SkipMap::new();
		namespaces_by_name.insert("system".to_string(), system_namespace_id);
		namespaces_by_name.insert("default".to_string(), default_namespace_id);

		Self(Arc::new(MaterializedCatalogInner {
			namespaces,
			namespaces_by_name,
			procedures: SkipMap::new(),
			procedures_by_name: SkipMap::new(),
			procedures_by_variant: SkipMap::new(),
			tables: SkipMap::new(),
			tables_by_name: SkipMap::new(),
			views: SkipMap::new(),
			views_by_name: SkipMap::new(),
			flows: SkipMap::new(),
			flows_by_name: SkipMap::new(),
			primary_keys: SkipMap::new(),
			source_retention_policies: SkipMap::new(),
			operator_retention_policies: SkipMap::new(),
			dictionaries: SkipMap::new(),
			dictionaries_by_name: SkipMap::new(),
			sumtypes: SkipMap::new(),
			sumtypes_by_name: SkipMap::new(),
			ringbuffers: SkipMap::new(),
			ringbuffers_by_name: SkipMap::new(),
			subscriptions: SkipMap::new(),
			handlers: SkipMap::new(),
			handlers_by_name: SkipMap::new(),
			handlers_by_variant: SkipMap::new(),
			users: SkipMap::new(),
			users_by_name: SkipMap::new(),
			users_by_identity: SkipMap::new(),
			roles: SkipMap::new(),
			roles_by_name: SkipMap::new(),
			user_roles: SkipMap::new(),
			policies: SkipMap::new(),
			policies_by_name: SkipMap::new(),
			migrations: SkipMap::new(),
			migrations_by_name: SkipMap::new(),
			migration_events: SkipMap::new(),
			vtable_user: SkipMap::new(),
			vtable_user_by_name: SkipMap::new(),
		}))
	}

	/// Register a user-defined virtual table
	///
	/// Returns an error if a virtual table with the same name already exists in the namespace.
	pub fn register_vtable_user(&self, def: Arc<VTableDef>) -> Result<()> {
		let key = (def.namespace, def.name.clone());

		// Check if already exists
		if self.vtable_user_by_name.contains_key(&key) {
			// Get namespace name for error message
			let ns_name = self
				.namespaces
				.get(&def.namespace)
				.map(|e| e.value().get_latest().map(|n| n.name.clone()).unwrap_or_default())
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
				.map(|e| e.value().get_latest().map(|n| n.name.clone()).unwrap_or_default())
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
	pub fn find_vtable_user_by_name(&self, namespace: NamespaceId, name: &str) -> Option<Arc<VTableDef>> {
		let key = (namespace, name.to_string());
		self.vtable_user_by_name
			.get(&key)
			.and_then(|entry| self.vtable_user.get(entry.value()).map(|e| e.value().clone()))
	}

	/// Find a user-defined virtual table by ID
	pub fn find_vtable_user(&self, id: VTableId) -> Option<Arc<VTableDef>> {
		self.vtable_user.get(&id).map(|e| e.value().clone())
	}

	/// List all user-defined virtual tables in a namespace
	pub fn list_vtable_user_in_namespace(&self, namespace: NamespaceId) -> Vec<Arc<VTableDef>> {
		self.vtable_user
			.iter()
			.filter(|e| e.value().namespace == namespace)
			.map(|e| e.value().clone())
			.collect()
	}

	/// List all user-defined virtual tables
	pub fn list_vtable_user_all(&self) -> Vec<Arc<VTableDef>> {
		self.vtable_user.iter().map(|e| e.value().clone()).collect()
	}
}
