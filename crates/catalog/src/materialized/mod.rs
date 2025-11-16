// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod flow;
pub mod load;
mod namespace;
mod operator_retention_policy;
mod primary_key;
mod source_retention_policy;
mod table;
mod view;

use std::sync::Arc;

use crossbeam_skiplist::SkipMap;
use reifydb_core::{
	interface::{
		FlowDef, FlowId, FlowNodeId, NamespaceDef, NamespaceId, PrimaryKeyDef, PrimaryKeyId, SourceId,
		TableDef, TableId, ViewDef, ViewId,
	},
	retention::RetentionPolicy,
	util::MultiVersionContainer,
};

use crate::system::SystemCatalog;

pub type MultiVersionNamespaceDef = MultiVersionContainer<NamespaceDef>;
pub type MultiVersionTableDef = MultiVersionContainer<TableDef>;
pub type MultiVersionViewDef = MultiVersionContainer<ViewDef>;
pub type MultiVersionFlowDef = MultiVersionContainer<FlowDef>;
pub type MultiVersionPrimaryKeyDef = MultiVersionContainer<PrimaryKeyDef>;
pub type MultiVersionRetentionPolicy = MultiVersionContainer<RetentionPolicy>;

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
	/// Index from (namespace_id, table_name) to table ID for fast name
	/// lookups
	pub(crate) tables_by_name: SkipMap<(NamespaceId, String), TableId>,

	/// MultiVersion view definitions indexed by view ID
	pub(crate) views: SkipMap<ViewId, MultiVersionViewDef>,
	/// Index from (namespace_id, view_name) to view ID for fast name
	/// lookups
	pub(crate) views_by_name: SkipMap<(NamespaceId, String), ViewId>,

	/// MultiVersion flow definitions indexed by flow ID
	pub(crate) flows: SkipMap<FlowId, MultiVersionFlowDef>,
	/// Index from (namespace_id, flow_name) to flow ID for fast name
	/// lookups
	pub(crate) flows_by_name: SkipMap<(NamespaceId, String), FlowId>,

	/// MultiVersion primary key definitions indexed by primary key ID
	pub(crate) primary_keys: SkipMap<PrimaryKeyId, MultiVersionPrimaryKeyDef>,

	/// MultiVersion source retention policies indexed by source ID
	pub(crate) source_retention_policies: SkipMap<SourceId, MultiVersionRetentionPolicy>,

	/// MultiVersion operator retention policies indexed by operator ID
	pub(crate) operator_retention_policies: SkipMap<FlowNodeId, MultiVersionRetentionPolicy>,

	/// System catalog with version information (None until initialized)
	pub(crate) system_catalog: Option<SystemCatalog>,
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

		let namespaces_by_name = SkipMap::new();
		namespaces_by_name.insert("system".to_string(), system_namespace_id);

		Self(Arc::new(MaterializedCatalogInner {
			namespaces,
			namespaces_by_name,
			tables: SkipMap::new(),
			tables_by_name: SkipMap::new(),
			views: SkipMap::new(),
			views_by_name: SkipMap::new(),
			flows: SkipMap::new(),
			flows_by_name: SkipMap::new(),
			primary_keys: SkipMap::new(),
			source_retention_policies: SkipMap::new(),
			operator_retention_policies: SkipMap::new(),
			system_catalog: None,
		}))
	}

	/// Set the system catalog (called once during database initialization)
	pub fn set_system_catalog(&self, catalog: SystemCatalog) {
		// Use unsafe to mutate through Arc (safe because only called
		// once during init)
		unsafe {
			let inner = Arc::as_ptr(&self.0) as *mut MaterializedCatalogInner;
			(*inner).system_catalog = Some(catalog);
		}
	}

	/// Get the system catalog
	pub fn system_catalog(&self) -> Option<&SystemCatalog> {
		self.0.system_catalog.as_ref()
	}
}
