// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(feature = "auth")]
pub mod identity;

use reifydb_catalog::catalog::{
	Catalog,
	namespace::NamespaceToCreate,
	table::{TableColumnToCreate, TableToCreate},
	view::{ViewColumnToCreate, ViewToCreate},
};
use reifydb_catalog::store::view::create::ViewStorageConfig;
use reifydb_core::interface::catalog::{id::NamespaceId, namespace::Namespace, table::Table, view::View};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::{
	fragment::Fragment,
	value::{constraint::TypeConstraint, identity::IdentityId, value_type::ValueType},
};

use crate::engine::AsEngine;

pub fn namespace(name: &str) -> NamespaceBuilder {
	NamespaceBuilder {
		name: name.to_string(),
	}
}

pub fn table(qualified_name: &str) -> TableBuilder {
	let (namespace, name) = split_qualified(qualified_name);
	TableBuilder {
		namespace,
		name,
		columns: Vec::new(),
	}
}

pub fn view(qualified_name: &str) -> ViewBuilder {
	let (namespace, name) = split_qualified(qualified_name);
	ViewBuilder {
		namespace,
		name,
		columns: Vec::new(),
	}
}

pub struct NamespaceBuilder {
	name: String,
}

impl NamespaceBuilder {
	pub fn create(self, engine: &impl AsEngine) -> Namespace {
		let engine = engine.standard_engine();
		let catalog = engine.catalog();
		let mut admin = engine.begin_admin(IdentityId::root()).unwrap();
		let namespace = ensure_namespace(&catalog, &mut admin, &self.name);
		admin.commit().unwrap();
		namespace
	}
}

pub struct TableBuilder {
	namespace: String,
	name: String,
	columns: Vec<TableColumnToCreate>,
}

impl TableBuilder {
	pub fn column(mut self, name: &str, value_type: ValueType) -> Self {
		self.columns.push(TableColumnToCreate {
			name: Fragment::internal(name),
			fragment: Fragment::None,
			constraint: TypeConstraint::unconstrained(value_type),
			properties: vec![],
			auto_increment: false,
			dictionary_id: None,
		});
		self
	}

	pub fn column_with(mut self, column: TableColumnToCreate) -> Self {
		self.columns.push(column);
		self
	}

	pub fn create(self, engine: &impl AsEngine) -> Table {
		let engine = engine.standard_engine();
		let catalog = engine.catalog();
		let mut admin = engine.begin_admin(IdentityId::root()).unwrap();
		let namespace = ensure_namespace(&catalog, &mut admin, &self.namespace);
		let table = catalog
			.create_table(
				&mut admin,
				TableToCreate {
					name: Fragment::internal(&self.name),
					namespace: namespace.id(),
					columns: self.columns,
					retention_strategy: None,
					primary_key_columns: None,
					partition_by: vec![],
					underlying: false,
				},
			)
			.unwrap();
		admin.commit().unwrap();
		table
	}
}

pub struct ViewBuilder {
	namespace: String,
	name: String,
	columns: Vec<ViewColumnToCreate>,
}

impl ViewBuilder {
	pub fn column(mut self, name: &str, value_type: ValueType) -> Self {
		self.columns.push(ViewColumnToCreate {
			name: Fragment::internal(name),
			fragment: Fragment::None,
			constraint: TypeConstraint::unconstrained(value_type),
			dictionary_id: None,
		});
		self
	}

	pub fn create(self, engine: &impl AsEngine) -> View {
		let engine = engine.standard_engine();
		let catalog = engine.catalog();
		let mut admin = engine.begin_admin(IdentityId::root()).unwrap();
		let namespace = ensure_namespace(&catalog, &mut admin, &self.namespace);
		let view = catalog
			.create_deferred_view(
				&mut admin,
				ViewToCreate {
					name: Fragment::internal(&self.name),
					namespace: namespace.id(),
					columns: self.columns,
					storage: ViewStorageConfig::default(),
					sort: vec![],
				},
			)
			.unwrap();
		admin.commit().unwrap();
		view
	}
}

fn ensure_namespace(catalog: &Catalog, admin: &mut AdminTransaction, name: &str) -> Namespace {
	if let Some(existing) = catalog.find_namespace_by_name(&mut Transaction::Admin(&mut *admin), name).unwrap() {
		return existing;
	}
	let local_name = name.rsplit_once("::").map(|(_, s)| s).unwrap_or(name);
	catalog
		.create_namespace(
			admin,
			NamespaceToCreate {
				namespace_fragment: None,
				name: name.to_string(),
				local_name: local_name.to_string(),
				parent_id: NamespaceId::ROOT,
				grpc: None,
				token: None,
			},
		)
		.unwrap()
}

fn split_qualified(qualified_name: &str) -> (String, String) {
	match qualified_name.rsplit_once("::") {
		Some((namespace, name)) => (namespace.to_string(), name.to_string()),
		None => ("test_namespace".to_string(), qualified_name.to_string()),
	}
}
