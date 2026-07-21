// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::encoded::shape::{RowShape, fingerprint::RowShapeFingerprint};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		id::{NamespaceId, TableId},
		namespace::Namespace,
		table::Table,
	},
};

pub trait HostCatalog: Send + Sync {
	fn find_namespace(&self, namespace: NamespaceId, version: CommitVersion) -> Option<Namespace>;
	fn find_namespace_by_name(&self, namespace: &str, version: CommitVersion) -> Option<Namespace>;
	fn find_table(&self, table: TableId, version: CommitVersion) -> Option<Table>;
	fn find_table_by_name(&self, namespace: NamespaceId, name: &str, version: CommitVersion) -> Option<Table>;
	fn find_row_shape(&self, fingerprint: RowShapeFingerprint) -> Option<RowShape>;
}

pub struct StandardHostCatalog {
	catalog: Catalog,
}

impl StandardHostCatalog {
	pub fn new(catalog: Catalog) -> Self {
		Self {
			catalog,
		}
	}
}

impl HostCatalog for StandardHostCatalog {
	fn find_namespace(&self, namespace: NamespaceId, version: CommitVersion) -> Option<Namespace> {
		self.catalog.cache().find_namespace_at(namespace, version)
	}
	fn find_namespace_by_name(&self, namespace: &str, version: CommitVersion) -> Option<Namespace> {
		self.catalog.cache().find_namespace_by_name_at(namespace, version)
	}
	fn find_table(&self, table: TableId, version: CommitVersion) -> Option<Table> {
		self.catalog.cache().find_table_at(table, version)
	}
	fn find_table_by_name(&self, namespace: NamespaceId, name: &str, version: CommitVersion) -> Option<Table> {
		self.catalog.cache().find_table_by_name_at(namespace, name, version)
	}
	fn find_row_shape(&self, fingerprint: RowShapeFingerprint) -> Option<RowShape> {
		self.catalog.cache().find_row_shape(fingerprint)
	}
}
