// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use PhysicalPlan::CreateTable;
use reifydb_catalog::CatalogStore;
use reifydb_core::{
	diagnostic::catalog::namespace_not_found,
	interface::{QueryTransaction, identifier::NamespaceIdentifier, resolved::ResolvedNamespace},
};
use reifydb_type::return_error;

use crate::plan::{
	logical,
	physical::{Compiler, CreateTableNode, PhysicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create_table<'a>(
		rx: &mut impl QueryTransaction,
		create: logical::CreateTableNode<'a>,
	) -> crate::Result<PhysicalPlan<'a>> {
		let Some(namespace_def) = CatalogStore::find_namespace_by_name(rx, create.table.namespace.text())?
		else {
			return_error!(namespace_not_found(
				create.table.namespace.clone(),
				create.table.namespace.text()
			));
		};

		// Create a ResolvedNamespace
		let namespace_id = NamespaceIdentifier::new(create.table.namespace.clone());
		let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);

		Ok(CreateTable(CreateTableNode {
			namespace: resolved_namespace,
			table: create.table.clone(),
			if_not_exists: create.if_not_exists,
			columns: create.columns,
		}))
	}
}
