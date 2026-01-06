// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use PhysicalPlan::CreateTable;
use reifydb_core::{diagnostic::catalog::namespace_not_found, interface::resolved::ResolvedNamespace};
use reifydb_transaction::IntoStandardTransaction;
use reifydb_type::{Fragment, return_error};

use crate::plan::{
	logical,
	physical::{Compiler, CreateTableNode, PhysicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create_table<T: IntoStandardTransaction>(
		&self,
		rx: &mut T,
		create: logical::CreateTableNode,
	) -> crate::Result<PhysicalPlan> {
		// Get namespace name from the MaybeQualified type
		let namespace_name = create.table.namespace.as_ref().map(|n| n.text()).unwrap_or("default");
		let Some(namespace_def) = self.catalog.find_namespace_by_name(rx, namespace_name)? else {
			let ns_fragment = create
				.table
				.namespace
				.clone()
				.unwrap_or_else(|| Fragment::internal("default".to_string()));
			return_error!(namespace_not_found(ns_fragment, namespace_name));
		};

		// Create a ResolvedNamespace
		let namespace_id = create
			.table
			.namespace
			.clone()
			.unwrap_or_else(|| Fragment::internal(namespace_def.name.clone()));
		let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);

		Ok(CreateTable(CreateTableNode {
			namespace: resolved_namespace,
			table: create.table.name.clone(), // Extract just the name Fragment
			if_not_exists: create.if_not_exists,
			columns: create.columns,
			primary_key: create.primary_key,
		}))
	}
}
