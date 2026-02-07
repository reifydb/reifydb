// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use PhysicalPlan::CreateTable;
use reifydb_core::{error::diagnostic::catalog::namespace_not_found, interface::resolved::ResolvedNamespace};
use reifydb_transaction::transaction::AsTransaction;
use reifydb_type::{fragment::Fragment, return_error};

use crate::plan::{
	logical,
	physical::{Compiler, CreateTableNode, PhysicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_table<T: AsTransaction>(
		&mut self,
		rx: &mut T,
		create: logical::CreateTableNode<'_>,
	) -> crate::Result<PhysicalPlan<'bump>> {
		// Get namespace name from the MaybeQualified type
		let namespace_name = create.table.namespace.as_ref().map(|n| n.text()).unwrap_or("default");
		let Some(namespace_def) = self.catalog.find_namespace_by_name(rx, namespace_name)? else {
			let ns_fragment = match create.table.namespace {
				Some(n) => self.interner.intern_fragment(&n),
				None => Fragment::internal("default".to_string()),
			};
			return_error!(namespace_not_found(ns_fragment, namespace_name));
		};

		// Create a ResolvedNamespace
		let namespace_id = match create.table.namespace {
			Some(n) => self.interner.intern_fragment(&n),
			None => Fragment::internal(namespace_def.name.clone()),
		};
		let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);

		Ok(CreateTable(CreateTableNode {
			namespace: resolved_namespace,
			table: self.interner.intern_fragment(&create.table.name),
			if_not_exists: create.if_not_exists,
			columns: create.columns,
			primary_key: super::materialize_primary_key(&mut self.interner, create.primary_key),
		}))
	}
}
