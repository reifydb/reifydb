// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{error::diagnostic::catalog::namespace_not_found, interface::resolved::ResolvedNamespace};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, return_error};

use crate::{
	nodes::{CreatePrimaryKeyNode, PrimaryKeyColumn},
	plan::{
		logical,
		physical::{Compiler, PhysicalPlan},
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_primary_key(
		&mut self,
		rx: &mut Transaction<'_>,
		create: logical::CreatePrimaryKeyNode<'_>,
	) -> crate::Result<PhysicalPlan<'bump>> {
		let namespace_name = if create.table.namespace.is_empty() {
			"default".to_string()
		} else {
			create.table.namespace.iter().map(|n| n.text()).collect::<Vec<_>>().join(".")
		};
		let Some(namespace_def) = self.catalog.find_namespace_by_name(rx, &namespace_name)? else {
			let ns_fragment = if let Some(n) = create.table.namespace.first() {
				let interned = self.interner.intern_fragment(n);
				interned.with_text(&namespace_name)
			} else {
				Fragment::internal("default".to_string())
			};
			return_error!(namespace_not_found(ns_fragment, &namespace_name));
		};

		let namespace_id = if let Some(n) = create.table.namespace.first() {
			let interned = self.interner.intern_fragment(n);
			interned.with_text(&namespace_def.name)
		} else {
			Fragment::internal(namespace_def.name.clone())
		};
		let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);

		let columns = create
			.columns
			.into_iter()
			.map(|col| PrimaryKeyColumn {
				column: self.interner.intern_fragment(&col.column),
				order: col.order,
			})
			.collect();

		Ok(PhysicalPlan::CreatePrimaryKey(CreatePrimaryKeyNode {
			namespace: resolved_namespace,
			table: self.interner.intern_fragment(&create.table.name),
			columns,
		}))
	}
}
