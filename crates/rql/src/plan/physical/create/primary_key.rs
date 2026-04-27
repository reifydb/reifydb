// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{error::diagnostic::catalog::namespace_not_found, interface::resolved::ResolvedNamespace};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, return_error};

use crate::{
	Result,
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
	) -> Result<PhysicalPlan<'bump>> {
		let ns_segments: Vec<&str> = create.table.namespace.iter().map(|n| n.text()).collect();
		let Some(namespace) = self.catalog.find_namespace_by_segments(rx, &ns_segments)? else {
			let ns_fragment = if let Some(n) = create.table.namespace.first() {
				let interned = self.interner.intern_fragment(n);
				interned.with_text(ns_segments.join("::"))
			} else {
				Fragment::internal("default")
			};
			return_error!(namespace_not_found(ns_fragment, &ns_segments.join("::")));
		};

		let namespace_id = if let Some(n) = create.table.namespace.first() {
			let interned = self.interner.intern_fragment(n);
			interned.with_text(namespace.name())
		} else {
			Fragment::internal(namespace.name())
		};
		let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace);

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
