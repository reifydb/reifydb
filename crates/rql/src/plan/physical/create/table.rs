// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use PhysicalPlan::CreateTable;
use reifydb_core::{error::diagnostic::catalog::namespace_not_found, interface::resolved::ResolvedNamespace};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, return_error};

use crate::{
	Result,
	plan::{
		logical,
		physical::{Compiler, CreateTableNode, PhysicalPlan},
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_table(
		&mut self,
		rx: &mut Transaction<'_>,
		create: logical::CreateTableNode<'_>,
	) -> Result<PhysicalPlan<'bump>> {
		// Get namespace name from the MaybeQualified type (join all segments for nested namespaces)
		let ns_segments: Vec<&str> = create.table.namespace.iter().map(|n| n.text()).collect();
		let Some(namespace) = self.catalog.find_namespace_by_segments(rx, &ns_segments)? else {
			let ns_fragment = if let Some(n) = create.table.namespace.first() {
				let interned = self.interner.intern_fragment(n);
				interned.with_text(ns_segments.join("::"))
			} else {
				Fragment::internal("default".to_string())
			};
			return_error!(namespace_not_found(ns_fragment, &ns_segments.join("::")));
		};

		let namespace_id = if let Some(n) = create.table.namespace.first() {
			let interned = self.interner.intern_fragment(n);
			interned.with_text(namespace.name())
		} else {
			Fragment::internal(namespace.name().to_string())
		};
		let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace);

		Ok(CreateTable(CreateTableNode {
			namespace: resolved_namespace,
			table: self.interner.intern_fragment(&create.table.name),
			if_not_exists: create.if_not_exists,
			columns: create.columns,
			ttl: create.ttl,
		}))
	}
}
