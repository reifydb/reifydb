// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{error::diagnostic::catalog::namespace_not_found, interface::resolved::ResolvedNamespace};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{fragment::Fragment, return_error};

use crate::{
	Result,
	nodes::CreateSegmentTreeNode,
	plan::{
		logical,
		physical::{Compiler, PhysicalPlan},
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_segment_tree(
		&mut self,
		rx: &mut Transaction<'_>,
		create: logical::CreateSegmentTreeNode<'_>,
	) -> Result<PhysicalPlan<'bump>> {
		let ns_segments: Vec<&str> = create.segment_tree.namespace.iter().map(|n| n.text()).collect();
		let Some(namespace) = self.catalog.find_namespace_by_segments(rx, &ns_segments)? else {
			let ns_fragment = if let Some(n) = create.segment_tree.namespace.first() {
				let interned = self.interner.intern_fragment(n);
				interned.with_text(ns_segments.join("::"))
			} else {
				Fragment::internal("default")
			};
			return_error!(namespace_not_found(ns_fragment, &ns_segments.join("::")));
		};

		let namespace_id = if let Some(n) = create.segment_tree.namespace.first() {
			let interned = self.interner.intern_fragment(n);
			interned.with_text(namespace.name())
		} else {
			Fragment::internal(namespace.name())
		};
		let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace);

		Ok(PhysicalPlan::CreateSegmentTree(CreateSegmentTreeNode {
			namespace: resolved_namespace,
			segment_tree: self.interner.intern_fragment(&create.segment_tree.name),
			columns: create.columns,
			key: create.key,
			aggregates: create.aggregates,
			partition_by: create.partition_by,
			persistent: create.persistent,
		}))
	}
}
