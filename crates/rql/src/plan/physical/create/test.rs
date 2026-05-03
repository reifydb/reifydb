// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::error::diagnostic::catalog::namespace_not_found;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, return_error};

use crate::{
	Result, nodes,
	plan::{
		logical,
		physical::{Compiler, PhysicalPlan},
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_test(
		&mut self,
		rx: &mut Transaction<'_>,
		create: logical::CreateTestNode<'_>,
	) -> Result<PhysicalPlan<'bump>> {
		let ns_segments: Vec<&str> = create.test.namespace.iter().map(|n| n.text()).collect();
		let Some(namespace_def) = self.catalog.find_namespace_by_segments(rx, &ns_segments)? else {
			let ns_fragment = if let Some(n) = create.test.namespace.first() {
				let interned = self.interner.intern_fragment(n);
				interned.with_text(ns_segments.join("::"))
			} else {
				Fragment::internal("default")
			};
			return_error!(namespace_not_found(ns_fragment, &ns_segments.join("::")));
		};

		Ok(PhysicalPlan::CreateTest(nodes::CreateTestNode {
			namespace: namespace_def,
			name: self.interner.intern_fragment(&create.test.name),
			cases: create.cases,
			body_source: create.body_source,
		}))
	}
}
