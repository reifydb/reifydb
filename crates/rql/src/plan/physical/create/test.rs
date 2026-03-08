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
		// Resolve namespace
		let namespace_name = if create.test.namespace.is_empty() {
			"default".to_string()
		} else {
			create.test.namespace.iter().map(|n| n.text()).collect::<Vec<_>>().join("::")
		};
		let Some(namespace_def) = self.catalog.find_namespace_by_name(rx, &namespace_name)? else {
			let ns_fragment = if let Some(n) = create.test.namespace.first() {
				let interned = self.interner.intern_fragment(n);
				interned.with_text(&namespace_name)
			} else {
				Fragment::internal("default".to_string())
			};
			return_error!(namespace_not_found(ns_fragment, &namespace_name));
		};

		Ok(PhysicalPlan::CreateTest(nodes::CreateTestNode {
			namespace: namespace_def,
			name: self.interner.intern_fragment(&create.test.name),
			body_source: create.body_source,
		}))
	}
}
