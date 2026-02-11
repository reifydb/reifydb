// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use PhysicalPlan::CreateFlow;
use reifydb_core::error::diagnostic::catalog::namespace_not_found;
use reifydb_transaction::transaction::AsTransaction;
use reifydb_type::{fragment::Fragment, return_error};

use crate::plan::{
	logical,
	physical::{Compiler, CreateFlowNode, PhysicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_flow<T: AsTransaction>(
		&mut self,
		rx: &mut T,
		create: logical::CreateFlowNode<'bump>,
	) -> crate::Result<PhysicalPlan<'bump>> {
		// Get namespace name from the MaybeQualified type (join all segments for nested namespaces)
		let namespace_name = if create.flow.namespace.is_empty() {
			"default".to_string()
		} else {
			create.flow.namespace.iter().map(|n| n.text()).collect::<Vec<_>>().join(".")
		};
		let Some(namespace) = self.catalog.find_namespace_by_name(rx, &namespace_name)? else {
			let ns_fragment = if let Some(n) = create.flow.namespace.first() {
				let interned = self.interner.intern_fragment(n);
				interned.with_text(&namespace_name)
			} else {
				Fragment::internal("default".to_string())
			};
			return_error!(namespace_not_found(ns_fragment, &namespace_name));
		};

		let physical_plan = self.compile(rx, create.as_clause)?.unwrap();

		Ok(CreateFlow(CreateFlowNode {
			namespace,
			flow: self.interner.intern_fragment(&create.flow.name),
			if_not_exists: create.if_not_exists,
			as_clause: self.bump_box(physical_plan),
		}))
	}
}
