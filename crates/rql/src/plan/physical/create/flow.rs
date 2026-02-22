// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use PhysicalPlan::CreateFlow;
use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::plan::{
	logical,
	physical::{Compiler, CreateFlowNode, PhysicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_flow(
		&mut self,
		rx: &mut Transaction<'_>,
		create: logical::CreateFlowNode<'bump>,
	) -> crate::Result<PhysicalPlan<'bump>> {
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
			return Err(CatalogError::NotFound {
				kind: CatalogObjectKind::Namespace,
				namespace: namespace_name.to_string(),
				name: String::new(),
				fragment: ns_fragment,
			}
			.into());
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
