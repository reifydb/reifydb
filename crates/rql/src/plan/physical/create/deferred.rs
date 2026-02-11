// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use PhysicalPlan::CreateDeferredView;
use reifydb_core::error::diagnostic::catalog::namespace_not_found;
use reifydb_transaction::transaction::AsTransaction;
use reifydb_type::{fragment::Fragment, return_error};

use crate::plan::{
	logical,
	physical::{Compiler, CreateDeferredViewNode, PhysicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_deferred<T: AsTransaction>(
		&mut self,
		rx: &mut T,
		create: logical::CreateDeferredViewNode<'bump>,
	) -> crate::Result<PhysicalPlan<'bump>> {
		// Get namespace name from the MaybeQualified type (join all segments for nested namespaces)
		let namespace_name = if create.view.namespace.is_empty() {
			"default".to_string()
		} else {
			create.view.namespace.iter().map(|n| n.text()).collect::<Vec<_>>().join(".")
		};
		let Some(namespace) = self.catalog.find_namespace_by_name(rx, &namespace_name)? else {
			let ns_fragment = if let Some(n) = create.view.namespace.first() {
				let interned = self.interner.intern_fragment(n);
				interned.with_text(&namespace_name)
			} else {
				Fragment::internal("default".to_string())
			};
			return_error!(namespace_not_found(ns_fragment, &namespace_name));
		};

		let physical_plan = self.compile(rx, create.as_clause)?.unwrap();

		Ok(CreateDeferredView(CreateDeferredViewNode {
			namespace,
			view: self.interner.intern_fragment(&create.view.name),
			if_not_exists: create.if_not_exists,
			columns: create.columns,
			as_clause: self.bump_box(physical_plan),
			primary_key: super::materialize_primary_key(&mut self.interner, create.primary_key),
		}))
	}
}
