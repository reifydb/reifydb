// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::error::diagnostic::catalog::namespace_not_found;
use reifydb_transaction::transaction::AsTransaction;
use reifydb_type::{fragment::Fragment, return_error};

use crate::{
	nodes::CreateReducerNode,
	plan::{
		logical,
		physical::{Compiler, PhysicalPlan},
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_reducer<T: AsTransaction>(
		&mut self,
		rx: &mut T,
		create: logical::CreateReducerNode<'bump>,
	) -> crate::Result<PhysicalPlan<'bump>> {
		let namespace_name = if create.reducer.namespace.is_empty() {
			"default".to_string()
		} else {
			create.reducer.namespace.iter().map(|n| n.text()).collect::<Vec<_>>().join(".")
		};
		let Some(namespace) = self.catalog.find_namespace_by_name(rx, &namespace_name)? else {
			let ns_fragment = if let Some(n) = create.reducer.namespace.first() {
				let interned = self.interner.intern_fragment(n);
				interned.with_text(&namespace_name)
			} else {
				Fragment::internal("default".to_string())
			};
			return_error!(namespace_not_found(ns_fragment, &namespace_name));
		};

		let key = create.key.iter().map(|k| self.interner.intern_fragment(k)).collect();

		Ok(PhysicalPlan::CreateReducer(CreateReducerNode {
			namespace,
			reducer: self.interner.intern_fragment(&create.reducer.name),
			columns: create.columns,
			key,
		}))
	}
}
