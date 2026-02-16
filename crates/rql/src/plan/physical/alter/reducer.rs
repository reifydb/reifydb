// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::error::diagnostic::catalog::namespace_not_found;
use reifydb_transaction::transaction::AsTransaction;
use reifydb_type::{fragment::Fragment, return_error};

use crate::plan::{
	logical::alter::reducer::{AlterReducerNode as LogicalAlterReducerNode, LogicalAlterReducerAction},
	physical::{self, Compiler, PhysicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_alter_reducer<T: AsTransaction>(
		&mut self,
		rx: &mut T,
		alter: LogicalAlterReducerNode<'bump>,
	) -> crate::Result<PhysicalPlan<'bump>> {
		let namespace_name = if alter.reducer.namespace.is_empty() {
			"default".to_string()
		} else {
			alter.reducer.namespace.iter().map(|n| n.text()).collect::<Vec<_>>().join(".")
		};
		let Some(namespace) = self.catalog.find_namespace_by_name(rx, &namespace_name)? else {
			let ns_fragment = if let Some(n) = alter.reducer.namespace.first() {
				let interned = self.interner.intern_fragment(n);
				interned.with_text(&namespace_name)
			} else {
				Fragment::internal("default".to_string())
			};
			return_error!(namespace_not_found(ns_fragment, &namespace_name));
		};

		let action = match alter.action {
			LogicalAlterReducerAction::AddAction {
				name,
				columns,
				on_dispatch,
			} => {
				let physical_on_dispatch = self.compile(rx, on_dispatch)?.unwrap();
				physical::AlterReducerAction::AddAction {
					name: self.interner.intern_fragment(&name),
					columns,
					on_dispatch: self.bump_box(physical_on_dispatch),
				}
			}
			LogicalAlterReducerAction::AlterAction {
				name,
				on_dispatch,
			} => {
				let physical_on_dispatch = self.compile(rx, on_dispatch)?.unwrap();
				physical::AlterReducerAction::AlterAction {
					name: self.interner.intern_fragment(&name),
					on_dispatch: self.bump_box(physical_on_dispatch),
				}
			}
			LogicalAlterReducerAction::DropAction {
				name,
			} => physical::AlterReducerAction::DropAction {
				name: self.interner.intern_fragment(&name),
			},
		};

		Ok(PhysicalPlan::AlterReducer(physical::AlterReducerNode {
			namespace,
			reducer: self.interner.intern_fragment(&alter.reducer.name),
			action,
		}))
	}
}
