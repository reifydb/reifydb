// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::marker::PhantomData;

use reifydb_core::{error::diagnostic::catalog::namespace_not_found, interface::resolved::ResolvedNamespace};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, return_error};

use crate::{
	Result,
	plan::{
		logical::alter::table::{
			AlterTableAction as LogicalAlterTableAction, AlterTableNode as LogicalAlterTableNode,
		},
		physical::{AlterTableAction, AlterTableNode, Compiler, PhysicalPlan},
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_alter_table(
		&mut self,
		rx: &mut Transaction<'_>,
		alter: LogicalAlterTableNode<'bump>,
	) -> Result<PhysicalPlan<'bump>> {
		let namespace_name = if alter.table.namespace.is_empty() {
			"default".to_string()
		} else {
			alter.table.namespace.iter().map(|n| n.text()).collect::<Vec<_>>().join(".")
		};
		let Some(namespace_def) = self.catalog.find_namespace_by_name(rx, &namespace_name)? else {
			let ns_fragment = if let Some(n) = alter.table.namespace.first() {
				let interned = self.interner.intern_fragment(n);
				interned.with_text(&namespace_name)
			} else {
				Fragment::internal("default".to_string())
			};
			return_error!(namespace_not_found(ns_fragment, &namespace_name));
		};

		let namespace_id = if let Some(n) = alter.table.namespace.first() {
			let interned = self.interner.intern_fragment(n);
			interned.with_text(&namespace_def.name)
		} else {
			Fragment::internal(namespace_def.name.clone())
		};
		let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);

		let action = match alter.action {
			LogicalAlterTableAction::AddColumn {
				column,
			} => AlterTableAction::AddColumn {
				column,
			},
			LogicalAlterTableAction::DropColumn {
				column,
			} => AlterTableAction::DropColumn {
				column: self.interner.intern_fragment(&column),
			},
			LogicalAlterTableAction::RenameColumn {
				old_name,
				new_name,
			} => AlterTableAction::RenameColumn {
				old_name: self.interner.intern_fragment(&old_name),
				new_name: self.interner.intern_fragment(&new_name),
			},
		};

		Ok(PhysicalPlan::AlterTable(AlterTableNode {
			namespace: resolved_namespace,
			table: self.interner.intern_fragment(&alter.table.name),
			action,
			_phantom: PhantomData,
		}))
	}
}
