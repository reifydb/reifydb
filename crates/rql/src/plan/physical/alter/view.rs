// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::Transaction;

use crate::{
	nodes::{
		AlterViewColumnIdentifier, AlterViewIdentifier, AlterViewIndexColumn, AlterViewNode, AlterViewOperation,
	},
	plan::{
		logical,
		physical::{Compiler, PhysicalPlan},
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_alter_view(
		&mut self,
		_rx: &mut Transaction<'_>,
		alter: logical::alter::view::AlterViewNode<'_>,
	) -> crate::Result<PhysicalPlan<'bump>> {
		// Materialize logical node to physical node
		let view = AlterViewIdentifier {
			namespace: alter.view.namespace.first().map(|n| self.interner.intern_fragment(n)),
			name: self.interner.intern_fragment(&alter.view.name),
		};

		let mut operations = Vec::with_capacity(alter.operations.len());
		for op in alter.operations {
			match op {
				logical::alter::view::AlterViewOperation::CreatePrimaryKey {
					name,
					columns,
				} => {
					let mut physical_columns = Vec::with_capacity(columns.len());
					for col in columns {
						use crate::ast::identifier::MaybeQualifiedColumnPrimitive;
						let namespace = match &col.column.primitive {
							MaybeQualifiedColumnPrimitive::Primitive {
								namespace,
								..
							} => namespace.first().map(|n| self.interner.intern_fragment(n)),
							_ => None,
						};
						physical_columns.push(AlterViewIndexColumn {
							column: AlterViewColumnIdentifier {
								namespace,
								name: self.interner.intern_fragment(&col.column.name),
							},
							order: col.order,
						});
					}
					operations.push(AlterViewOperation::CreatePrimaryKey {
						name: name.map(|n| self.interner.intern_fragment(&n)),
						columns: physical_columns,
					});
				}
				logical::alter::view::AlterViewOperation::DropPrimaryKey => {
					operations.push(AlterViewOperation::DropPrimaryKey);
				}
			}
		}

		let plan = AlterViewNode {
			view,
			operations,
		};
		Ok(PhysicalPlan::AlterView(plan))
	}
}
