// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::sort::SortDirection;
use reifydb_transaction::transaction::AsTransaction;
use reifydb_type::fragment::Fragment;

use crate::plan::{
	logical,
	physical::{Compiler, PhysicalPlan},
};

#[derive(Debug, Clone, PartialEq)]
pub struct AlterViewNode {
	pub view: AlterViewIdentifier,
	pub operations: Vec<AlterViewOperation>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterViewIdentifier {
	pub namespace: Option<Fragment>,
	pub name: Fragment,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterViewOperation {
	CreatePrimaryKey {
		name: Option<Fragment>,
		columns: Vec<AlterIndexColumn>,
	},
	DropPrimaryKey,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterIndexColumn {
	pub column: AlterColumnIdentifier,
	pub order: Option<SortDirection>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterColumnIdentifier {
	pub namespace: Option<Fragment>,
	pub name: Fragment,
}

impl Compiler {
	pub(crate) fn compile_alter_view<T: AsTransaction>(
		&mut self,
		_rx: &mut T,
		alter: logical::alter::view::AlterViewNode<'_>,
	) -> crate::Result<PhysicalPlan> {
		// Materialize logical node to physical node
		let view = AlterViewIdentifier {
			namespace: alter.view.namespace.map(|n| self.interner.intern_fragment(&n)),
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
						let namespace = match col.column.primitive {
							MaybeQualifiedColumnPrimitive::Primitive {
								namespace,
								..
							} => namespace.map(|n| self.interner.intern_fragment(&n)),
							_ => None,
						};
						physical_columns.push(AlterIndexColumn {
							column: AlterColumnIdentifier {
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
