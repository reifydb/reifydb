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
		&self,
		_rx: &mut T,
		alter: logical::alter::view::AlterViewNode<'_>,
	) -> crate::Result<PhysicalPlan> {
		// Materialize logical node to physical node
		let view = AlterViewIdentifier {
			namespace: alter.view.namespace.map(|n| n.to_owned()),
			name: alter.view.name.to_owned(),
		};

		let operations = alter
			.operations
			.into_iter()
			.map(|op| match op {
				logical::alter::view::AlterViewOperation::CreatePrimaryKey {
					name,
					columns,
				} => {
					let columns = columns
						.into_iter()
						.map(|col| {
							use crate::ast::identifier::MaybeQualifiedColumnPrimitive;
							let namespace = match col.column.primitive {
								MaybeQualifiedColumnPrimitive::Primitive {
									namespace,
									..
								} => namespace.map(|n| n.to_owned()),
								_ => None,
							};
							AlterIndexColumn {
								column: AlterColumnIdentifier {
									namespace,
									name: col.column.name.to_owned(),
								},
								order: col.order,
							}
						})
						.collect();
					AlterViewOperation::CreatePrimaryKey {
						name: name.map(|n| n.to_owned()),
						columns,
					}
				}
				logical::alter::view::AlterViewOperation::DropPrimaryKey => {
					AlterViewOperation::DropPrimaryKey
				}
			})
			.collect();

		let plan = AlterViewNode {
			view,
			operations,
		};
		Ok(PhysicalPlan::AlterView(plan))
	}
}
