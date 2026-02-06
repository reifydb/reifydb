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
pub struct AlterTableNode {
	pub table: AlterTableIdentifier,
	pub operations: Vec<AlterTableOperation>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterTableIdentifier {
	pub namespace: Option<Fragment>,
	pub name: Fragment,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableOperation {
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
	pub(crate) fn compile_alter_table<T: AsTransaction>(
		&self,
		_rx: &mut T,
		alter: logical::alter::table::AlterTableNode<'_>,
	) -> crate::Result<PhysicalPlan> {
		// Materialize logical node to physical node
		let table = AlterTableIdentifier {
			namespace: alter.table.namespace.map(|n| n.to_owned()),
			name: alter.table.name.to_owned(),
		};

		let operations = alter
			.operations
			.into_iter()
			.map(|op| match op {
				logical::alter::table::AlterTableOperation::CreatePrimaryKey {
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
					AlterTableOperation::CreatePrimaryKey {
						name: name.map(|n| n.to_owned()),
						columns,
					}
				}
				logical::alter::table::AlterTableOperation::DropPrimaryKey => {
					AlterTableOperation::DropPrimaryKey
				}
			})
			.collect();

		let plan = AlterTableNode {
			table,
			operations,
		};
		Ok(PhysicalPlan::AlterTable(plan))
	}
}
