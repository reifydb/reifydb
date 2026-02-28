// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::table::TableColumnToCreate;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	Result,
	ast::{
		ast::{AstAlterTable, AstAlterTableAction, AstColumnProperty},
		identifier::MaybeQualifiedTableIdentifier,
	},
	bump::BumpFragment,
	convert_data_type_with_constraints,
	plan::logical::{Compiler, LogicalPlan},
};

#[derive(Debug)]
pub struct AlterTableNode<'bump> {
	pub table: MaybeQualifiedTableIdentifier<'bump>,
	pub action: AlterTableAction<'bump>,
}

#[derive(Debug)]
pub enum AlterTableAction<'bump> {
	AddColumn {
		column: TableColumnToCreate,
	},
	DropColumn {
		column: BumpFragment<'bump>,
	},
	RenameColumn {
		old_name: BumpFragment<'bump>,
		new_name: BumpFragment<'bump>,
	},
}

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_alter_table(
		&self,
		ast: AstAlterTable<'bump>,
		_tx: &mut Transaction<'_>,
	) -> Result<LogicalPlan<'bump>> {
		let table = ast.table;

		let action = match ast.action {
			AstAlterTableAction::AddColumn {
				column,
			} => {
				let constraint = convert_data_type_with_constraints(&column.ty)?;

				let mut auto_increment = false;
				let dictionary_id = None;
				for property in &column.properties {
					match property {
						AstColumnProperty::AutoIncrement => auto_increment = true,
						_ => {}
					}
				}

				let col = TableColumnToCreate {
					name: Fragment::internal(column.name.text().to_string()),
					fragment: Fragment::internal(column.name.text().to_string()),
					constraint,
					properties: vec![],
					auto_increment,
					dictionary_id,
				};
				AlterTableAction::AddColumn {
					column: col,
				}
			}
			AstAlterTableAction::DropColumn {
				column,
			} => AlterTableAction::DropColumn {
				column,
			},
			AstAlterTableAction::RenameColumn {
				old_name,
				new_name,
			} => AlterTableAction::RenameColumn {
				old_name,
				new_name,
			},
		};

		Ok(LogicalPlan::AlterTable(AlterTableNode {
			table,
			action,
		}))
	}
}
