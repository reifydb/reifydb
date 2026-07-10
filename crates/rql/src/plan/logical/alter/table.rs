// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::table::TableColumnToCreate;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::fragment::Fragment;

use crate::{
	Result,
	ast::{
		ast::{AstAlterTable, AstAlterTableAction, AstColumnProperty},
		identifier::MaybeQualifiedTableIdentifier,
	},
	bump::BumpFragment,
	convert_data_type_with_constraints,
	diagnostic::AstError,
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
	DropPartition {
		values: Vec<(String, String)>,
		remove_registry: bool,
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
					if let AstColumnProperty::AutoIncrement = property {
						auto_increment = true
					}
				}

				let col = TableColumnToCreate {
					name: Fragment::internal(column.name.text()),
					fragment: Fragment::internal(column.name.text()),
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
			AstAlterTableAction::DropPartition {
				spec,
				remove_registry,
			} => {
				let mut values = Vec::with_capacity(spec.keyed_values.len());
				for kv in &spec.keyed_values {
					if !kv.value.is_literal_text() {
						return Err(AstError::UnsupportedAstNode {
							node_type: "DROP PARTITION value (expected a string literal)"
								.to_string(),
							fragment: Fragment::internal(kv.key.text()),
						}
						.into());
					}
					let text = kv.value.as_literal_text();
					values.push((kv.key.text().to_string(), text.value().to_string()));
				}
				AlterTableAction::DropPartition {
					values,
					remove_registry,
				}
			}
		};

		Ok(LogicalPlan::AlterTable(AlterTableNode {
			table,
			action,
		}))
	}
}
