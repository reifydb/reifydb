// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::{AlterTableAction, AlterTableNode};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn execute_alter_table(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: AlterTableNode,
) -> Result<Columns> {
	let namespace_id = plan.namespace.def().id;
	let namespace_name = plan.namespace.name().to_string();
	let table_name = plan.table.text().to_string();

	let table =
		services.catalog.get_table_by_name(&mut Transaction::Admin(txn), namespace_id, plan.table.clone())?;

	let (operation, details) = match plan.action {
		AlterTableAction::AddColumn {
			column,
		} => {
			let col_name = column.name.text().to_string();
			services.catalog.add_table_column(txn, table.id, column, &namespace_name)?;
			("ADD COLUMN", Value::Utf8(col_name))
		}
		AlterTableAction::DropColumn {
			column,
		} => {
			let col_name = column.text().to_string();
			services.catalog.drop_table_column(txn, table.id, column.text(), &namespace_name)?;
			("DROP COLUMN", Value::Utf8(col_name))
		}
		AlterTableAction::RenameColumn {
			old_name,
			new_name,
		} => {
			let detail = format!("{} -> {}", old_name.text(), new_name.text());
			services.catalog.rename_table_column(
				txn,
				table.id,
				old_name.text(),
				new_name.text(),
				&namespace_name,
			)?;
			("RENAME COLUMN", Value::Utf8(detail))
		}
	};

	Ok(Columns::single_row([
		("operation", Value::Utf8(operation.to_string())),
		("namespace", Value::Utf8(namespace_name)),
		("table", Value::Utf8(table_name)),
		("details", details),
	]))
}

#[cfg(test)]
mod tests {
	use reifydb_transaction::transaction::admin::AdminTransaction;
	use reifydb_type::{
		params::Params,
		value::{Value, identity::IdentityId},
	};

	use crate::{
		test_utils::create_test_admin_transaction,
		vm::{Admin, executor::Executor},
	};

	fn setup() -> (Executor, AdminTransaction, IdentityId) {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();
		let identity = IdentityId::root();

		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE app",
				params: Params::default(),
				identity,
			},
		)
		.unwrap();

		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE TABLE app::users { id: Int4, name: Utf8 }",
				params: Params::default(),
				identity,
			},
		)
		.unwrap();

		(instance, txn, identity)
	}

	#[test]
	fn test_alter_table_add_column() {
		let (instance, mut txn, identity) = setup();

		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "ALTER TABLE app::users ADD COLUMN email: Utf8",
					params: Params::default(),
					identity,
				},
			)
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("ADD COLUMN".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("app".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Utf8("users".to_string()));
		assert_eq!(frame[3].get_value(0), Value::Utf8("email".to_string()));
	}

	#[test]
	fn test_alter_table_drop_column() {
		let (instance, mut txn, identity) = setup();

		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "ALTER TABLE app::users DROP COLUMN name",
					params: Params::default(),
					identity,
				},
			)
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("DROP COLUMN".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("app".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Utf8("users".to_string()));
		assert_eq!(frame[3].get_value(0), Value::Utf8("name".to_string()));
	}

	#[test]
	fn test_alter_table_rename_column() {
		let (instance, mut txn, identity) = setup();

		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "ALTER TABLE app::users RENAME COLUMN name TO full_name",
					params: Params::default(),
					identity,
				},
			)
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("RENAME COLUMN".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("app".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Utf8("users".to_string()));
		assert_eq!(frame[3].get_value(0), Value::Utf8("name -> full_name".to_string()));
	}

	#[test]
	fn test_alter_table_drop_nonexistent_column() {
		let (instance, mut txn, identity) = setup();

		let err = instance
			.admin(
				&mut txn,
				Admin {
					rql: "ALTER TABLE app::users DROP COLUMN nonexistent",
					params: Params::default(),
					identity,
				},
			)
			.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_039");
	}

	#[test]
	fn test_alter_table_rename_nonexistent_column() {
		let (instance, mut txn, identity) = setup();

		let err = instance
			.admin(
				&mut txn,
				Admin {
					rql: "ALTER TABLE app::users RENAME COLUMN nonexistent TO new_name",
					params: Params::default(),
					identity,
				},
			)
			.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_039");
	}
}
