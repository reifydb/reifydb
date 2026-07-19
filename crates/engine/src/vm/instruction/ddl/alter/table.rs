// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	interface::catalog::shape::ShapeId,
	internal_error,
	key::{
		EncodableKey,
		partition::PartitionKey,
		partitioned_row::{PartitionedRowKey, RowLocator},
	},
	value::column::columns::Columns,
};
use reifydb_rql::nodes::{AlterTableAction, AlterTableNode};
use reifydb_transaction::{
	multi::RangeScope,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_value::value::{Value, partition::Partition, row_number::RowNumber, value_type::ValueType};

use crate::{Result, transaction::operation::table::TableOperations, vm::services::Services};

pub(crate) fn execute_alter_table(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: AlterTableNode,
) -> Result<Columns> {
	let namespace_id = plan.namespace.def().id();
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
		AlterTableAction::DropPartition {
			values,
			remove_registry,
		} => {
			if table.partition_by.is_empty() {
				return Err(internal_error!("table {} is not partitioned", table_name));
			}

			let mut part_values = Vec::with_capacity(table.partition_by.len());
			for col_name in &table.partition_by {
				let Some((_, text)) = values.iter().find(|(c, _)| c == col_name) else {
					return Err(internal_error!(
						"DROP PARTITION must bind partition column {}",
						col_name
					));
				};
				let is_utf8 = table
					.columns
					.iter()
					.find(|c| &c.name == col_name)
					.map(|c| c.constraint.get_type()) == Some(ValueType::Utf8);
				if !is_utf8 {
					return Err(internal_error!(
						"DROP PARTITION currently supports only Utf8 partition columns (column {})",
						col_name
					));
				}
				part_values.push(Value::Utf8(text.clone()));
			}

			let partition = Partition::of(&part_values);
			let shape = ShapeId::Table(table.id);

			let mut ids: Vec<RowNumber> = Vec::new();
			let mut last_key: Option<EncodedKey> = None;
			loop {
				let batch: Vec<_> = txn
					.range(
						PartitionedRowKey::partition_scan_range(
							shape,
							partition,
							last_key.as_ref(),
						),
						RangeScope::All,
						1024,
					)?
					.collect::<Result<Vec<_>>>()?;
				if batch.is_empty() {
					break;
				}
				let n = batch.len();
				for entry in batch {
					if let Some(RowLocator::Row(rn)) =
						PartitionedRowKey::decode(&entry.key).map(|pk| pk.locator)
					{
						ids.push(rn);
					}
					last_key = Some(entry.key);
				}
				if n < 1024 {
					break;
				}
			}

			let dropped = ids.len() as u64;
			if !ids.is_empty() {
				let partitions = vec![partition; ids.len()];
				txn.remove_from_table(&table, &ids, &partitions)?;
			}
			if remove_registry {
				txn.remove(&PartitionKey::encoded(shape, partition))?;
				("DROP PARTITION", Value::Uint8(dropped))
			} else {
				("TRUNCATE PARTITION", Value::Uint8(dropped))
			}
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
	use reifydb_value::{params::Params, value::Value};

	use crate::test_harness::create_test_admin_transaction;
	use crate::{
		vm::{Admin, executor::Executor},
	};

	fn setup() -> (Executor, AdminTransaction) {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();

		let r = instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE app",
				params: Params::default(),
			},
		);
		if let Some(e) = r.error {
			panic!("{e:?}");
		}

		let r = instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE TABLE app::users { id: Int4, name: Utf8 }",
				params: Params::default(),
			},
		);
		if let Some(e) = r.error {
			panic!("{e:?}");
		}

		(instance, txn)
	}

	#[test]
	fn test_alter_table_add_column() {
		let (instance, mut txn) = setup();

		let r = instance.admin(
			&mut txn,
			Admin {
				rql: "ALTER TABLE app::users ADD COLUMN email: Utf8",
				params: Params::default(),
			},
		);
		if let Some(e) = r.error {
			panic!("{e:?}");
		}
		let frame = &r[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("ADD COLUMN".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("app".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Utf8("users".to_string()));
		assert_eq!(frame[3].get_value(0), Value::Utf8("email".to_string()));
	}

	#[test]
	fn test_alter_table_drop_column() {
		let (instance, mut txn) = setup();

		let r = instance.admin(
			&mut txn,
			Admin {
				rql: "ALTER TABLE app::users DROP COLUMN name",
				params: Params::default(),
			},
		);
		if let Some(e) = r.error {
			panic!("{e:?}");
		}
		let frame = &r[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("DROP COLUMN".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("app".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Utf8("users".to_string()));
		assert_eq!(frame[3].get_value(0), Value::Utf8("name".to_string()));
	}

	#[test]
	fn test_alter_table_rename_column() {
		let (instance, mut txn) = setup();

		let r = instance.admin(
			&mut txn,
			Admin {
				rql: "ALTER TABLE app::users RENAME COLUMN name TO full_name",
				params: Params::default(),
			},
		);
		if let Some(e) = r.error {
			panic!("{e:?}");
		}
		let frame = &r[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("RENAME COLUMN".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("app".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Utf8("users".to_string()));
		assert_eq!(frame[3].get_value(0), Value::Utf8("name -> full_name".to_string()));
	}

	#[test]
	fn test_alter_table_drop_nonexistent_column() {
		let (instance, mut txn) = setup();

		let r = instance.admin(
			&mut txn,
			Admin {
				rql: "ALTER TABLE app::users DROP COLUMN nonexistent",
				params: Params::default(),
			},
		);
		assert!(r.is_err());
		assert_eq!(r.error.unwrap().diagnostic().code, "CA_039");
	}

	#[test]
	fn test_alter_table_rename_nonexistent_column() {
		let (instance, mut txn) = setup();

		let r = instance.admin(
			&mut txn,
			Admin {
				rql: "ALTER TABLE app::users RENAME COLUMN nonexistent TO new_name",
				params: Params::default(),
			},
		);
		assert!(r.is_err());
		assert_eq!(r.error.unwrap().diagnostic().code, "CA_039");
	}
}
