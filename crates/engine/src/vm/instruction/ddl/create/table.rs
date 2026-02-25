// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::table::{TableColumnToCreate, TableToCreate};
use reifydb_core::{interface::catalog::change::CatalogTrackTableChangeOperations, value::column::columns::Columns};
use reifydb_rql::nodes::CreateTableNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::{
	fragment::Fragment,
	value::{
		Value,
		constraint::{Constraint, TypeConstraint},
		r#type::Type,
	},
};

use crate::vm::services::Services;

pub(crate) fn create_table(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateTableNode,
) -> crate::Result<Columns> {
	// Check if table already exists using the catalog
	if let Some(_) = services.catalog.find_table_by_name(
		&mut Transaction::Admin(txn),
		plan.namespace.def().id,
		plan.table.text(),
	)? {
		if plan.if_not_exists {
			return Ok(Columns::single_row([
				("namespace", Value::Utf8(plan.namespace.name().to_string())),
				("table", Value::Utf8(plan.table.text().to_string())),
				("created", Value::Boolean(false)),
			]));
		}
		// The error will be returned by create_table if the
		// table exists
	}

	let columns = expand_sumtype_columns(services, txn, plan.columns)?;

	let table = services.catalog.create_table(
		txn,
		TableToCreate {
			name: plan.table.clone(),
			namespace: plan.namespace.def().id,
			columns,
			retention_policy: None,
			primary_key_columns: None,
		},
	)?;
	txn.track_table_def_created(table.clone())?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace.name().to_string())),
		("table", Value::Utf8(plan.table.text().to_string())),
		("created", Value::Boolean(true)),
	]))
}

fn expand_sumtype_columns(
	services: &Services,
	txn: &mut AdminTransaction,
	columns: Vec<TableColumnToCreate>,
) -> crate::Result<Vec<TableColumnToCreate>> {
	let mut expanded = Vec::with_capacity(columns.len());

	for col in columns {
		match col.constraint.constraint() {
			Some(Constraint::SumType(id)) => {
				let def = services.catalog.get_sumtype(&mut Transaction::Admin(&mut *txn), *id)?;
				let col_name = col.name.text();

				expanded.push(TableColumnToCreate {
					name: Fragment::internal(format!("{col_name}_tag")),
					fragment: col.fragment.clone(),
					constraint: TypeConstraint::with_constraint(
						Type::Uint1,
						Constraint::SumType(*id),
					),
					policies: vec![],
					auto_increment: false,
					dictionary_id: None,
				});

				for variant in &def.variants {
					for field in &variant.fields {
						let field_type = Type::Option(Box::new(field.field_type.get_type()));
						expanded.push(TableColumnToCreate {
							name: Fragment::internal(format!(
								"{col_name}_{}_{}",
								variant.name, field.name
							)),
							fragment: col.fragment.clone(),
							constraint: TypeConstraint::unconstrained(field_type),
							policies: vec![],
							auto_increment: false,
							dictionary_id: None,
						});
					}
				}
			}
			_ => {
				expanded.push(col);
			}
		}
	}

	Ok(expanded)
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::auth::Identity;
	use reifydb_type::{params::Params, value::Value};

	use crate::{
		test_utils::create_test_admin_transaction,
		vm::{Admin, executor::Executor},
	};

	#[test]
	fn test_create_table() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();
		let identity = Identity::root();

		// Create namespace first
		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE test_namespace",
				params: Params::default(),
				identity: &identity,
			},
		)
		.unwrap();

		// First creation should succeed
		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE TABLE test_namespace::test_table { id: Int4 }",
					params: Params::default(),
					identity: &identity,
				},
			)
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("test_namespace".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_table".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Boolean(true));

		// Creating the same table again should return error
		let err = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE TABLE test_namespace::test_table { id: Int4 }",
					params: Params::default(),
					identity: &identity,
				},
			)
			.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_003");
	}

	#[test]
	fn test_create_same_table_in_different_schema() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();
		let identity = Identity::root();

		// Create both namespaces
		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE test_namespace",
				params: Params::default(),
				identity: &identity,
			},
		)
		.unwrap();
		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE another_schema",
				params: Params::default(),
				identity: &identity,
			},
		)
		.unwrap();

		// Create table in first namespace
		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE TABLE test_namespace::test_table { id: Int4 }",
					params: Params::default(),
					identity: &identity,
				},
			)
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("test_namespace".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_table".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Boolean(true));

		// Create table with same name in different namespace
		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE TABLE another_schema::test_table { id: Int4 }",
					params: Params::default(),
					identity: &identity,
				},
			)
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[0].get_value(0), Value::Utf8("another_schema".to_string()));
		assert_eq!(frame[1].get_value(0), Value::Utf8("test_table".to_string()));
		assert_eq!(frame[2].get_value(0), Value::Boolean(true));
	}

	#[test]
	fn test_create_table_with_sumtype_column() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();
		let identity = Identity::root();

		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE app",
				params: Params::default(),
				identity: &identity,
			},
		)
		.unwrap();

		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE ENUM app::Shape { Circle { radius: Float8 }, Rectangle { width: Float8, height: Float8 } }",
				params: Params::default(),
				identity: &identity,
			},
		)
		.unwrap();

		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE TABLE app::drawings { id: Int4, shape: app::Shape }",
					params: Params::default(),
					identity: &identity,
				},
			)
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[2].get_value(0), Value::Boolean(true));
	}

	#[test]
	fn test_create_table_with_unit_sumtype_column() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();
		let identity = Identity::root();

		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE app",
				params: Params::default(),
				identity: &identity,
			},
		)
		.unwrap();

		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE ENUM app::Status { Active, Inactive, Pending }",
				params: Params::default(),
				identity: &identity,
			},
		)
		.unwrap();

		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "CREATE TABLE app::tasks { id: Int4, status: app::Status }",
					params: Params::default(),
					identity: &identity,
				},
			)
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[2].get_value(0), Value::Boolean(true));
	}

	#[test]
	fn test_insert_with_sumtype_constructor() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();
		let identity = Identity::root();

		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE app",
				params: Params::default(),
				identity: &identity,
			},
		)
		.unwrap();

		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE ENUM app::Shape { Circle { radius: Float8 }, Rectangle { width: Float8, height: Float8 } }",
				params: Params::default(),
				identity: &identity,
			},
		)
		.unwrap();

		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE TABLE app::drawings { id: Int4, shape: app::Shape }",
				params: Params::default(),
				identity: &identity,
			},
		)
		.unwrap();

		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "INSERT app::drawings [{ id: 1, shape: app::Shape::Circle { radius: 5.0 } }]",
					params: Params::default(),
					identity: &identity,
				},
			)
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[2].get_value(0), Value::Uint8(1));
	}

	#[test]
	fn test_insert_with_unit_sumtype_constructor() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();
		let identity = Identity::root();

		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE app",
				params: Params::default(),
				identity: &identity,
			},
		)
		.unwrap();

		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE ENUM app::Status { Active, Inactive, Pending }",
				params: Params::default(),
				identity: &identity,
			},
		)
		.unwrap();

		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE TABLE app::tasks { id: Int4, status: app::Status }",
				params: Params::default(),
				identity: &identity,
			},
		)
		.unwrap();

		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "INSERT app::tasks [{ id: 1, status: app::Status::Active }]",
					params: Params::default(),
					identity: &identity,
				},
			)
			.unwrap();
		let frame = &frames[0];
		assert_eq!(frame[2].get_value(0), Value::Uint8(1));
	}

	#[test]
	fn test_filter_is_variant() {
		let instance = Executor::testing();
		let mut txn = create_test_admin_transaction();
		let identity = Identity::root();

		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE NAMESPACE app",
				params: Params::default(),
				identity: &identity,
			},
		)
		.unwrap();

		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE ENUM app::Shape { Circle { radius: Float8 }, Rectangle { width: Float8, height: Float8 } }",
				params: Params::default(),
				identity: &identity,
			},
		)
		.unwrap();

		instance.admin(
			&mut txn,
			Admin {
				rql: "CREATE TABLE app::drawings { id: Int4, shape: app::Shape }",
				params: Params::default(),
				identity: &identity,
			},
		)
		.unwrap();

		instance.admin(
			&mut txn,
			Admin {
				rql: "INSERT app::drawings [{ id: 1, shape: app::Shape::Circle { radius: 5.0 } }]",
				params: Params::default(),
				identity: &identity,
			},
		)
		.unwrap();

		instance.admin(
			&mut txn,
			Admin {
				rql: "INSERT app::drawings [{ id: 2, shape: app::Shape::Rectangle { width: 3.0, height: 4.0 } }]",
				params: Params::default(),
				identity: &identity,
			},
		)
		.unwrap();

		instance.admin(
			&mut txn,
			Admin {
				rql: "INSERT app::drawings [{ id: 3, shape: app::Shape::Circle { radius: 10.0 } }]",
				params: Params::default(),
				identity: &identity,
			},
		)
		.unwrap();

		let frames = instance
			.admin(
				&mut txn,
				Admin {
					rql: "FROM app::drawings | FILTER shape IS app::Shape::Circle",
					params: Params::default(),
					identity: &identity,
				},
			)
			.unwrap();

		assert!(!frames.is_empty());
		let frame = &frames[0];
		let id_col = frame.columns.iter().find(|c| c.name == "id").expect("id column");
		assert_eq!(id_col.data.len(), 2);
		let mut ids: Vec<Value> = (0..2).map(|i| id_col.get_value(i)).collect();
		ids.sort_by_key(|v| match v {
			Value::Int4(n) => *n,
			_ => panic!("expected Int4"),
		});
		assert_eq!(ids, vec![Value::Int4(1), Value::Int4(3)]);
	}
}
