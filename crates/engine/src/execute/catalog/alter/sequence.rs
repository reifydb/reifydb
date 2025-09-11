// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use catalog::schema_not_found;
use reifydb_catalog::{CatalogStore, sequence::ColumnSequence};
use reifydb_core::{
	ColumnDescriptor,
	interface::{EvaluationContext, Params, Transaction},
	value::columnar::Columns,
};
use reifydb_rql::plan::physical::AlterSequencePlan;
use reifydb_type::{
	Value,
	diagnostic::{
		catalog, catalog::table_not_found, query::column_not_found,
		sequence::can_not_alter_not_auto_increment,
	},
	return_error,
};

use crate::{
	StandardCommandTransaction, evaluate::evaluate, execute::Executor,
};

impl Executor {
	pub(crate) fn alter_table_sequence<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		plan: AlterSequencePlan,
	) -> crate::Result<Columns> {
		let schema_name = plan.sequence.schema.text();

		let Some(schema) =
			CatalogStore::find_schema_by_name(txn, schema_name)?
		else {
			return_error!(schema_not_found(
				plan.sequence.schema.clone().into_owned(),
				schema_name,
			));
		};

		// Get the table name from the column's source
		let table_name = match &plan.column.source {
			reifydb_core::interface::identifier::ColumnSource::Source { source, .. } => source.text(),
			reifydb_core::interface::identifier::ColumnSource::Alias(alias) => alias.text(),
		};

		let Some(table) = CatalogStore::find_table_by_name(
			txn, schema.id, table_name,
		)?
		else {
			return_error!(table_not_found(
				plan.column
					.source
					.as_fragment()
					.clone()
					.into_owned(),
				&schema.name,
				table_name,
			));
		};

		let Some(column) = CatalogStore::find_column_by_name(
			txn,
			table.id,
			plan.column.name.text(),
		)?
		else {
			return_error!(column_not_found(
				plan.column.name.clone().into_owned()
			));
		};

		if !column.auto_increment {
			return_error!(can_not_alter_not_auto_increment(
				plan.column.name
			));
		}

		// For catalog operations, use empty params since no
		// ExecutionContext is available
		let empty_params = Params::None;
		let value = evaluate(
			&EvaluationContext {
				target_column: Some(ColumnDescriptor {
					schema: None,
					table: None,
					column: None,
					column_type: Some(column
						.constraint
						.get_type()
						.clone()),
					policies: vec![],
				}),
				column_policies: vec![],
				columns: Columns::empty(),
				row_count: 1,
				take: None,
				params: &empty_params,
			},
			&plan.value,
		)?;

		let data = value.data();
		debug_assert_eq!(data.len(), 1);

		let value = data.get_value(0);
		ColumnSequence::set_value(
			txn,
			table.id,
			column.id,
			value.clone(),
		)?;

		Ok(Columns::single_row([
			("schema", Value::Utf8(schema.name)),
			("table", Value::Utf8(table.name)),
			("column", Value::Utf8(column.name)),
			("value", value),
		]))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_catalog::{
		CatalogStore,
		table::{TableColumnToCreate, TableToCreate},
		test_utils::ensure_test_schema,
	};
	use reifydb_core::interface::{
		Params,
		expression::{
			ConstantExpression::Number, Expression::Constant,
		},
		identifier::{
			ColumnIdentifier, ColumnSource, SequenceIdentifier,
		},
	};
	use reifydb_rql::plan::physical::{AlterSequencePlan, PhysicalPlan};
	use reifydb_type::{Fragment, Type, TypeConstraint, Value};

	use crate::{
		execute::Executor, test_utils::create_test_command_transaction,
	};

	#[test]
	fn test_ok() {
		let mut txn = create_test_command_transaction();
		let test_schema = ensure_test_schema(&mut txn);

		CatalogStore::create_table(
			&mut txn,
			TableToCreate {
				fragment: None,
				schema: test_schema.id,
				table: "users".to_string(),
				columns: vec![
					TableColumnToCreate {
						fragment: None,
						name: "id".to_string(),
						constraint: TypeConstraint::unconstrained(Type::Int4),
						policies: vec![],
						auto_increment: true,
					},
					TableColumnToCreate {
						fragment: None,
						name: "name".to_string(),
						constraint: TypeConstraint::unconstrained(Type::Utf8),
						policies: vec![],
						auto_increment: false,
					},
				],
			},
		)
		.unwrap();

		// Alter the sequence to start at 1000
		let plan = AlterSequencePlan {
			sequence: SequenceIdentifier::new(
				Fragment::owned_internal("test_schema"),
				Fragment::owned_internal("users_id_seq"),
			),
			column: ColumnIdentifier {
				source: ColumnSource::Source {
					schema: Fragment::owned_internal(
						"test_schema",
					),
					source: Fragment::owned_internal(
						"users",
					),
				},
				name: Fragment::owned_internal("id"),
			},
			value: Constant(Number {
				fragment: Fragment::owned_internal("1000"),
			}),
		};

		let result = Executor::testing()
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::AlterSequence(plan),
				Params::default(),
			)
			.unwrap();

		assert_eq!(
			result.row(0)[0],
			Value::Utf8("test_schema".to_string())
		);
		assert_eq!(result.row(0)[1], Value::Utf8("users".to_string()));
		assert_eq!(result.row(0)[2], Value::Utf8("id".to_string()));
		assert_eq!(result.row(0)[3], Value::Int4(1000));
	}

	#[test]
	fn test_non_auto_increment_column() {
		let mut txn = create_test_command_transaction();
		let test_schema = ensure_test_schema(&mut txn);
		CatalogStore::create_table(
			&mut txn,
			TableToCreate {
				fragment: None,
				schema: test_schema.id,
				table: "items".to_string(),
				columns: vec![TableColumnToCreate {
					fragment: None,
					name: "id".to_string(),
					constraint:
						TypeConstraint::unconstrained(
							Type::Int4,
						),
					policies: vec![],
					auto_increment: false,
				}],
			},
		)
		.unwrap();

		// Try to alter sequence on non-auto-increment column
		let plan = AlterSequencePlan {
			sequence: SequenceIdentifier::new(
				Fragment::owned_internal("test_schema"),
				Fragment::owned_internal("items_id_seq"),
			),
			column: ColumnIdentifier {
				source: ColumnSource::Source {
					schema: Fragment::owned_internal(
						"test_schema",
					),
					source: Fragment::owned_internal(
						"items",
					),
				},
				name: Fragment::owned_internal("id"),
			},
			value: Constant(Number {
				fragment: Fragment::owned_internal("100"),
			}),
		};

		let err = Executor::testing()
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::AlterSequence(plan),
				Params::default(),
			)
			.unwrap_err();

		let diagnostic = err.diagnostic();
		assert_eq!(diagnostic.code, "SEQUENCE_002");
	}

	#[test]
	fn test_schema_not_found() {
		let mut txn = create_test_command_transaction();

		let plan = AlterSequencePlan {
			sequence: SequenceIdentifier::new(
				Fragment::owned_internal("non_existent_schema"),
				Fragment::owned_internal("some_table_id_seq"),
			),
			column: ColumnIdentifier {
				source: ColumnSource::Source {
					schema: Fragment::owned_internal(
						"non_existent_schema",
					),
					source: Fragment::owned_internal(
						"some_table",
					),
				},
				name: Fragment::owned_internal("id"),
			},
			value: Constant(Number {
				fragment: Fragment::owned_internal("1000"),
			}),
		};

		let err = Executor::testing()
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::AlterSequence(plan),
				Params::default(),
			)
			.unwrap_err();

		assert_eq!(err.diagnostic().code, "CA_002");
	}

	#[test]
	fn test_table_not_found() {
		let mut txn = create_test_command_transaction();
		ensure_test_schema(&mut txn);

		let plan = AlterSequencePlan {
			sequence: SequenceIdentifier::new(
				Fragment::owned_internal("test_schema"),
				Fragment::owned_internal(
					"non_existent_table_id_seq",
				),
			),
			column: ColumnIdentifier {
				source: ColumnSource::Source {
					schema: Fragment::owned_internal(
						"test_schema",
					),
					source: Fragment::owned_internal(
						"non_existent_table",
					),
				},
				name: Fragment::owned_internal("id"),
			},
			value: Constant(Number {
				fragment: Fragment::owned_internal("1000"),
			}),
		};

		let err = Executor::testing()
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::AlterSequence(plan),
				Params::default(),
			)
			.unwrap_err();

		assert_eq!(err.diagnostic().code, "CA_004");
	}

	#[test]
	fn test_column_not_found() {
		let mut txn = create_test_command_transaction();
		let test_schema = ensure_test_schema(&mut txn);

		CatalogStore::create_table(
			&mut txn,
			TableToCreate {
				fragment: None,
				schema: test_schema.id,
				table: "posts".to_string(),
				columns: vec![TableColumnToCreate {
					fragment: None,
					name: "id".to_string(),
					constraint:
						TypeConstraint::unconstrained(
							Type::Int4,
						),
					policies: vec![],
					auto_increment: true,
				}],
			},
		)
		.unwrap();

		// Try to alter sequence on non-existent column
		let plan = AlterSequencePlan {
			sequence: SequenceIdentifier::new(
				Fragment::owned_internal("test_schema"),
				Fragment::owned_internal(
					"posts_non_existent_column_seq",
				),
			),
			column: ColumnIdentifier {
				source: ColumnSource::Source {
					schema: Fragment::owned_internal(
						"test_schema",
					),
					source: Fragment::owned_internal(
						"posts",
					),
				},
				name: Fragment::owned_internal(
					"non_existent_column",
				),
			},
			value: Constant(Number {
				fragment: Fragment::owned_internal("1000"),
			}),
		};

		let err = Executor::testing()
			.execute_command_plan(
				&mut txn,
				PhysicalPlan::AlterSequence(plan),
				Params::default(),
			)
			.unwrap_err();

		assert_eq!(err.diagnostic().code, "QUERY_001");
	}
}
