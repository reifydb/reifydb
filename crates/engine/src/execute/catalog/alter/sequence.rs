// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use catalog::namespace_not_found;
use reifydb_catalog::{CatalogStore, sequence::ColumnSequence};
use reifydb_core::{
	interface::{ColumnEvaluationContext, Params, TargetColumn, Transaction},
	value::column::Columns,
};
use reifydb_rql::plan::physical::AlterSequenceNode;
use reifydb_type::{
	Value,
	diagnostic::{
		catalog, catalog::table_not_found, query::column_not_found, sequence::can_not_alter_not_auto_increment,
	},
	return_error,
};

use crate::{StandardCommandTransaction, evaluate::column::evaluate, execute::Executor};

impl Executor {
	pub(crate) fn alter_table_sequence<'a, T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		plan: AlterSequenceNode,
	) -> crate::Result<Columns<'a>> {
		let namespace_name = plan.sequence.namespace().name();

		let Some(namespace) = CatalogStore::find_namespace_by_name(txn, namespace_name)? else {
			return_error!(namespace_not_found(
				plan.sequence.identifier().clone().into_owned(),
				namespace_name,
			));
		};

		// Get the table name from the column's source
		let table_name = match &plan.column.source {
			reifydb_core::interface::identifier::ColumnSource::Source {
				source,
				..
			} => source.text(),
			reifydb_core::interface::identifier::ColumnSource::Alias(alias) => alias.text(),
		};

		let Some(table) = CatalogStore::find_table_by_name(txn, namespace.id, table_name)? else {
			return_error!(table_not_found(
				plan.column.source.as_fragment().clone().into_owned(),
				&namespace.name,
				table_name,
			));
		};

		let Some(column) = CatalogStore::find_column_by_name(txn, table.id, plan.column.name.text())? else {
			return_error!(column_not_found(plan.column.name.clone().into_owned()));
		};

		if !column.auto_increment {
			return_error!(can_not_alter_not_auto_increment(plan.column.name));
		}

		// For catalog operations, use empty params since no
		// ExecutionContext is available
		let empty_params = Params::None;
		let value = evaluate(
			&ColumnEvaluationContext {
				target: Some(TargetColumn::Partial {
					source_name: None,
					column_name: None,
					column_type: column.constraint.get_type(),
					policies: column.policies.into_iter().map(|p| p.policy).collect(),
				}),
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
		ColumnSequence::set_value(txn, table.id, column.id, value.clone())?;

		Ok(Columns::single_row([
			("namespace", Value::Utf8(namespace.name)),
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
		test_utils::ensure_test_namespace,
	};
	use reifydb_core::interface::{
		NamespaceDef, NamespaceId, Params,
		expression::{ConstantExpression::Number, Expression::Constant},
		identifier::{ColumnIdentifier, ColumnSource},
		resolved::{ResolvedNamespace, ResolvedSequence, SequenceDef},
	};
	use reifydb_rql::plan::physical::{AlterSequenceNode, PhysicalPlan};
	use reifydb_type::{Fragment, Type, TypeConstraint, Value};

	use crate::{execute::Executor, test_utils::create_test_command_transaction};

	fn create_test_resolved_sequence(namespace_name: &str, sequence_name: &str) -> ResolvedSequence<'static> {
		let namespace = ResolvedNamespace::new(
			Fragment::owned_internal(namespace_name),
			NamespaceDef {
				id: NamespaceId(1),
				name: namespace_name.to_string(),
			},
		);
		let sequence_def = SequenceDef {
			name: sequence_name.to_string(),
			current_value: 1,
			increment: 1,
		};
		ResolvedSequence::new(Fragment::owned_internal(sequence_name), namespace, sequence_def)
	}

	#[test]
	fn test_ok() {
		let instance = Executor::testing();
		let mut txn = create_test_command_transaction();
		let test_schema = ensure_test_namespace(&mut txn);

		CatalogStore::create_table(
			&mut txn,
			TableToCreate {
				fragment: None,
				namespace: test_schema.id,
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
		let plan = AlterSequenceNode {
			sequence: create_test_resolved_sequence("test_namespace", "users_id_seq"),
			column: ColumnIdentifier {
				source: ColumnSource::Source {
					namespace: Fragment::owned_internal("test_namespace"),
					source: Fragment::owned_internal("users"),
				},
				name: Fragment::owned_internal("id"),
			},
			value: Constant(Number {
				fragment: Fragment::owned_internal("1000"),
			}),
		};

		let result = instance
			.execute_command_plan(&mut txn, PhysicalPlan::AlterSequence(plan), Params::default())
			.unwrap();

		assert_eq!(result.row(0)[0], Value::Utf8("test_namespace".to_string()));
		assert_eq!(result.row(0)[1], Value::Utf8("users".to_string()));
		assert_eq!(result.row(0)[2], Value::Utf8("id".to_string()));
		assert_eq!(result.row(0)[3], Value::Int4(1000));
	}

	#[test]
	fn test_non_auto_increment_column() {
		let instance = Executor::testing();
		let mut txn = create_test_command_transaction();
		let test_schema = ensure_test_namespace(&mut txn);
		CatalogStore::create_table(
			&mut txn,
			TableToCreate {
				fragment: None,
				namespace: test_schema.id,
				table: "items".to_string(),
				columns: vec![TableColumnToCreate {
					fragment: None,
					name: "id".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Int4),
					policies: vec![],
					auto_increment: false,
				}],
			},
		)
		.unwrap();

		// Try to alter sequence on non-auto-increment column
		let plan = AlterSequenceNode {
			sequence: create_test_resolved_sequence("test_namespace", "items_id_seq"),
			column: ColumnIdentifier {
				source: ColumnSource::Source {
					namespace: Fragment::owned_internal("test_namespace"),
					source: Fragment::owned_internal("items"),
				},
				name: Fragment::owned_internal("id"),
			},
			value: Constant(Number {
				fragment: Fragment::owned_internal("100"),
			}),
		};

		let err = instance
			.execute_command_plan(&mut txn, PhysicalPlan::AlterSequence(plan), Params::default())
			.unwrap_err();

		let diagnostic = err.diagnostic();
		assert_eq!(diagnostic.code, "SEQUENCE_002");
	}

	#[test]
	fn test_schema_not_found() {
		let instance = Executor::testing();
		let mut txn = create_test_command_transaction();

		let plan = AlterSequenceNode {
			sequence: create_test_resolved_sequence("non_existent_schema", "some_table_id_seq"),
			column: ColumnIdentifier {
				source: ColumnSource::Source {
					namespace: Fragment::owned_internal("non_existent_schema"),
					source: Fragment::owned_internal("some_table"),
				},
				name: Fragment::owned_internal("id"),
			},
			value: Constant(Number {
				fragment: Fragment::owned_internal("1000"),
			}),
		};

		let err = instance
			.execute_command_plan(&mut txn, PhysicalPlan::AlterSequence(plan), Params::default())
			.unwrap_err();

		assert_eq!(err.diagnostic().code, "CA_002");
	}

	#[test]
	fn test_table_not_found() {
		let instance = Executor::testing();
		let mut txn = create_test_command_transaction();
		ensure_test_namespace(&mut txn);

		let plan = AlterSequenceNode {
			sequence: create_test_resolved_sequence("test_namespace", "non_existent_table_id_seq"),
			column: ColumnIdentifier {
				source: ColumnSource::Source {
					namespace: Fragment::owned_internal("test_namespace"),
					source: Fragment::owned_internal("non_existent_table"),
				},
				name: Fragment::owned_internal("id"),
			},
			value: Constant(Number {
				fragment: Fragment::owned_internal("1000"),
			}),
		};

		let err = instance
			.execute_command_plan(&mut txn, PhysicalPlan::AlterSequence(plan), Params::default())
			.unwrap_err();

		assert_eq!(err.diagnostic().code, "CA_004");
	}

	#[test]
	fn test_column_not_found() {
		let instance = Executor::testing();
		let mut txn = create_test_command_transaction();
		let test_schema = ensure_test_namespace(&mut txn);

		CatalogStore::create_table(
			&mut txn,
			TableToCreate {
				fragment: None,
				namespace: test_schema.id,
				table: "posts".to_string(),
				columns: vec![TableColumnToCreate {
					fragment: None,
					name: "id".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Int4),
					policies: vec![],
					auto_increment: true,
				}],
			},
		)
		.unwrap();

		// Try to alter sequence on non-existent column
		let plan = AlterSequenceNode {
			sequence: create_test_resolved_sequence("test_namespace", "posts_non_existent_column_seq"),
			column: ColumnIdentifier {
				source: ColumnSource::Source {
					namespace: Fragment::owned_internal("test_namespace"),
					source: Fragment::owned_internal("posts"),
				},
				name: Fragment::owned_internal("non_existent_column"),
			},
			value: Constant(Number {
				fragment: Fragment::owned_internal("1000"),
			}),
		};

		let err = instance
			.execute_command_plan(&mut txn, PhysicalPlan::AlterSequence(plan), Params::default())
			.unwrap_err();

		assert_eq!(err.diagnostic().code, "QUERY_001");
	}
}
