// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use catalog::namespace_not_found;
use reifydb_catalog::{CatalogStore, sequence::ColumnSequence};
use reifydb_core::{
	interface::{EvaluationContext, Params, TargetColumn, Transaction},
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

use crate::{StandardCommandTransaction, evaluate::evaluate, execute::Executor};

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

		// Get the table from the resolved column's source
		let table = match plan.column.source() {
			reifydb_core::interface::resolved::ResolvedSource::Table(t) => t.def().clone(),
			_ => {
				// In a real implementation, we'd handle other source types
				// For now, just return an error
				return_error!(table_not_found(
					plan.column.identifier().clone().into_owned(),
					&namespace.name,
					"_unknown_",
				));
			}
		};

		// The column is already resolved, so we can use its def directly
		let column = plan.column.def().clone();

		if !column.auto_increment {
			return_error!(can_not_alter_not_auto_increment(plan.column.identifier().clone().into_owned()));
		}

		// For catalog operations, use empty params since no
		// ExecutionContext is available
		let empty_params = Params::None;
		let value = evaluate(
			&EvaluationContext {
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
		ColumnDef, ColumnId, NamespaceDef, NamespaceId, Params, TableDef, TableId,
		catalog::ColumnIndex,
		expression::{ConstantExpression::Number, Expression::Constant},
		resolved::{
			ResolvedColumn, ResolvedNamespace, ResolvedSequence, ResolvedSource, ResolvedTable, SequenceDef,
		},
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

	fn create_test_resolved_column(
		namespace_name: &str,
		table_name: &str,
		column_name: &str,
		auto_increment: bool,
	) -> ResolvedColumn<'static> {
		let namespace = ResolvedNamespace::new(
			Fragment::owned_internal(namespace_name),
			NamespaceDef {
				id: NamespaceId(1),
				name: namespace_name.to_string(),
			},
		);

		let table_def = TableDef {
			id: TableId(1),
			namespace: NamespaceId(1),
			name: table_name.to_string(),
			columns: vec![ColumnDef {
				id: ColumnId(1),
				name: column_name.to_string(),
				constraint: TypeConstraint::unconstrained(Type::Int8),
				policies: vec![],
				index: ColumnIndex(0),
				auto_increment,
			}],
			primary_key: None,
		};

		let resolved_table = ResolvedTable::new(Fragment::owned_internal(table_name), namespace, table_def);

		let column_def = ColumnDef {
			id: ColumnId(1),
			name: column_name.to_string(),
			constraint: TypeConstraint::unconstrained(Type::Int8),
			policies: vec![],
			index: ColumnIndex(0),
			auto_increment,
		};

		ResolvedColumn::new(
			Fragment::owned_internal(column_name),
			ResolvedSource::Table(resolved_table),
			column_def,
		)
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
			column: create_test_resolved_column("test_namespace", "users", "id", true),
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
			column: create_test_resolved_column("test_namespace", "items", "id", false),
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
			column: create_test_resolved_column("non_existent_schema", "some_table", "id", true),
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
			column: create_test_resolved_column("test_namespace", "non_existent_table", "id", true),
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
			column: create_test_resolved_column("test_namespace", "posts", "non_existent_column", true),
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
