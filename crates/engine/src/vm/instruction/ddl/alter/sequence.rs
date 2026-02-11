// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	error::diagnostic::sequence::can_not_alter_not_auto_increment,
	interface::{evaluate::TargetColumn, resolved::ResolvedPrimitive},
	value::column::columns::Columns,
};
use reifydb_rql::nodes::AlterSequenceNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::{params::Params, return_error, value::Value};

use crate::{
	expression::{context::EvalContext, eval::evaluate},
	vm::{services::Services, stack::SymbolTable},
};

pub(crate) fn alter_table_sequence<'a>(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: AlterSequenceNode,
) -> crate::Result<Columns> {
	// let namespace_name = plan.sequence.namespace().name();
	// let Some(namespace) = CatalogStore::find_namespace_by_name(txn, namespace_name)? else {
	// 	return_error!(namespace_not_found(
	// 		plan.sequence.identifier().clone(),
	// 		namespace_name,
	// 	));
	// };

	// Get the table from the resolved column's source
	let table = match plan.column.primitive() {
		ResolvedPrimitive::Table(t) => t.def().clone(),
		_ => unimplemented!(),
	};

	// The column is already resolved, so we can use its def directly
	let column = plan.column.def().clone();

	if !column.auto_increment {
		return_error!(can_not_alter_not_auto_increment(plan.column.identifier().clone()));
	}

	// For catalog operations, use empty params since no
	// ExecutionContext is available
	use std::sync::LazyLock;
	static EMPTY_PARAMS: LazyLock<Params> = LazyLock::new(|| Params::None);
	static EMPTY_SYMBOL_TABLE: LazyLock<SymbolTable> = LazyLock::new(|| SymbolTable::new());

	let value = evaluate(
		&EvalContext {
			target: Some(TargetColumn::Partial {
				source_name: None,
				column_name: None,
				column_type: column.constraint.get_type(),
				policies: column.policies.into_iter().map(|p| p.policy).collect(),
			}),
			columns: Columns::empty(),
			row_count: 1,
			take: None,
			params: &EMPTY_PARAMS,
			symbol_table: &EMPTY_SYMBOL_TABLE,
			is_aggregate_context: false,
			functions: &services.functions,
			clock: &services.clock,
		},
		&plan.value,
		&services.functions,
		&services.clock,
	)?;

	let data = value.data();
	debug_assert_eq!(data.len(), 1);

	let value = data.get_value(0);
	services.catalog.column_sequence_set_value(txn, table.id, column.id, value.clone())?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.sequence.namespace().name().to_string())),
		("table", Value::Utf8(table.name)),
		("column", Value::Utf8(column.name)),
		("value", value),
	]))
}
