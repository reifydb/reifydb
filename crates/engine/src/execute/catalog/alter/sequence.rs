// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::sequence::ColumnSequence;
use reifydb_core::{
	interface::{ColumnEvaluationContext, Params, TargetColumn, Transaction, resolved::ResolvedSource},
	value::column::Columns,
};
use reifydb_rql::plan::physical::AlterSequenceNode;
use reifydb_type::{Value, diagnostic::sequence::can_not_alter_not_auto_increment, return_error};

use crate::{StandardCommandTransaction, evaluate::column::evaluate, execute::Executor};

impl Executor {
	pub(crate) fn alter_table_sequence<'a, T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		plan: AlterSequenceNode,
	) -> crate::Result<Columns<'a>> {
		// let namespace_name = plan.sequence.namespace().name();
		// let Some(namespace) = CatalogStore::find_namespace_by_name(txn, namespace_name)? else {
		// 	return_error!(namespace_not_found(
		// 		plan.sequence.identifier().clone().into_owned(),
		// 		namespace_name,
		// 	));
		// };

		// Get the table from the resolved column's source
		let table = match plan.column.source() {
			ResolvedSource::Table(t) => t.def().clone(),
			_ => unimplemented!(),
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
			("namespace", Value::Utf8(plan.sequence.namespace().name().to_string())),
			("table", Value::Utf8(table.name)),
			("column", Value::Utf8(column.name)),
			("value", value),
		]))
	}
}
