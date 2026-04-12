// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData, headers::ColumnHeaders};
use reifydb_rql::query::QueryPlan;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{params::Params, value::r#type::Type};

use crate::{
	Result,
	arena::QueryArena,
	vm::{
		services::Services,
		stack::{SymbolTable, Variable},
		vm::Vm,
		volcano::{
			compile::compile,
			query::{QueryContext, QueryNode},
		},
	},
};

impl Vm {
	pub(crate) fn exec_query(
		&mut self,
		services: &Arc<Services>,
		tx: &mut Transaction<'_>,
		plan: &QueryPlan,
		params: &Params,
	) -> Result<()> {
		let mut std_txn = tx.reborrow();
		if let Some(columns) =
			run_query_plan(services, &mut std_txn, plan.clone(), params.clone(), &mut self.symbols)?
		{
			self.stack.push(Variable::columns(columns));
		}
		Ok(())
	}
}

pub(crate) fn run_query_plan(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: QueryPlan,
	params: Params,
	symbols: &mut SymbolTable,
) -> Result<Option<Columns>> {
	let identity = txn.identity();
	let context = Arc::new(QueryContext {
		services: services.clone(),
		source: None,
		batch_size: 1024,
		params,
		symbols: symbols.clone(),
		identity,
	});

	let mut query_node = compile(plan, txn, context.clone());
	query_node.initialize(txn, &context)?;

	let mut all_columns: Option<Columns> = None;
	let mut mutable_context = (*context).clone();
	let mut arena = QueryArena::new();

	while let Some(batch) = query_node.next(txn, &mut mutable_context)? {
		match &mut all_columns {
			None => all_columns = Some(batch),
			Some(existing) => existing.append_columns(batch)?,
		}
		arena.reset();
	}

	if all_columns.is_none() {
		let headers = query_node.headers().unwrap_or_else(ColumnHeaders::empty);
		let empty_columns: Vec<Column> = headers
			.columns
			.into_iter()
			.map(|name| Column {
				name,
				data: ColumnData::none_typed(Type::Boolean, 0),
			})
			.collect();
		return Ok(Some(Columns::new(empty_columns)));
	}

	Ok(all_columns)
}
