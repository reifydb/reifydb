// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::RecordBatch;
use reifydb_core::{
	interface::catalog::config::{ConfigKey, GetConfig},
	value::{
		batch::{batch, concat, heap_size},
		column::{builder::ColumnBuilder, factory, headers::ColumnHeaders},
	},
};
use reifydb_evaluate::stack::{SymbolTable, Variable};
use reifydb_rql::query::QueryPlan;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	params::Params,
	value::system_columns::{SystemColumn, with_system_column},
};

use crate::{
	Result,
	vm::{
		services::Services,
		vm::Vm,
		volcano::{
			compile::compile,
			query::{QueryContext, QueryNode, charge_query_memory_bytes, query_budget},
		},
	},
};

impl<'a> Vm<'a> {
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
) -> Result<Option<RecordBatch>> {
	let identity = txn.identity();
	let context = Arc::new(QueryContext {
		services: services.clone(),
		source: None,
		batch_size: services.catalog.get_config_uint2(ConfigKey::QueryRowBatchSize) as u64,
		params,
		symbols: symbols.clone(),
		identity,
		memory: query_budget(services),
	});

	let mut query_node = compile(plan, txn, context.clone());
	query_node.initialize(txn, &context)?;

	let mut batches: Vec<RecordBatch> = Vec::new();
	let mut charged = 0usize;
	let mut total = 0usize;
	let mut mutable_context = (*context).clone();

	while let Some(next) = query_node.next(txn, &mut mutable_context)? {
		total += heap_size(&next)?;
		batches.push(next);
		charge_query_memory_bytes(&context.memory, &mut charged, total)?;
	}

	if batches.is_empty() {
		let headers = query_node.headers().unwrap_or_else(ColumnHeaders::empty);
		let mut user = Vec::new();
		let mut system = Vec::new();
		for name in headers.columns {
			match SystemColumn::from_name(name.text()) {
				Some(column) => system.push(column),
				None => user.push(factory::none(name.text(), 0)),
			}
		}
		let mut empty = batch(user)?;
		for column in system {
			let (_, array) = ColumnBuilder::with_capacity(column.ty(), 0).finish(column.name());
			empty = with_system_column(empty, column, array)?;
		}
		return Ok(Some(empty));
	}

	Ok(Some(concat(&batches)?))
}
