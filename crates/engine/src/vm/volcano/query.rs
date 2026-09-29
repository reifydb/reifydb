// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::RecordBatch;
use reifydb_core::{
	error::diagnostic::{operation, query},
	interface::{
		catalog::config::{ConfigKey, GetConfig},
		resolved::ResolvedObject,
	},
	sort::SortKey,
	util::budget::MemoryBudget,
	value::{batch::empty_batch, column::headers::ColumnHeaders},
};
use reifydb_evaluate::{expression::context::EvalContext, stack::SymbolTable};
use reifydb_extension::transform::context::TransformContext;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	byte_size::ByteSize,
	error,
	params::Params,
	value::{column_view::ColumnView, identity::IdentityId, system_columns::check_user_columns},
};

use crate::{Result, vm::services::Services};

pub fn query_budget(services: &Services) -> Arc<MemoryBudget> {
	let limit = services.catalog.get_config_uint8(ConfigKey::QueryMemoryLimit);
	Arc::new(MemoryBudget::new(ByteSize::from_bytes(limit)))
}

pub(crate) fn ensure_sort_key_orderable(key: &SortKey, data: &ColumnView<'_>) -> Result<()> {
	let ty = data.get_type();
	if ty.is_scalar() {
		Ok(())
	} else {
		Err(error!(operation::sort_key_not_orderable(key.column.clone(), ty)))
	}
}

pub(crate) fn charge_query_memory_bytes(budget: &MemoryBudget, charged: &mut usize, total: usize) -> Result<()> {
	if total > *charged {
		let delta = (total - *charged) as u64;
		if !budget.try_charge(ByteSize::from_bytes(delta)) {
			return Err(error!(query::memory_limit_exceeded(budget.used(), budget.limit())));
		}
		*charged = total;
	}
	Ok(())
}

pub trait QueryNode: Send + Sync {
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()>;

	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<RecordBatch>>;

	fn headers(&self) -> Option<ColumnHeaders>;
}

#[derive(Clone)]
pub struct QueryContext {
	pub services: Arc<Services>,
	pub source: Option<ResolvedObject>,
	pub batch_size: u64,
	pub params: Params,
	pub symbols: SymbolTable,
	pub identity: IdentityId,
	pub memory: Arc<MemoryBudget>,
}

impl QueryNode for Box<dyn QueryNode> {
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		(**self).initialize(rx, ctx)
	}

	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<RecordBatch>> {
		let result = (**self).next(rx, ctx)?;
		if let Some(ref batch) = result {
			check_user_columns(batch)?;
		}
		Ok(result)
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		(**self).headers()
	}
}

pub fn eval_context_from_query<'a>(ctx: &'a QueryContext) -> EvalContext<'a> {
	EvalContext {
		target: None,
		batch: empty_batch(),
		row_count: 1,
		take: None,
		params: &ctx.params,
		symbols: &ctx.symbols,
		is_aggregate_context: false,
		routines: &ctx.services.routines,
		runtime_context: &ctx.services.runtime_context,
		identity: ctx.identity,
	}
}

pub fn eval_context_from_transform<'a>(ctx: &'a TransformContext<'a>, stored: &'a QueryContext) -> EvalContext<'a> {
	EvalContext {
		target: None,
		batch: empty_batch(),
		row_count: 1,
		take: None,
		params: ctx.params,
		symbols: &stored.symbols,
		is_aggregate_context: false,
		routines: &stored.services.routines,
		runtime_context: ctx.runtime_context,
		identity: stored.identity,
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		util::budget::MemoryBudget,
		value::{
			batch::{batch, heap_size},
			column::factory::int4,
		},
	};
	use reifydb_value::byte_size::ByteSize;

	use super::charge_query_memory_bytes;

	#[test]
	fn charge_query_memory_delta_charges_and_rejects_over_budget() {
		let budget = MemoryBudget::new(ByteSize::from_kib(1));
		let mut charged = 0usize;

		let small = batch(vec![int4("c", [1i32, 2, 3, 4])]).expect("one int4 column");
		let small_size = heap_size(&small).expect("an int4 batch has a heap size");
		charge_query_memory_bytes(&budget, &mut charged, small_size).expect("small buffer fits under 1 KiB");
		let after_first = budget.used().as_bytes();
		assert!(after_first > 0, "charging a non-empty buffer must consume budget");
		assert_eq!(charged as u64, after_first, "charged must track exactly what the budget recorded");

		charge_query_memory_bytes(&budget, &mut charged, small_size)
			.expect("re-charge of the same buffer is free");
		assert_eq!(
			budget.used().as_bytes(),
			after_first,
			"delta charging must not double count an unchanged buffer"
		);

		let big = batch(vec![int4("c", 0..4000i32)]).expect("one int4 column");
		let mut big_charged = 0usize;
		let big_size = heap_size(&big).expect("an int4 batch has a heap size");
		let err = charge_query_memory_bytes(&budget, &mut big_charged, big_size).unwrap_err();
		assert_eq!(err.0.code, "QUERY_006", "an over-budget charge must raise the memory-limit diagnostic");
	}
}
