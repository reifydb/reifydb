// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	error::diagnostic::query,
	interface::{
		catalog::config::{ConfigKey, GetConfig},
		resolved::ResolvedShape,
	},
	util::budget::MemoryBudget,
	value::column::{columns::Columns, headers::ColumnHeaders},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{byte_size::ByteSize, error, params::Params, value::identity::IdentityId};

use crate::{
	Result,
	vm::{services::Services, stack::SymbolTable},
};

pub fn query_budget(services: &Services) -> Arc<MemoryBudget> {
	let limit = services.catalog.get_config_uint8(ConfigKey::QueryMemoryLimit);
	Arc::new(MemoryBudget::new(ByteSize::from_bytes(limit)))
}

pub(crate) fn charge_query_memory(budget: &MemoryBudget, charged: &mut usize, buffer: &Columns) -> Result<()> {
	let total = buffer.heap_size();
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

	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<Columns>>;

	fn headers(&self) -> Option<ColumnHeaders>;
}

#[derive(Clone)]
pub struct QueryContext {
	pub services: Arc<Services>,
	pub source: Option<ResolvedShape>,
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

	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<Columns>> {
		let result = (**self).next(rx, ctx)?;
		if let Some(ref columns) = result {
			columns.assert_invariants("QueryNode::next output");
		}
		Ok(result)
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		(**self).headers()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		util::budget::MemoryBudget,
		value::column::{ColumnWithName, columns::Columns},
	};
	use reifydb_value::byte_size::ByteSize;

	use super::charge_query_memory;

	#[test]
	fn charge_query_memory_delta_charges_and_rejects_over_budget() {
		let budget = MemoryBudget::new(ByteSize::from_kib(1));
		let mut charged = 0usize;

		let small = Columns::new(vec![ColumnWithName::int4("c", [1i32, 2, 3, 4])]);
		charge_query_memory(&budget, &mut charged, &small).expect("small buffer fits under 1 KiB");
		let after_first = budget.used().as_bytes();
		assert!(after_first > 0, "charging a non-empty buffer must consume budget");
		assert_eq!(charged as u64, after_first, "charged must track exactly what the budget recorded");

		charge_query_memory(&budget, &mut charged, &small).expect("re-charge of the same buffer is free");
		assert_eq!(
			budget.used().as_bytes(),
			after_first,
			"delta charging must not double count an unchanged buffer"
		);

		let big = Columns::new(vec![ColumnWithName::int4("c", 0..4000i32)]);
		let mut big_charged = 0usize;
		let err = charge_query_memory(&budget, &mut big_charged, &big).unwrap_err();
		assert_eq!(err.0.code, "QUERY_006", "an over-budget charge must raise the memory-limit diagnostic");
	}
}
