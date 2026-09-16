// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::column::Column,
	value::column::{
		buffer::{ColumnBuffer, write::check_digest_write_type},
		cast::cast_column_data,
		columns::Columns,
	},
};
use reifydb_evaluate::{expression::context::EvalContext, stack::SymbolTable};
use reifydb_routine_abi::registry::Routines;
use reifydb_runtime::context::{RuntimeContext, clock::Clock};
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	value::{Value, identity::IdentityId},
};

use crate::Result;

pub(super) struct RowCoercer {
	runtime_context: RuntimeContext,
	routines: Routines,
	symbols: SymbolTable,
	identity: IdentityId,
}

impl RowCoercer {
	pub(super) fn new(identity: IdentityId) -> Self {
		Self {
			runtime_context: RuntimeContext::with_clock(Clock::Real),
			routines: Routines::empty(),
			symbols: SymbolTable::new(),
			identity,
		}
	}

	pub(super) fn coerce(&self, value: Value, column: &Column, source_name: &str, row_idx: usize) -> Result<Value> {
		if matches!(value, Value::None { .. }) {
			return Ok(value);
		}
		self.cast(value, column).map_err(|mut e| {
			e.0.notes.push(format!("row {} of the bulk insert into `{}`", row_idx + 1, source_name));
			e
		})
	}

	fn cast(&self, value: Value, column: &Column) -> Result<Value> {
		let target = column.constraint.get_type();
		let fragment = || Fragment::internal(&column.name);
		check_digest_write_type(&value.get_type(), &target, fragment)?;
		let cast_target = target.inner_type().clone();
		if value.get_type() == cast_target {
			return Ok(value);
		}
		let ctx = EvalContext {
			params: &Params::None,
			symbols: &self.symbols,
			routines: &self.routines,
			runtime_context: &self.runtime_context,
			identity: self.identity,
			is_aggregate_context: false,
			columns: Columns::empty(),
			row_count: 1,
			target: None,
			take: None,
		};
		Ok(cast_column_data(&ctx, &ColumnBuffer::from(value), cast_target, fragment)?.get_value(0))
	}
}
