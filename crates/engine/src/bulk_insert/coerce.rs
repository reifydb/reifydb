// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::{
	interface::catalog::column::Column,
	value::{
		batch::empty_batch,
		column::{
			cast::cast_column_data,
			factory::from_one,
			write::{check_digest_write, check_digest_write_type},
		},
	},
};
use reifydb_evaluate::{expression::context::EvalContext, stack::SymbolTable};
use reifydb_routine_abi::registry::Routines;
use reifydb_runtime::context::{RuntimeContext, clock::Clock};
use reifydb_value::{
	error::Error,
	fragment::Fragment,
	params::Params,
	value::{Value, column_view::ColumnView, identity::IdentityId},
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
		self.cast(value, column).map_err(|e| with_row_note(e, source_name, row_idx))
	}

	pub(super) fn cast_column(&self, view: &ColumnView<'_>, column: &Column) -> Result<(FieldRef, ArrayRef)> {
		let target = column.constraint.get_type();
		let fragment = || Fragment::internal(&column.name);
		check_digest_write(view, &target, fragment)?;
		cast_column_data(&self.context(view.len()), view, target.inner_type().clone(), fragment)
	}

	fn cast(&self, value: Value, column: &Column) -> Result<Value> {
		let target = column.constraint.get_type();
		let fragment = || Fragment::internal(&column.name);
		check_digest_write_type(&value.get_type(), &target, fragment)?;
		let cast_target = target.inner_type().clone();
		if value.get_type() == cast_target {
			return Ok(value);
		}
		let column = from_one("value", value);
		let casted =
			cast_column_data(&self.context(1), &ColumnView::try_from(&column)?, cast_target, fragment)?;
		Ok(ColumnView::try_from(&casted)?.get_value(0))
	}

	fn context(&self, row_count: usize) -> EvalContext<'_> {
		EvalContext {
			params: &Params::None,
			symbols: &self.symbols,
			routines: &self.routines,
			runtime_context: &self.runtime_context,
			identity: self.identity,
			is_aggregate_context: false,
			batch: empty_batch(),
			row_count,
			target: None,
			take: None,
		}
	}
}

pub(super) fn with_row_note(mut error: Error, source_name: &str, row_idx: usize) -> Error {
	error.0.notes.push(format!("row {} of the bulk insert into `{}`", row_idx + 1, source_name));
	error
}
