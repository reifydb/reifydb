// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::column::Column,
	value::column::{buffer::ColumnBuffer, columns::Columns},
};
use reifydb_routine::routine::registry::Routines;
use reifydb_runtime::context::{RuntimeContext, clock::Clock};
use reifydb_type::{fragment::Fragment, params::Params, value::identity::IdentityId};

use crate::{
	Result,
	expression::{cast::cast_column_data, context::EvalContext},
	vm::stack::SymbolTable,
};

/// Coerce each column's data to the target type in batch.
pub(super) fn coerce_columns(
	column_data: &[ColumnBuffer],
	columns: &[Column],
	num_rows: usize,
) -> Result<Vec<ColumnBuffer>> {
	let runtime_ctx = RuntimeContext::with_clock(Clock::Real);
	let routines = Routines::empty();
	let ctx = EvalContext {
		params: &Params::None,
		symbols: &SymbolTable::new(),
		routines: &routines,
		runtime_context: &runtime_ctx,
		arena: None,
		identity: IdentityId::root(),
		is_aggregate_context: false,
		columns: Columns::empty(),
		row_count: num_rows,
		target: None,
		take: None,
	};

	let mut coerced_columns: Vec<ColumnBuffer> = Vec::with_capacity(columns.len());

	for (col_idx, col) in columns.iter().enumerate() {
		let target = col.constraint.get_type();
		// For Option(T) columns, cast to the inner type T; None values pass through unchanged
		let cast_target = target.inner_type().clone();
		let source_data = &column_data[col_idx];

		let coerced = cast_column_data(&ctx, source_data, cast_target, || Fragment::internal(&col.name))?;
		coerced_columns.push(coerced);
	}

	Ok(coerced_columns)
}
