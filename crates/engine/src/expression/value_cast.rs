// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use reifydb_core::{
	interface::evaluate::ValueCast,
	value::column::{buffer::ColumnBuffer, columns::Columns},
};
use reifydb_routine::routine::registry::Routines;
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	value::{Value, identity::IdentityId, value_type::ValueType},
};

use crate::{
	expression::{cast::cast_column_data, context::EvalContext},
	vm::stack::SymbolTable,
};

pub struct EngineValueCast {
	routines: Routines,
	runtime_context: RuntimeContext,
}

impl EngineValueCast {
	pub fn new(routines: Routines, runtime_context: RuntimeContext) -> Self {
		Self {
			routines,
			runtime_context,
		}
	}
}

impl ValueCast for EngineValueCast {
	fn cast(&self, value: Value, target: &ValueType) -> reifydb_value::Result<Value> {
		if value.get_type() == *target {
			return Ok(value);
		}

		static EMPTY_PARAMS: LazyLock<Params> = LazyLock::new(|| Params::None);
		static EMPTY_SYMBOL_TABLE: LazyLock<SymbolTable> = LazyLock::new(SymbolTable::new);

		let base = EvalContext {
			params: &EMPTY_PARAMS,
			symbols: &EMPTY_SYMBOL_TABLE,
			routines: &self.routines,
			runtime_context: &self.runtime_context,
			arena: None,
			identity: IdentityId::root(),
			is_aggregate_context: false,
			columns: Columns::empty(),
			row_count: 1,
			target: None,
			take: None,
		};
		let eval_ctx = base.with_eval_empty();
		let data = ColumnBuffer::from(value.clone());
		let display = value.to_string();
		let cast = cast_column_data(&eval_ctx, &data, target.clone(), || Fragment::internal(display.clone()))?;
		Ok(cast.get_value(0))
	}
}
