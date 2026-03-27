// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::function::registry::FunctionsBuilder;
use reifydb_core::value::column::{columns::Columns, data::ColumnData};
use reifydb_type::value::{Value, r#type::Type};

mod changed;
mod event;
mod handler;

use changed::TestingChanged;
use event::TestingEventsDispatched;
use handler::TestingHandlersInvoked;

pub fn register_testing_functions(builder: FunctionsBuilder) -> FunctionsBuilder {
	builder.register_generator("testing::events::dispatched", TestingEventsDispatched::new)
		.register_generator("testing::handlers::invoked", TestingHandlersInvoked::new)
		.register_generator("testing::tables::changed", || TestingChanged::new("tables"))
		.register_generator("testing::views::changed", || TestingChanged::new("views"))
		.register_generator("testing::series::changed", || TestingChanged::new("series"))
		.register_generator("testing::ringbuffers::changed", || TestingChanged::new("ringbuffers"))
		.register_generator("testing::dictionaries::changed", || TestingChanged::new("dictionaries"))
}

pub(crate) fn extract_optional_string_arg(params: &Columns) -> Option<String> {
	if params.is_empty() {
		return None;
	}
	let col = params.iter().next()?;
	if col.data().len() == 0 {
		return None;
	}
	match col.data().get_value(0) {
		Value::Utf8(s) => Some(s),
		_ => None,
	}
}

pub(crate) fn column_for_values(values: &[Value]) -> ColumnData {
	let first_type = values.iter().find_map(|v| {
		if matches!(v, Value::None { .. }) {
			None
		} else {
			Some(v.get_type())
		}
	});
	match first_type {
		Some(ty) => ColumnData::with_capacity(ty, values.len()),
		None => ColumnData::none_typed(Type::Boolean, 0),
	}
}
