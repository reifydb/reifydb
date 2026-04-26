// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::LazyLock;

use reifydb_core::{
	internal_error,
	testing::CapturedEvent,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	error::Error,
	params::Params,
	value::{Value, r#type::Type},
};

use crate::routine::{ProcedureContext, Routine, RoutineError, RoutineInfo};

static INFO: LazyLock<RoutineInfo> = LazyLock::new(|| RoutineInfo::new("testing::events::dispatched"));

pub struct TestingEventsDispatched;

impl Default for TestingEventsDispatched {
	fn default() -> Self {
		Self::new()
	}
}

impl TestingEventsDispatched {
	pub fn new() -> Self {
		Self
	}
}

impl<'a, 'tx> Routine<ProcedureContext<'a, 'tx>> for TestingEventsDispatched {
	fn info(&self) -> &RoutineInfo {
		&INFO
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Any
	}

	fn execute(&self, ctx: &mut ProcedureContext<'a, 'tx>, _args: &Columns) -> Result<Columns, RoutineError> {
		let events = match ctx.tx {
			Transaction::Test(t) => &**t.events,
			_ => {
				return Err(
					internal_error!("testing::events::dispatched() requires a test transaction").into()
				);
			}
		};
		let filter_arg = extract_optional_string_param(ctx.params);
		Ok(build_dispatched_events(events, filter_arg.as_deref())?)
	}
}

fn extract_optional_string_param(params: &Params) -> Option<String> {
	match params {
		Params::Positional(args) if !args.is_empty() => match &args[0] {
			Value::Utf8(s) => Some(s.clone()),
			_ => None,
		},
		_ => None,
	}
}

fn build_dispatched_events(events: &[CapturedEvent], filter_name: Option<&str>) -> Result<Columns, Error> {
	let filter: Option<(&str, &str)> = filter_name.and_then(|s| {
		let parts: Vec<&str> = s.splitn(2, "::").collect();
		if parts.len() == 2 {
			Some((parts[0], parts[1]))
		} else {
			None
		}
	});

	let events: Vec<_> = events
		.iter()
		.filter(|e| {
			if let Some((ns, name)) = filter {
				e.namespace == ns && e.event == name
			} else {
				true
			}
		})
		.collect();

	if events.is_empty() {
		return Ok(Columns::empty());
	}

	let mut seq_data = ColumnBuffer::uint8_with_capacity(events.len());
	let mut ns_data = ColumnBuffer::utf8_with_capacity(events.len());
	let mut event_data = ColumnBuffer::utf8_with_capacity(events.len());
	let mut variant_data = ColumnBuffer::utf8_with_capacity(events.len());
	let mut depth_data = ColumnBuffer::uint1_with_capacity(events.len());

	let mut field_names: Vec<String> = Vec::new();
	for event in &events {
		for col in event.columns.iter() {
			let name = col.name().text().to_string();
			if !field_names.contains(&name) {
				field_names.push(name);
			}
		}
	}

	let mut field_columns: Vec<Vec<Value>> = vec![Vec::with_capacity(events.len()); field_names.len()];

	for event in &events {
		seq_data.push(event.sequence);
		ns_data.push(event.namespace.as_str());
		event_data.push(event.event.as_str());
		variant_data.push(event.variant.as_str());
		depth_data.push(event.depth);

		for (i, field_name) in field_names.iter().enumerate() {
			let val = event
				.columns
				.column(field_name)
				.map(|col| col.data().get_value(0))
				.unwrap_or(Value::none());
			field_columns[i].push(val);
		}
	}

	let mut columns = vec![
		ColumnWithName::new("sequence", seq_data),
		ColumnWithName::new("namespace", ns_data),
		ColumnWithName::new("event", event_data),
		ColumnWithName::new("variant", variant_data),
		ColumnWithName::new("depth", depth_data),
	];

	for (i, name) in field_names.iter().enumerate() {
		let mut data = column_for_values(&field_columns[i]);
		for val in &field_columns[i] {
			data.push_value(val.clone());
		}
		columns.push(ColumnWithName::new(name.as_str(), data));
	}

	Ok(Columns::new(columns))
}

fn column_for_values(values: &[Value]) -> ColumnBuffer {
	let first_type = values.iter().find_map(|v| {
		if matches!(v, Value::None { .. }) {
			None
		} else {
			Some(v.get_type())
		}
	});
	match first_type {
		Some(ty) => ColumnBuffer::with_capacity(ty, values.len()),
		None => ColumnBuffer::none_typed(Type::Boolean, 0),
	}
}
