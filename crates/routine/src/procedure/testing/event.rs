// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::FieldRef;
use reifydb_core::{
	internal_error,
	testing::CapturedEvent,
	value::{
		batch::{batch, empty_batch},
		column::{builder::ColumnBuilder, factory},
	},
};
use reifydb_routine_abi::{Routine, RoutineInfo, context::ProcedureContext, error::RoutineError};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	error::Error,
	params::Params,
	value::{
		Value,
		column_view::ColumnView,
		system_columns::{column_view, user_columns},
		value_type::ValueType,
	},
};

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

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Any
	}

	fn execute(
		&self,
		ctx: &mut ProcedureContext<'a, 'tx>,
		_args: &[(FieldRef, ArrayRef)],
	) -> Result<RecordBatch, RoutineError> {
		let events = match ctx.tx {
			Transaction::Test(t) => &**t.events,
			_ => {
				return Err(internal_error!(
					"testing::events::dispatched() requires a test transaction"
				)
				.into());
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

fn build_dispatched_events(events: &[CapturedEvent], filter_name: Option<&str>) -> Result<RecordBatch, Error> {
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
		return Ok(empty_batch());
	}

	let mut seq_data = ColumnBuilder::with_capacity(ValueType::Uint8, events.len());
	let mut ns_data = ColumnBuilder::with_capacity(ValueType::Utf8, events.len());
	let mut event_data = ColumnBuilder::with_capacity(ValueType::Utf8, events.len());
	let mut variant_data = ColumnBuilder::with_capacity(ValueType::Utf8, events.len());
	let mut depth_data = ColumnBuilder::with_capacity(ValueType::Uint1, events.len());

	let mut field_names: Vec<String> = Vec::new();
	for event in &events {
		for (field, _) in user_columns(&event.columns) {
			let name = field.name().to_string();
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
			let val = column_view(&event.columns, field_name)?
				.map(|col| col.get_value(0))
				.unwrap_or(Value::none());
			field_columns[i].push(val);
		}
	}

	let mut columns = vec![
		seq_data.finish("sequence"),
		ns_data.finish("namespace"),
		event_data.finish("event"),
		variant_data.finish("variant"),
		depth_data.finish("depth"),
	];

	for (i, name) in field_names.iter().enumerate() {
		let mut data = column_for_values(&field_columns[i])?;
		for val in &field_columns[i] {
			data.push_value(val.clone());
		}
		columns.push(data.finish(name.as_str()));
	}

	batch(columns)
}

fn column_for_values(values: &[Value]) -> Result<ColumnBuilder, Error> {
	let first_type = values.iter().find_map(|v| {
		if matches!(v, Value::None { .. }) {
			None
		} else {
			Some(v.get_type())
		}
	});
	match first_type {
		Some(ty) => Ok(ColumnBuilder::with_capacity(ty, values.len())),
		None => {
			let none = factory::none("", 0);
			Ok(ColumnBuilder::from_view(&ColumnView::try_from(&none)?))
		}
	}
}
