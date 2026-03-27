// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::function::{GeneratorContext, GeneratorFunction, error::GeneratorFunctionResult};
use reifydb_core::{
	internal_error,
	testing::CapturedEvent,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{Result, value::Value};

use super::{column_for_values, extract_optional_string_arg};

pub(crate) struct TestingEventsDispatched;

impl TestingEventsDispatched {
	pub fn new() -> Self {
		Self
	}
}

impl GeneratorFunction for TestingEventsDispatched {
	fn generate<'a>(&self, ctx: GeneratorContext<'a>) -> GeneratorFunctionResult<Columns> {
		let events = match ctx.txn {
			Transaction::Test(t) => &**t.events,
			_ => {
				return Err(internal_error!(
					"testing::events::dispatched() requires a test transaction"
				)
				.into());
			}
		};
		let filter_arg = extract_optional_string_arg(&ctx.params);
		Ok(build_dispatched_events(events, filter_arg.as_deref())?)
	}
}

fn build_dispatched_events(events: &[CapturedEvent], filter_name: Option<&str>) -> Result<Columns> {
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

	let mut seq_data = ColumnData::uint8_with_capacity(events.len());
	let mut ns_data = ColumnData::utf8_with_capacity(events.len());
	let mut event_data = ColumnData::utf8_with_capacity(events.len());
	let mut variant_data = ColumnData::utf8_with_capacity(events.len());
	let mut depth_data = ColumnData::uint1_with_capacity(events.len());

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
		Column::new("sequence", seq_data),
		Column::new("namespace", ns_data),
		Column::new("event", event_data),
		Column::new("variant", variant_data),
		Column::new("depth", depth_data),
	];

	for (i, name) in field_names.iter().enumerate() {
		let mut data = column_for_values(&field_columns[i]);
		for val in &field_columns[i] {
			data.push_value(val.clone());
		}
		columns.push(Column::new(name.as_str(), data));
	}

	Ok(Columns::new(columns))
}
