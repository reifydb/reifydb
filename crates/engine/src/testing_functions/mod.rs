// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	internal_error,
	testing::{MutationRecord, TestingContext},
	util::ioc::IocContainer,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::{testing::TestingViewMutationCaptor, transaction::Transaction};
use reifydb_type::value::{Value, r#type::Type};

use crate::Result;

/// Handle a built-in `testing::*` function call.
///
/// Returns `Columns` to be pushed onto the VM stack.
pub(crate) fn handle_testing_call(
	func_name: &str,
	args: &[Value],
	testing: &Option<TestingContext>,
	ioc: &IocContainer,
	tx: &mut Transaction<'_>,
) -> Result<Columns> {
	let base_ctx = testing
		.as_ref()
		.ok_or_else(|| internal_error!("testing::* functions require an active test context"))?;

	match func_name {
		"testing::events::dispatched" => build_dispatched_events(base_ctx, args),
		"testing::handlers::invoked" => build_handler_invocations(base_ctx, args),
		"testing::tables::changed" => build_mutations(base_ctx, args, "tables"),
		"testing::views::changed" => {
			maybe_flush_view_mutations(ioc, tx)?;
			let view_ctx = active_view_testing_context(base_ctx, tx);
			build_mutations(view_ctx, args, "views")
		}
		"testing::series::changed" => build_mutations(base_ctx, args, "series"),
		"testing::ringbuffers::changed" => build_mutations(base_ctx, args, "ringbuffers"),
		"testing::dictionaries::changed" => build_mutations(base_ctx, args, "dictionaries"),
		_ => Err(internal_error!("Unknown testing function: {}", func_name)),
	}
}

fn maybe_flush_view_mutations(ioc: &IocContainer, tx: &mut Transaction<'_>) -> Result<()> {
	let Ok(flusher) = ioc.resolve::<Arc<dyn TestingViewMutationCaptor>>() else {
		return Ok(());
	};

	match tx {
		Transaction::Admin(admin) => flusher.capture(admin)?,
		Transaction::Subscription(sub) => flusher.capture(sub.as_admin_mut())?,
		_ => {}
	}

	Ok(())
}

fn active_view_testing_context<'a>(base: &'a TestingContext, tx: &'a Transaction<'_>) -> &'a TestingContext {
	match tx {
		Transaction::Admin(admin) => admin.testing.as_ref().unwrap_or(base),
		Transaction::Command(cmd) => cmd.testing.as_ref().unwrap_or(base),
		Transaction::Query(_) => base,
		Transaction::Subscription(sub) => sub.as_admin().testing.as_ref().unwrap_or(base),
	}
}

fn build_dispatched_events(ctx: &TestingContext, args: &[Value]) -> Result<Columns> {
	// Optional filter by event type name (e.g., "app::order")
	let filter: Option<(&str, &str)> = if let Some(Value::Utf8(s)) = args.first() {
		let parts: Vec<&str> = s.splitn(2, "::").collect();
		if parts.len() == 2 {
			Some((parts[0], parts[1]))
		} else {
			None
		}
	} else {
		None
	};

	let events: Vec<_> = ctx
		.events
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

	// Build metadata columns
	let mut seq_data = ColumnData::uint8_with_capacity(events.len());
	let mut ns_data = ColumnData::utf8_with_capacity(events.len());
	let mut event_data = ColumnData::utf8_with_capacity(events.len());
	let mut variant_data = ColumnData::utf8_with_capacity(events.len());
	let mut depth_data = ColumnData::uint1_with_capacity(events.len());

	// Collect all unique field names across events (preserving first-seen order)
	let mut field_names: Vec<String> = Vec::new();
	for event in &events {
		for col in event.columns.iter() {
			let name = col.name().text().to_string();
			if !field_names.contains(&name) {
				field_names.push(name);
			}
		}
	}

	// Build field columns
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

fn build_handler_invocations(ctx: &TestingContext, args: &[Value]) -> Result<Columns> {
	// Optional filter by handler name (e.g., "ns::handler_name")
	let filter: Option<(&str, &str)> = if let Some(Value::Utf8(s)) = args.first() {
		let parts: Vec<&str> = s.splitn(2, "::").collect();
		if parts.len() == 2 {
			Some((parts[0], parts[1]))
		} else {
			None
		}
	} else {
		None
	};

	let invocations: Vec<_> = ctx
		.handler_invocations
		.iter()
		.filter(|inv| {
			if let Some((ns, name)) = filter {
				inv.namespace == ns && inv.handler == name
			} else {
				true
			}
		})
		.collect();

	if invocations.is_empty() {
		return Ok(Columns::empty());
	}

	let mut seq_data = ColumnData::uint8_with_capacity(invocations.len());
	let mut ns_data = ColumnData::utf8_with_capacity(invocations.len());
	let mut handler_data = ColumnData::utf8_with_capacity(invocations.len());
	let mut event_data = ColumnData::utf8_with_capacity(invocations.len());
	let mut variant_data = ColumnData::utf8_with_capacity(invocations.len());
	let mut duration_data = ColumnData::uint8_with_capacity(invocations.len());
	let mut outcome_data = ColumnData::utf8_with_capacity(invocations.len());
	let mut message_data = ColumnData::utf8_with_capacity(invocations.len());

	for inv in &invocations {
		seq_data.push(inv.sequence);
		ns_data.push(inv.namespace.as_str());
		handler_data.push(inv.handler.as_str());
		event_data.push(inv.event.as_str());
		variant_data.push(inv.variant.as_str());
		duration_data.push(inv.duration_ns);
		outcome_data.push(inv.outcome.as_str());
		message_data.push(inv.message.as_str());
	}

	Ok(Columns::new(vec![
		Column::new("sequence", seq_data),
		Column::new("namespace", ns_data),
		Column::new("handler", handler_data),
		Column::new("event", event_data),
		Column::new("variant", variant_data),
		Column::new("duration", duration_data),
		Column::new("outcome", outcome_data),
		Column::new("message", message_data),
	]))
}

/// Create a ColumnData with the correct type for a slice of dynamic values.
/// Inspects the first non-none value to determine the column type.
fn column_for_values(values: &[Value]) -> ColumnData {
	let first_type = values.iter().find_map(|v| {
		if matches!(v, Value::None { .. }) {
			None
		} else {
			Some(v.get_type())
		}
	});
	match first_type {
		Some(ty) => ColumnData::with_capacity(ty, values.len()),
		// All none — use Boolean as the inner type (doesn't matter since all values are none)
		None => ColumnData::none_typed(Type::Boolean, 0),
	}
}

fn build_mutations(ctx: &TestingContext, args: &[Value], primitive_type: &str) -> Result<Columns> {
	// Collect (target_label, record) pairs — either single-key or all keys for this type
	let entries: Vec<(&str, &MutationRecord)> = if let Some(Value::Utf8(s)) = args.first() {
		// With arg: look up prefixed key
		let full_key = format!("{}::{}", primitive_type, s);
		match ctx.mutations.get(&full_key) {
			Some(records) => records.iter().map(|r| (s.as_str(), r)).collect(),
			None => return Ok(Columns::empty()),
		}
	} else {
		// Without arg: iterate all keys starting with "{primitive_type}::"
		let prefix = format!("{}::", primitive_type);
		let mut all: Vec<(&str, &MutationRecord)> = Vec::new();
		for (key, records) in &ctx.mutations {
			if let Some(target) = key.strip_prefix(&prefix) {
				for rec in records {
					all.push((target, rec));
				}
			}
		}
		all.sort_by_key(|(_, r)| r.sequence);
		all
	};

	if entries.is_empty() {
		return Ok(Columns::empty());
	}

	// Build metadata columns
	let mut seq_data = ColumnData::uint8_with_capacity(entries.len());
	let mut op_data = ColumnData::utf8_with_capacity(entries.len());
	let mut target_data = ColumnData::utf8_with_capacity(entries.len());

	// Collect all field names from old and new columns
	let mut field_names: Vec<String> = Vec::new();
	for (_, rec) in &entries {
		for col in rec.old.iter() {
			let name = col.name().text().to_string();
			if !field_names.contains(&name) {
				field_names.push(name);
			}
		}
		for col in rec.new.iter() {
			let name = col.name().text().to_string();
			if !field_names.contains(&name) {
				field_names.push(name);
			}
		}
	}

	// Build old_/new_ columns
	let mut old_columns: Vec<Vec<Value>> = vec![Vec::with_capacity(entries.len()); field_names.len()];
	let mut new_columns: Vec<Vec<Value>> = vec![Vec::with_capacity(entries.len()); field_names.len()];

	for (target, rec) in &entries {
		seq_data.push(rec.sequence);
		op_data.push(rec.op.as_str());
		target_data.push(*target);

		for (i, field_name) in field_names.iter().enumerate() {
			let old_val =
				rec.old.column(field_name).map(|col| col.data().get_value(0)).unwrap_or(Value::none());
			old_columns[i].push(old_val);

			let new_val =
				rec.new.column(field_name).map(|col| col.data().get_value(0)).unwrap_or(Value::none());
			new_columns[i].push(new_val);
		}
	}

	let mut columns =
		vec![Column::new("sequence", seq_data), Column::new("op", op_data), Column::new("target", target_data)];

	for (i, name) in field_names.iter().enumerate() {
		let mut old_data = column_for_values(&old_columns[i]);
		for val in &old_columns[i] {
			old_data.push_value(val.clone());
		}
		columns.push(Column::new(format!("old_{}", name), old_data));

		let mut new_data = column_for_values(&new_columns[i]);
		for val in &new_columns[i] {
			new_data.push_value(val.clone());
		}
		columns.push(Column::new(format!("new_{}", name), new_data));
	}

	Ok(Columns::new(columns))
}
