// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	testing::TestingContext,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_type::Result;

use super::{extract_optional_string_arg, testing_context_from_ioc};
use crate::{GeneratorContext, GeneratorFunction, error::GeneratorFunctionResult};

pub(crate) struct TestingHandlersInvoked;

impl TestingHandlersInvoked {
	pub fn new() -> Self {
		Self
	}
}

impl GeneratorFunction for TestingHandlersInvoked {
	fn generate<'a>(&self, ctx: GeneratorContext<'a>) -> GeneratorFunctionResult<Columns> {
		let testing = testing_context_from_ioc(ctx.ioc)?;
		let guard = testing.lock();
		let filter_arg = extract_optional_string_arg(&ctx.params);
		Ok(build_handler_invocations(&guard, filter_arg.as_deref())?)
	}
}

fn build_handler_invocations(ctx: &TestingContext, filter_name: Option<&str>) -> Result<Columns> {
	let filter: Option<(&str, &str)> = filter_name.and_then(|s| {
		let parts: Vec<&str> = s.splitn(2, "::").collect();
		if parts.len() == 2 {
			Some((parts[0], parts[1]))
		} else {
			None
		}
	});

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
