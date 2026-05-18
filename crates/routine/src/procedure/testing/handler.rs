// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::LazyLock;

use reifydb_core::{
	internal_error,
	testing::CapturedInvocation,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	error::Error,
	params::Params,
	value::{Value, r#type::Type},
};

use crate::routine::{Routine, RoutineInfo, context::ProcedureContext, error::RoutineError};

static INFO: LazyLock<RoutineInfo> = LazyLock::new(|| RoutineInfo::new("testing::handlers::invoked"));

pub struct TestingHandlersInvoked;

impl Default for TestingHandlersInvoked {
	fn default() -> Self {
		Self::new()
	}
}

impl TestingHandlersInvoked {
	pub fn new() -> Self {
		Self
	}
}

impl<'a, 'tx> Routine<ProcedureContext<'a, 'tx>> for TestingHandlersInvoked {
	fn info(&self) -> &RoutineInfo {
		&INFO
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Any
	}

	fn execute(&self, ctx: &mut ProcedureContext<'a, 'tx>, _args: &Columns) -> Result<Columns, RoutineError> {
		let invocations = match ctx.tx {
			Transaction::Test(t) => &**t.invocations,
			_ => {
				return Err(internal_error!(
					"testing::handlers::invoked() requires a test transaction"
				)
				.into());
			}
		};
		let filter_arg = extract_optional_string_param(ctx.params);
		Ok(build_invocations(invocations, filter_arg.as_deref())?)
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

fn build_invocations(invocations: &[CapturedInvocation], filter_name: Option<&str>) -> Result<Columns, Error> {
	let filter: Option<(&str, &str)> = filter_name.and_then(|s| {
		let parts: Vec<&str> = s.splitn(2, "::").collect();
		if parts.len() == 2 {
			Some((parts[0], parts[1]))
		} else {
			None
		}
	});

	let invocations: Vec<_> = invocations
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

	let mut seq_data = ColumnBuffer::uint8_with_capacity(invocations.len());
	let mut ns_data = ColumnBuffer::utf8_with_capacity(invocations.len());
	let mut handler_data = ColumnBuffer::utf8_with_capacity(invocations.len());
	let mut event_data = ColumnBuffer::utf8_with_capacity(invocations.len());
	let mut variant_data = ColumnBuffer::utf8_with_capacity(invocations.len());
	let mut duration_data = ColumnBuffer::uint8_with_capacity(invocations.len());
	let mut outcome_data = ColumnBuffer::utf8_with_capacity(invocations.len());
	let mut message_data = ColumnBuffer::utf8_with_capacity(invocations.len());

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
		ColumnWithName::new("sequence", seq_data),
		ColumnWithName::new("namespace", ns_data),
		ColumnWithName::new("handler", handler_data),
		ColumnWithName::new("event", event_data),
		ColumnWithName::new("variant", variant_data),
		ColumnWithName::new("duration", duration_data),
		ColumnWithName::new("outcome", outcome_data),
		ColumnWithName::new("message", message_data),
	]))
}
