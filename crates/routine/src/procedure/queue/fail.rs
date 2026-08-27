// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use reifydb_core::{interface::catalog::queue::AttemptOutcome, value::column::columns::Columns};
use reifydb_routine_abi::{Routine, RoutineInfo, context::ProcedureContext, error::RoutineError};
use reifydb_value::value::value_type::ValueType;
use tracing::{field::Empty, instrument};

use crate::procedure::{
	identity::set_attribute::extract_args,
	queue::{
		ack::{optional_utf8_arg, record_outcome},
		require_command_transaction, utf8_arg,
	},
};

static INFO: LazyLock<RoutineInfo> = LazyLock::new(|| RoutineInfo::new("queue::fail"));

const PROCEDURE: &str = "queue::fail";

pub struct QueueFail;

impl Default for QueueFail {
	fn default() -> Self {
		Self::new()
	}
}

impl QueueFail {
	pub fn new() -> Self {
		Self
	}
}

impl<'a, 'tx> Routine<ProcedureContext<'a, 'tx>> for QueueFail {
	fn info(&self) -> &RoutineInfo {
		&INFO
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Any
	}

	#[instrument(name = "queue::fail", level = "debug", skip_all, fields(status = Empty))]
	fn execute(&self, ctx: &mut ProcedureContext<'a, 'tx>, _args: &Columns) -> Result<Columns, RoutineError> {
		require_command_transaction(PROCEDURE, ctx.tx)?;

		let args = extract_args(PROCEDURE, ctx.params, 2)?;
		let raw_token = utf8_arg(PROCEDURE, &args[0], 0)?;
		let response = optional_utf8_arg(PROCEDURE, &args[1], 1)?;

		record_outcome(PROCEDURE, AttemptOutcome::Err, ctx, &raw_token, response)
	}
}
