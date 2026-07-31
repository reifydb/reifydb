// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use reifydb_core::{
	interface::catalog::queue::{QueueItemStatus, decode_queue_item_state, encode_queue_item_state},
	key::queue_schedule::{QueueItemStateKey, QueuePartitionKey},
	value::column::columns::Columns,
};
use reifydb_codec::row::bytes::RowBuilder;
use reifydb_routine_abi::{
	Routine, RoutineInfo,
	context::ProcedureContext,
	error::{QueueError, RoutineError},
};
use reifydb_value::{
	fragment::Fragment,
	value::{Value, duration::Duration, value_type::ValueType},
};

use crate::procedure::{
	identity::set_attribute::extract_args,
	queue::{require_command_transaction, token::ClaimToken, utf8_arg},
};

static INFO: LazyLock<RoutineInfo> = LazyLock::new(|| RoutineInfo::new("queue::extend"));

const PROCEDURE: &str = "queue::extend";

pub struct QueueExtend;

impl Default for QueueExtend {
	fn default() -> Self {
		Self::new()
	}
}

impl QueueExtend {
	pub fn new() -> Self {
		Self
	}
}

impl<'a, 'tx> Routine<ProcedureContext<'a, 'tx>> for QueueExtend {
	fn info(&self) -> &RoutineInfo {
		&INFO
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Any
	}

	fn execute(&self, ctx: &mut ProcedureContext<'a, 'tx>, _args: &Columns) -> Result<Columns, RoutineError> {
		require_command_transaction(PROCEDURE, ctx.tx)?;

		let args = extract_args(PROCEDURE, ctx.params, 2)?;
		let raw_token = utf8_arg(PROCEDURE, &args[0], 0)?;
		let ttl = positive_duration(&args[1], 1)?;

		let token = ClaimToken::parse(PROCEDURE, &ctx.fragment, &raw_token)?;
		let now = ctx.runtime_context.clock.now();

		let single =
			ctx.tx.single()
				.ok_or_else(|| RoutineError::ProcedureExecutionFailed {
					procedure: Fragment::internal(PROCEDURE),
					reason: "must run in a command transaction".to_string(),
				})?
				.clone();

		let fragment = ctx.fragment.clone();
		let stale = |reason: &str| -> RoutineError {
			QueueError::ExtendStale {
				procedure: PROCEDURE,
				fragment: fragment.clone(),
				token: raw_token.clone(),
				reason: reason.to_string(),
			}
			.into()
		};

		let lock_key = QueuePartitionKey::encoded(token.queue, token.partition);
		let mut tx = single.begin_command_ranged(
			[&lock_key],
			vec![QueueItemStateKey::partition_scan(token.queue, token.partition)],
		)?;

		let state_key = QueueItemStateKey::encoded(token.queue, token.partition, token.row);
		let Some(stored) = tx.get(&state_key)? else {
			return Err(stale("the item has no scheduling state"));
		};
		let Some(mut state) = decode_queue_item_state(&stored.bytes) else {
			return Err(stale("the item's scheduling state is unreadable"));
		};

		if state.status != QueueItemStatus::Leased {
			return Err(stale("the item is not leased"));
		}
		if state.attempt != token.attempt {
			return Err(stale("the lease has been reissued under a later attempt"));
		}

		let requested = now.add_duration(&ttl)?;
		let deadline = match state.lease_deadline {
			Some(existing) if existing > requested => existing,
			_ => requested,
		};
		state.lease_deadline = Some(deadline);

		tx.set(&state_key, encode_queue_item_state(&state).freeze_bytes())?;
		tx.commit()?;

		Ok(Columns::single_row([
			("item", Value::Uint8(token.row.0)),
			("attempt", Value::Uint4(token.attempt)),
			("deadline", Value::DateTime(deadline)),
		]))
	}
}

fn positive_duration(value: &Value, argument_index: usize) -> Result<Duration, RoutineError> {
	match value {
		Value::Duration(duration) if duration.is_positive() => Ok(*duration),
		Value::Duration(_) => Err(RoutineError::ProcedureExecutionFailed {
			procedure: Fragment::internal(PROCEDURE),
			reason: "ttl must be positive".to_string(),
		}),
		other => Err(RoutineError::ProcedureInvalidArgumentType {
			procedure: Fragment::internal(PROCEDURE),
			argument_index,
			expected: vec![ValueType::Duration],
			actual: other.get_type(),
		}),
	}
}
