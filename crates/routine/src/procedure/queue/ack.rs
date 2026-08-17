// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use reifydb_codec::row::{pod::EncodedPodRow, queue_attempt::EncodedQueueAttemptRow};
use reifydb_core::{
	interface::{
		catalog::queue::{
			AttemptOutcome, Queue, QueueAttemptRecord, QueueFailure, QueueItemState, QueueItemStatus,
			decode_queue_attempt, decode_queue_item_state, encode_queue_attempt, on_failure,
		},
		store::SingleVersionGet,
	},
	key::{queue_attempt::QueueAttemptKey, queue_schedule::QueueItemStateKey},
	value::column::columns::Columns,
};
use reifydb_routine_abi::{Routine, RoutineInfo, context::ProcedureContext, error::RoutineError};
use reifydb_transaction::change::{QueueAckTransition, QueueRowAck, RowChange};
use reifydb_value::value::{Value, datetime::DateTime, value_type::ValueType};
use tracing::{Span, field::Empty, instrument};

use crate::procedure::{
	identity::set_attribute::extract_args,
	queue::{require_command_transaction, resolve_queue_by_id, token::ClaimToken, utf8_arg},
};

static INFO: LazyLock<RoutineInfo> = LazyLock::new(|| RoutineInfo::new("queue::ack"));

const PROCEDURE: &str = "queue::ack";

pub(crate) const STATUS_OK: &str = "ok";
pub(crate) const STATUS_REPEAT: &str = "repeat";
pub(crate) const STATUS_STALE: &str = "stale";

pub struct QueueAck;

impl Default for QueueAck {
	fn default() -> Self {
		Self::new()
	}
}

impl QueueAck {
	pub fn new() -> Self {
		Self
	}
}

impl<'a, 'tx> Routine<ProcedureContext<'a, 'tx>> for QueueAck {
	fn info(&self) -> &RoutineInfo {
		&INFO
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Any
	}

	#[instrument(name = "queue::ack", level = "debug", skip_all, fields(status = Empty))]
	fn execute(&self, ctx: &mut ProcedureContext<'a, 'tx>, _args: &Columns) -> Result<Columns, RoutineError> {
		require_command_transaction(PROCEDURE, ctx.tx)?;

		let args = extract_args(PROCEDURE, ctx.params, 1)?;
		let raw_token = utf8_arg(PROCEDURE, &args[0], 0)?;

		record_outcome(PROCEDURE, AttemptOutcome::Ok, ctx, &raw_token, None)
	}
}

pub(crate) fn optional_utf8_arg(
	procedure: &'static str,
	value: &Value,
	argument_index: usize,
) -> Result<Option<String>, RoutineError> {
	match value {
		Value::None {
			..
		} => Ok(None),
		other => utf8_arg(procedure, other, argument_index).map(Some),
	}
}

pub(crate) fn outcome_name(outcome: AttemptOutcome) -> &'static str {
	match outcome {
		AttemptOutcome::Ok => "ok",
		AttemptOutcome::Err => "err",
		AttemptOutcome::Dead => "dead",
	}
}

pub(crate) fn record_outcome(
	procedure: &'static str,
	outcome: AttemptOutcome,
	ctx: &mut ProcedureContext<'_, '_>,
	raw_token: &str,
	response: Option<String>,
) -> Result<Columns, RoutineError> {
	let token = ClaimToken::parse(procedure, &ctx.fragment, raw_token)?;
	let now = ctx.runtime_context.clock.now();
	let queue = resolve_queue_by_id(ctx.catalog, &mut *ctx.tx, token.queue, &ctx.fragment)?;

	let state = live_state(ctx, &token)?;
	let existing = existing_attempt(ctx, &token)?;

	let status = match (existing, state.as_ref()) {
		(Some(record), Some(state)) if record.outcome == outcome => {
			track(ctx, &queue, &token, state, outcome, now);
			STATUS_OK
		}
		(Some(record), None) if record.outcome == outcome => STATUS_REPEAT,
		(Some(record), _) => {
			let anomaly = format!("conflicting late ack: {}", outcome_name(outcome));
			write_attempt(
				ctx,
				&token,
				QueueAttemptRecord {
					anomaly: Some(anomaly),
					..record
				},
			)?;
			STATUS_STALE
		}
		(None, Some(state)) => {
			write_attempt(
				ctx,
				&token,
				QueueAttemptRecord {
					worker: token.worker.clone(),
					outcome,
					response,
					finished_at: now,
					lost: false,
					anomaly: None,
				},
			)?;
			track(ctx, &queue, &token, state, outcome, now);
			STATUS_OK
		}
		(None, None) => {
			write_attempt(
				ctx,
				&token,
				QueueAttemptRecord {
					worker: token.worker.clone(),
					outcome,
					response,
					finished_at: now,
					lost: false,
					anomaly: Some("stale: the lease this token names is no longer live".to_string()),
				},
			)?;
			STATUS_STALE
		}
	};

	Span::current().record("status", status);

	Ok(Columns::single_row([
		("status", Value::Utf8(status.to_string())),
		("item", Value::Uint8(token.row.0)),
		("attempt", Value::Uint4(token.attempt)),
	]))
}

fn live_state(ctx: &ProcedureContext<'_, '_>, token: &ClaimToken) -> Result<Option<QueueItemState>, RoutineError> {
	let Some(single) = ctx.tx.single() else {
		return Ok(None);
	};
	let store = single.read_store();
	let key = QueueItemStateKey::encoded(token.queue, token.partition, token.row);

	let Some(stored) = SingleVersionGet::get(&store, &key)? else {
		return Ok(None);
	};
	let Some(state) = decode_queue_item_state(EncodedPodRow::view(&stored.bytes)) else {
		return Ok(None);
	};

	if state.status == QueueItemStatus::Leased && state.attempt == token.attempt {
		Ok(Some(state))
	} else {
		Ok(None)
	}
}

fn existing_attempt(
	ctx: &mut ProcedureContext<'_, '_>,
	token: &ClaimToken,
) -> Result<Option<QueueAttemptRecord>, RoutineError> {
	let key = QueueAttemptKey::encoded(token.queue, token.row, token.attempt);
	Ok(ctx.tx.get(&key)?.and_then(|stored| decode_queue_attempt(EncodedQueueAttemptRow::view(&stored.bytes))))
}

fn write_attempt(
	ctx: &mut ProcedureContext<'_, '_>,
	token: &ClaimToken,
	record: QueueAttemptRecord,
) -> Result<(), RoutineError> {
	let key = QueueAttemptKey::encoded(token.queue, token.row, token.attempt);
	ctx.tx.set(&key, encode_queue_attempt(&record))?;
	Ok(())
}

fn track(
	ctx: &mut ProcedureContext<'_, '_>,
	queue: &Queue,
	token: &ClaimToken,
	state: &QueueItemState,
	outcome: AttemptOutcome,
	now: DateTime,
) {
	let transition = match outcome {
		AttemptOutcome::Ok => QueueAckTransition::Done,
		AttemptOutcome::Dead => QueueAckTransition::Dead,
		AttemptOutcome::Err => match on_failure(&queue.retry, state, now) {
			QueueFailure::Dead => QueueAckTransition::Dead,
			QueueFailure::Retry {
				backoff_until,
			} => QueueAckTransition::Retry {
				backoff_until,
			},
		},
	};

	ctx.tx.track_row_change(&[RowChange::QueueAck(QueueRowAck {
		queue_id: token.queue,
		partition: token.partition,
		key_hash: queue.ordered_by().is_some().then_some(state.key_hash),
		row_number: token.row,
		attempt: token.attempt,
		transition,
	})]);
}
