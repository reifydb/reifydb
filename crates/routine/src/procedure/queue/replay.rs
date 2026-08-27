// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_core::{
	interface::{
		catalog::queue::{Queue, decode_queue_item_state},
		store::SingleVersionGet,
	},
	key::queue_schedule::QueueItemStateKey,
	value::column::columns::Columns,
};
use reifydb_routine_abi::{
	Routine, RoutineInfo,
	context::ProcedureContext,
	error::{QueueError, RoutineError},
};
use reifydb_transaction::{
	queue::scheduling::{ReplayOutcome, apply_replay_transition},
	single::SingleTransaction,
};
use reifydb_value::{
	fragment::Fragment,
	value::{Value, row_number::RowNumber, value_type::ValueType},
};
use tracing::instrument;

use crate::procedure::{
	identity::set_attribute::extract_args,
	queue::{require_command_transaction, resolve_queue_by_name, utf8_arg},
};

static INFO: LazyLock<RoutineInfo> = LazyLock::new(|| RoutineInfo::new("queue::replay"));

const PROCEDURE: &str = "queue::replay";

pub struct QueueReplay;

impl Default for QueueReplay {
	fn default() -> Self {
		Self::new()
	}
}

impl QueueReplay {
	pub fn new() -> Self {
		Self
	}
}

impl<'a, 'tx> Routine<ProcedureContext<'a, 'tx>> for QueueReplay {
	fn info(&self) -> &RoutineInfo {
		&INFO
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Any
	}

	#[instrument(name = "queue::replay", level = "debug", skip_all)]
	fn execute(&self, ctx: &mut ProcedureContext<'a, 'tx>, _args: &Columns) -> Result<Columns, RoutineError> {
		require_command_transaction(PROCEDURE, ctx.tx)?;

		let args = extract_args(PROCEDURE, ctx.params, 2)?;
		let queue_name = utf8_arg(PROCEDURE, &args[0], 0)?;
		let row = row_number_arg(&args[1], 1)?;

		let queue = resolve_queue_by_name(ctx.catalog, &mut *ctx.tx, &queue_name, &ctx.fragment)?;

		let single =
			ctx.tx.single()
				.ok_or_else(|| RoutineError::ProcedureExecutionFailed {
					procedure: Fragment::internal(PROCEDURE),
					reason: "must run in a command transaction".to_string(),
				})?
				.clone();

		let fragment = ctx.fragment.clone();
		let unknown = || -> RoutineError {
			QueueError::ReplayUnknownItem {
				procedure: PROCEDURE,
				fragment: fragment.clone(),
				queue: queue_name.clone(),
				item: row.0,
			}
			.into()
		};

		let Some((partition, key_hash)) = locate(&single, &queue, row)? else {
			return Err(unknown());
		};

		let state = match apply_replay_transition(&single, queue.id, partition, row, key_hash)? {
			ReplayOutcome::Ready => "ready",
			ReplayOutcome::Parked => "parked",
			ReplayOutcome::Unknown => return Err(unknown()),
			ReplayOutcome::Unreadable => {
				return Err(RoutineError::ProcedureExecutionFailed {
					procedure: Fragment::internal(PROCEDURE),
					reason: format!("the scheduling state of item {} is unreadable", row.0),
				});
			}
			ReplayOutcome::NotDead(status) => {
				return Err(QueueError::ReplayNotDead {
					procedure: PROCEDURE,
					fragment: fragment.clone(),
					queue: queue_name.clone(),
					item: row.0,
					status: format!("{status:?}").to_lowercase(),
				}
				.into());
			}
		};

		Ok(Columns::single_row([
			("queue", Value::Utf8(queue_name)),
			("item", Value::Uint8(row.0)),
			("state", Value::Utf8(state.to_string())),
		]))
	}
}

fn row_number_arg(value: &Value, argument_index: usize) -> Result<RowNumber, RoutineError> {
	let item = match value {
		Value::Int1(v) => i128::from(*v),
		Value::Int2(v) => i128::from(*v),
		Value::Int4(v) => i128::from(*v),
		Value::Int8(v) => i128::from(*v),
		Value::Uint1(v) => i128::from(*v),
		Value::Uint2(v) => i128::from(*v),
		Value::Uint4(v) => i128::from(*v),
		Value::Uint8(v) => i128::from(*v),
		other => {
			return Err(RoutineError::ProcedureInvalidArgumentType {
				procedure: Fragment::internal(PROCEDURE),
				argument_index,
				expected: vec![ValueType::Int4, ValueType::Int8, ValueType::Uint8],
				actual: other.get_type(),
			});
		}
	};

	if item < 1 || item > i128::from(u64::MAX) {
		return Err(RoutineError::ProcedureExecutionFailed {
			procedure: Fragment::internal(PROCEDURE),
			reason: format!("item must be a positive row number, got {item}"),
		});
	}

	Ok(RowNumber(item as u64))
}

fn locate(
	single: &SingleTransaction,
	queue: &Queue,
	row: RowNumber,
) -> Result<Option<(u16, Option<u64>)>, RoutineError> {
	let store = single.read_store();
	for partition in 0..queue.partitions() {
		let Some(stored) =
			SingleVersionGet::get(&store, &QueueItemStateKey::encoded(queue.id, partition, row))?
		else {
			continue;
		};
		let key_hash = decode_queue_item_state(EncodedPodRow::view(&stored.bytes))
			.and_then(|state| queue.ordered_by().is_some().then_some(state.key_hash));
		return Ok(Some((partition, key_hash)));
	}
	Ok(None)
}
