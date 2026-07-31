// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use reifydb_codec::encoded::row::EncodedRow;
use reifydb_core::{
	interface::{
		catalog::queue::{
			Queue, QueueItemStatus, decode_queue_item_state, decode_queue_partition_counters,
			encode_queue_item_state, encode_queue_partition_counters,
		},
		store::SingleVersionGet,
	},
	key::queue_schedule::{QueueDueKey, QueueItemStateKey, QueuePartitionKey},
	value::column::columns::Columns,
};
use reifydb_transaction::single::SingleTransaction;
use reifydb_value::{
	fragment::Fragment,
	util::cowvec::CowVec,
	value::{Value, row_number::RowNumber, value_type::ValueType},
};

use crate::{
	procedure::{
		identity::set_attribute::extract_args,
		queue::{require_command_transaction, resolve_queue_by_name, utf8_arg},
	},
	routine::{
		Routine, RoutineInfo,
		context::ProcedureContext,
		error::{QueueError, RoutineError},
	},
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

		let Some(partition) = locate(&single, &queue, row)? else {
			return Err(unknown());
		};

		revive(&single, &queue, partition, row, &queue_name, &fragment, &unknown)?;

		Ok(Columns::single_row([
			("queue", Value::Utf8(queue_name)),
			("item", Value::Uint8(row.0)),
			("state", Value::Utf8("ready".to_string())),
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

fn locate(single: &SingleTransaction, queue: &Queue, row: RowNumber) -> Result<Option<u16>, RoutineError> {
	let store = single.read_store();
	for partition in 0..queue.partitions() {
		if SingleVersionGet::get(&store, &QueueItemStateKey::encoded(queue.id, partition, row))?.is_some() {
			return Ok(Some(partition));
		}
	}
	Ok(None)
}

fn revive(
	single: &SingleTransaction,
	queue: &Queue,
	partition: u16,
	row: RowNumber,
	queue_name: &str,
	fragment: &Fragment,
	unknown: &dyn Fn() -> RoutineError,
) -> Result<(), RoutineError> {
	let lock_key = QueuePartitionKey::encoded(queue.id, partition);
	let mut tx = single.begin_command_ranged(
		[&lock_key],
		vec![
			QueueItemStateKey::partition_scan(queue.id, partition),
			QueueDueKey::partition_scan(queue.id, partition),
		],
	)?;

	let state_key = QueueItemStateKey::encoded(queue.id, partition, row);
	let Some(stored) = tx.get(&state_key)? else {
		return Err(unknown());
	};
	let Some(mut state) = decode_queue_item_state(&stored.row) else {
		return Err(RoutineError::ProcedureExecutionFailed {
			procedure: Fragment::internal(PROCEDURE),
			reason: format!("the scheduling state of item {} is unreadable", row.0),
		});
	};

	if state.status != QueueItemStatus::Dead {
		return Err(QueueError::ReplayNotDead {
			procedure: PROCEDURE,
			fragment: fragment.clone(),
			queue: queue_name.to_string(),
			item: row.0,
			status: format!("{:?}", state.status).to_lowercase(),
		}
		.into());
	}

	state.status = QueueItemStatus::Ready;
	state.budget_base = state.attempt;
	state.backoff_until = None;
	state.lease_deadline = None;

	tx.set(&state_key, encode_queue_item_state(&state))?;
	tx.set(&QueueDueKey::encoded(queue.id, partition, state.due(), row), EncodedRow(CowVec::new(vec![])))?;

	let mut counters =
		tx.get(&lock_key)?.map(|stored| decode_queue_partition_counters(&stored.row)).unwrap_or_default();
	counters.depth += 1;
	tx.set(&lock_key, encode_queue_partition_counters(&counters))?;

	tx.commit()?;

	Ok(())
}
