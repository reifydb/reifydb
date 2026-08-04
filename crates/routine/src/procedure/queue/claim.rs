// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use reifydb_core::{
	interface::{
		catalog::queue::{
			Queue, QueueItemStatus, decode_queue_item_state, decode_queue_partition_counters,
			encode_queue_item_state, encode_queue_partition_counters,
		},
		store::SingleVersionRangeRev,
	},
	key::{
		EncodableKey,
		queue_schedule::{QueueDueKey, QueueItemStateKey, QueuePartitionKey},
		row::RowKey,
	},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_codec::row::{bytes::RowBuilder, queue::EncodedQueueRow};
use reifydb_routine_abi::{Routine, RoutineInfo, context::ProcedureContext, error::RoutineError};
use reifydb_transaction::single::SingleTransaction;
use reifydb_value::{
	fragment::Fragment,
	value::{
		Value, datetime::DateTime, duration::Duration, partition::Partition, row_number::RowNumber,
		value_type::ValueType,
	},
};
use tracing::{Span, debug, field::Empty, instrument};

use crate::procedure::{
	identity::set_attribute::extract_args,
	queue::{require_command_transaction, resolve_queue_by_name, token::ClaimToken, utf8_arg},
};

static INFO: LazyLock<RoutineInfo> = LazyLock::new(|| RoutineInfo::new("queue::claim"));

const PROCEDURE: &str = "queue::claim";

pub struct QueueClaim;

impl Default for QueueClaim {
	fn default() -> Self {
		Self::new()
	}
}

impl QueueClaim {
	pub fn new() -> Self {
		Self
	}
}

struct Lease {
	partition: u16,
	row: RowNumber,
	attempt: u32,
	deadline: DateTime,
}

impl<'a, 'tx> Routine<ProcedureContext<'a, 'tx>> for QueueClaim {
	fn info(&self) -> &RoutineInfo {
		&INFO
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Any
	}

	#[instrument(
		name = "queue::claim",
		level = "debug",
		skip_all,
		fields(queue = Empty, worker = Empty, requested = Empty, claimed = Empty)
	)]
	fn execute(&self, ctx: &mut ProcedureContext<'a, 'tx>, _args: &Columns) -> Result<Columns, RoutineError> {
		require_command_transaction(PROCEDURE, ctx.tx)?;

		let args = extract_args(PROCEDURE, ctx.params, 4)?;
		let worker = non_empty_utf8(&args[0], 0)?;
		let queue_name = utf8_arg(PROCEDURE, &args[1], 1)?;
		let max_n = positive_count(&args[2], 2)?;
		let lease_ttl = positive_duration(&args[3], 3)?;

		let span = Span::current();
		span.record("queue", queue_name.as_str());
		span.record("worker", worker.as_str());
		span.record("requested", max_n);

		let now = ctx.runtime_context.clock.now();
		let queue = resolve_queue_by_name(ctx.catalog, &mut *ctx.tx, &queue_name, &ctx.fragment)?;

		let single =
			ctx.tx.single()
				.ok_or_else(|| RoutineError::ProcedureExecutionFailed {
					procedure: Fragment::internal(PROCEDURE),
					reason: "must run in a command transaction".to_string(),
				})?
				.clone();

		let leases = lease_due_items(ctx, &single, &queue, &worker, max_n, lease_ttl, now)?;
		span.record("claimed", leases.len());

		claimed_columns(ctx, &queue, &worker, &leases)
	}
}

fn non_empty_utf8(value: &Value, argument_index: usize) -> Result<String, RoutineError> {
	let text = utf8_arg(PROCEDURE, value, argument_index)?;
	if text.is_empty() {
		return Err(RoutineError::ProcedureExecutionFailed {
			procedure: Fragment::internal(PROCEDURE),
			reason: "worker id must not be empty".to_string(),
		});
	}
	Ok(text)
}

fn positive_count(value: &Value, argument_index: usize) -> Result<usize, RoutineError> {
	let count = match value {
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

	if count < 1 {
		return Err(RoutineError::ProcedureExecutionFailed {
			procedure: Fragment::internal(PROCEDURE),
			reason: format!("max_n must be at least 1, got {count}"),
		});
	}

	Ok(count.min(i128::from(u32::MAX)) as usize)
}

fn positive_duration(value: &Value, argument_index: usize) -> Result<Duration, RoutineError> {
	match value {
		Value::Duration(duration) if duration.is_positive() => Ok(*duration),
		Value::Duration(_) => Err(RoutineError::ProcedureExecutionFailed {
			procedure: Fragment::internal(PROCEDURE),
			reason: "lease ttl must be positive".to_string(),
		}),
		other => Err(RoutineError::ProcedureInvalidArgumentType {
			procedure: Fragment::internal(PROCEDURE),
			argument_index,
			expected: vec![ValueType::Duration],
			actual: other.get_type(),
		}),
	}
}

fn lease_due_items(
	ctx: &mut ProcedureContext<'_, '_>,
	single: &SingleTransaction,
	queue: &Queue,
	worker: &str,
	max_n: usize,
	lease_ttl: Duration,
	now: DateTime,
) -> Result<Vec<Lease>, RoutineError> {
	let partitions = queue.partitions();
	let start = (Partition::of(&[Value::Utf8(worker.to_string())]).0 % u128::from(partitions)) as u16;

	let mut leases = Vec::new();
	for offset in 0..partitions {
		if leases.len() >= max_n {
			break;
		}

		let partition = ((u32::from(start) + u32::from(offset)) % u32::from(partitions)) as u16;
		let candidates = due_candidates(single, queue, partition, now, max_n - leases.len())?;
		if candidates.is_empty() {
			continue;
		}

		let readable = readable_candidates(ctx, queue, &candidates)?;
		if readable.is_empty() {
			continue;
		}

		leases.extend(lease_candidates(single, queue, partition, &readable, lease_ttl, now)?);
	}

	Ok(leases)
}

fn readable_candidates(
	ctx: &mut ProcedureContext<'_, '_>,
	queue: &Queue,
	candidates: &[RowNumber],
) -> Result<Vec<RowNumber>, RoutineError> {
	let mut readable = Vec::with_capacity(candidates.len());
	for row in candidates {
		if ctx.tx.get(&RowKey::encoded(queue.id, *row))?.is_some() {
			readable.push(*row);
		} else {
			debug!(
				queue = queue.id.0,
				item = row.0,
				"the item row is not visible to this claim yet; leaving it for a later one"
			);
		}
	}

	Ok(readable)
}

#[instrument(name = "queue::claim::scan", level = "trace", skip_all, fields(queue = queue.id.0, partition = partition))]
fn due_candidates(
	single: &SingleTransaction,
	queue: &Queue,
	partition: u16,
	now: DateTime,
	need: usize,
) -> Result<Vec<RowNumber>, RoutineError> {
	let store = single.read_store();
	let batch = SingleVersionRangeRev::range_rev_batch(
		&store,
		QueueDueKey::partition_scan(queue.id, partition),
		need as u64,
	)?;

	Ok(batch.items
		.iter()
		.filter_map(|item| QueueDueKey::decode(&item.key))
		.take_while(|due| due.due <= now)
		.map(|due| due.row)
		.collect())
}

fn lease_candidates(
	single: &SingleTransaction,
	queue: &Queue,
	partition: u16,
	candidates: &[RowNumber],
	lease_ttl: Duration,
	now: DateTime,
) -> Result<Vec<Lease>, RoutineError> {
	let lock_key = QueuePartitionKey::encoded(queue.id, partition);
	let mut tx = single.begin_command_ranged(
		[&lock_key],
		vec![
			QueueItemStateKey::partition_scan(queue.id, partition),
			QueueDueKey::partition_scan(queue.id, partition),
		],
	)?;

	let mut leases = Vec::new();
	for row in candidates {
		let state_key = QueueItemStateKey::encoded(queue.id, partition, *row);
		let Some(stored) = tx.get(&state_key)? else {
			continue;
		};
		let Some(mut state) = decode_queue_item_state(&stored.bytes) else {
			continue;
		};
		if state.status != QueueItemStatus::Ready || state.due() > now {
			continue;
		}

		let due = state.due();
		let deadline = now.add_duration(&lease_ttl)?;

		state.status = QueueItemStatus::Leased;
		state.attempt += 1;
		state.lease_deadline = Some(deadline);

		tx.set(&state_key, encode_queue_item_state(&state).freeze_bytes())?;
		tx.remove(&QueueDueKey::encoded(queue.id, partition, due, *row))?;

		leases.push(Lease {
			partition,
			row: *row,
			attempt: state.attempt,
			deadline,
		});
	}

	if !leases.is_empty() {
		let mut counters = tx
			.get(&lock_key)?
			.map(|stored| decode_queue_partition_counters(&stored.bytes))
			.unwrap_or_default();
		counters.depth = counters.depth.saturating_sub(leases.len() as u64);
		counters.in_flight += leases.len() as u64;
		tx.set(&lock_key, encode_queue_partition_counters(&counters).freeze_bytes())?;
	}

	tx.commit()?;

	Ok(leases)
}

fn claimed_columns(
	ctx: &mut ProcedureContext<'_, '_>,
	queue: &Queue,
	worker: &str,
	leases: &[Lease],
) -> Result<Columns, RoutineError> {
	let mut tokens = Vec::with_capacity(leases.len());
	let mut items = Vec::with_capacity(leases.len());
	let mut attempts = Vec::with_capacity(leases.len());
	let mut deadlines = Vec::with_capacity(leases.len());
	let mut payloads: Vec<ColumnBuffer> = queue
		.columns
		.iter()
		.map(|column| ColumnBuffer::with_capacity(column.constraint.get_type(), leases.len()))
		.collect();

	for lease in leases {
		tokens.push(ClaimToken {
			queue: queue.id,
			partition: lease.partition,
			row: lease.row,
			attempt: lease.attempt,
			worker: worker.to_string(),
		}
		.format());
		items.push(lease.row.0);
		attempts.push(lease.attempt);
		deadlines.push(lease.deadline);

		push_payload(ctx, queue, lease.row, &mut payloads)?;
	}

	let mut columns = vec![
		ColumnWithName {
			name: Fragment::internal("token"),
			data: ColumnBuffer::utf8(tokens),
		},
		ColumnWithName {
			name: Fragment::internal("item"),
			data: ColumnBuffer::uint8(items),
		},
		ColumnWithName {
			name: Fragment::internal("attempt"),
			data: ColumnBuffer::uint4(attempts),
		},
		ColumnWithName {
			name: Fragment::internal("deadline"),
			data: ColumnBuffer::datetime(deadlines),
		},
	];

	for (column, data) in queue.columns.iter().zip(payloads) {
		columns.push(ColumnWithName {
			name: Fragment::internal(column.name.clone()),
			data,
		});
	}

	let row_numbers = leases.iter().map(|lease| lease.row).collect();

	Ok(Columns::new(columns).with_row_numbers(row_numbers))
}

fn push_payload(
	ctx: &mut ProcedureContext<'_, '_>,
	queue: &Queue,
	row: RowNumber,
	payloads: &mut [ColumnBuffer],
) -> Result<(), RoutineError> {
	let stored = ctx.tx.get(&RowKey::encoded(queue.id, row))?;

	let Some(stored) = stored else {
		for buffer in payloads.iter_mut() {
			buffer.push_value(Value::None {
				inner: buffer.get_type(),
			});
		}
		return Ok(());
	};

	let fingerprint = EncodedQueueRow::view(&stored.bytes).fingerprint();
	let Some(shape) = ctx.catalog.get_or_load_row_shape(fingerprint, &mut *ctx.tx)? else {
		return Err(RoutineError::ProcedureExecutionFailed {
			procedure: Fragment::internal(PROCEDURE),
			reason: format!("row shape {fingerprint:?} not found for queue {}", queue.name),
		});
	};

	for (index, buffer) in payloads.iter_mut().enumerate() {
		buffer.push_value(shape.get_value(&stored.bytes, index));
	}

	Ok(())
}
