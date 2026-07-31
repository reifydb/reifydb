// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use reifydb_codec::row::{
	pod::EncodedPodRowBuilder,
	shape::{RowFamily, RowShape, RowShapeField},
};
use reifydb_value::value::{datetime::DateTime, duration::Duration, value_type::ValueType};
use serde::{Deserialize, Serialize};

use crate::{
	common::TimeSource,
	interface::catalog::{
		column::Column,
		id::{NamespaceId, QueueId},
	},
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Queue {
	pub id: QueueId,
	pub namespace: NamespaceId,
	pub name: String,
	pub columns: Vec<Column>,
	pub dispatch: QueueDispatch,
	pub deduplicate: Option<QueueDeduplicate>,
	pub retention: QueueRetention,
	pub retry: QueueRetry,
	pub time: TimeSource,
}

impl Queue {
	pub const DEFAULT_PARTITIONS: u16 = 16;
	pub const MIN_PARTITIONS: u16 = 1;
	pub const MAX_PARTITIONS: u16 = 1024;
	pub const DEFAULT_RETRY_ATTEMPTS: u32 = 5;
	pub const DEFAULT_RETRY_BACKOFF: Duration = Duration::from_seconds_const(10);

	pub fn name(&self) -> &str {
		&self.name
	}

	pub fn partitions(&self) -> u16 {
		self.dispatch.partitions()
	}

	pub fn ordered_by(&self) -> Option<&str> {
		self.dispatch.ordered_by()
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum QueueDispatch {
	Fifo {
		partitions: u16,
		ordered_by: Option<String>,
	},
}

impl QueueDispatch {
	pub const TAG_FIFO: u8 = 0;

	pub fn tag(&self) -> u8 {
		match self {
			Self::Fifo {
				..
			} => Self::TAG_FIFO,
		}
	}

	pub fn partitions(&self) -> u16 {
		match self {
			Self::Fifo {
				partitions,
				..
			} => *partitions,
		}
	}

	pub fn ordered_by(&self) -> Option<&str> {
		match self {
			Self::Fifo {
				ordered_by,
				..
			} => ordered_by.as_deref(),
		}
	}
}

impl Default for QueueDispatch {
	fn default() -> Self {
		Self::Fifo {
			partitions: Queue::DEFAULT_PARTITIONS,
			ordered_by: None,
		}
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueDeduplicate {
	pub by: Vec<String>,
	pub ttl: Duration,
}

impl QueueDeduplicate {
	pub fn is_forever(&self) -> bool {
		self.ttl == Duration::MAX
	}
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct QueueRetention {
	pub done: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueRetry {
	pub attempts: u32,
	pub backoff: Duration,
}

impl Default for QueueRetry {
	fn default() -> Self {
		Self {
			attempts: Queue::DEFAULT_RETRY_ATTEMPTS,
			backoff: Queue::DEFAULT_RETRY_BACKOFF,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueItemStatus {
	Ready = 0,
	Leased = 1,
	Done = 2,
	Dead = 3,
	Parked = 4,
}

impl QueueItemStatus {
	pub fn tag(&self) -> u8 {
		*self as u8
	}
}

impl TryFrom<u8> for QueueItemStatus {
	type Error = u8;

	fn try_from(value: u8) -> Result<Self, Self::Error> {
		match value {
			0 => Ok(Self::Ready),
			1 => Ok(Self::Leased),
			2 => Ok(Self::Done),
			3 => Ok(Self::Dead),
			4 => Ok(Self::Parked),
			other => Err(other),
		}
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueueItemState {
	pub status: QueueItemStatus,
	pub attempt: u32,
	pub budget_base: u32,
	pub key_hash: u64,
	pub not_before: Option<DateTime>,
	pub lease_deadline: Option<DateTime>,
	pub backoff_until: Option<DateTime>,
}

impl QueueItemState {
	pub fn ready(not_before: Option<DateTime>) -> Self {
		Self {
			status: QueueItemStatus::Ready,
			attempt: 0,
			budget_base: 0,
			key_hash: 0,
			not_before,
			lease_deadline: None,
			backoff_until: None,
		}
	}

	pub fn due(&self) -> DateTime {
		self.not_before.unwrap_or_else(|| DateTime::from_nanos(0))
	}
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueuePartitionCounters {
	pub depth: u64,
	pub in_flight: u64,
	pub blocked_keys: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptOutcome {
	Ok = 0,
	Err = 1,
	Dead = 2,
}

impl AttemptOutcome {
	pub fn tag(&self) -> u8 {
		*self as u8
	}
}

impl TryFrom<u8> for AttemptOutcome {
	type Error = u8;

	fn try_from(value: u8) -> Result<Self, u8> {
		match value {
			0 => Ok(Self::Ok),
			1 => Ok(Self::Err),
			2 => Ok(Self::Dead),
			other => Err(other),
		}
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueueAttemptRecord {
	pub worker: String,
	pub outcome: AttemptOutcome,
	pub response: Option<String>,
	pub finished_at: DateTime,
	pub lost: bool,
	pub anomaly: Option<String>,
}

mod attempt_shape {
	use super::*;

	pub(super) const WORKER: usize = 0;
	pub(super) const OUTCOME: usize = 1;
	pub(super) const RESPONSE: usize = 2;
	pub(super) const FINISHED_AT: usize = 3;
	pub(super) const LOST: usize = 4;
	pub(super) const ANOMALY: usize = 5;

	pub(super) static SHAPE: LazyLock<RowShape> = LazyLock::new(|| {
		RowShape::new(RowFamily::Pod, vec![
			RowShapeField::unconstrained("worker", ValueType::Utf8),
			RowShapeField::unconstrained("outcome", ValueType::Uint1),
			RowShapeField::unconstrained("response", ValueType::Utf8),
			RowShapeField::unconstrained("finished_at", ValueType::DateTime),
			RowShapeField::unconstrained("lost", ValueType::Boolean),
			RowShapeField::unconstrained("anomaly", ValueType::Utf8),
		])
	});
}

pub fn encode_queue_attempt(record: &QueueAttemptRecord) -> EncodedPodRowBuilder {
	let shape = &attempt_shape::SHAPE;
	let mut row = shape.allocate_pod();
	shape.set_utf8(&mut row, attempt_shape::WORKER, &record.worker);
	shape.set::<u8>(&mut row, attempt_shape::OUTCOME, record.outcome.tag());
	if let Some(response) = &record.response {
		shape.set_utf8(&mut row, attempt_shape::RESPONSE, response);
	}
	shape.set::<DateTime>(&mut row, attempt_shape::FINISHED_AT, record.finished_at);
	shape.set::<bool>(&mut row, attempt_shape::LOST, record.lost);
	if let Some(anomaly) = &record.anomaly {
		shape.set_utf8(&mut row, attempt_shape::ANOMALY, anomaly);
	}
	row
}

pub fn decode_queue_attempt(row: &[u8]) -> Option<QueueAttemptRecord> {
	let shape = &attempt_shape::SHAPE;
	Some(QueueAttemptRecord {
		worker: shape.get_utf8(row, attempt_shape::WORKER).to_string(),
		outcome: shape.get::<u8>(row, attempt_shape::OUTCOME).try_into().ok()?,
		response: shape.try_get_utf8(row, attempt_shape::RESPONSE).map(str::to_string),
		finished_at: shape.get::<DateTime>(row, attempt_shape::FINISHED_AT),
		lost: shape.get::<bool>(row, attempt_shape::LOST),
		anomaly: shape.try_get_utf8(row, attempt_shape::ANOMALY).map(str::to_string),
	})
}

mod item_state_shape {
	use super::*;

	pub(super) const STATUS: usize = 0;
	pub(super) const ATTEMPT: usize = 1;
	pub(super) const BUDGET_BASE: usize = 2;
	pub(super) const KEY_HASH: usize = 3;
	pub(super) const NOT_BEFORE: usize = 4;
	pub(super) const LEASE_DEADLINE: usize = 5;
	pub(super) const BACKOFF_UNTIL: usize = 6;

	pub(super) static SHAPE: LazyLock<RowShape> = LazyLock::new(|| {
		RowShape::new(
			RowFamily::Pod,
			vec![
				RowShapeField::unconstrained("status", ValueType::Uint1),
				RowShapeField::unconstrained("attempt", ValueType::Uint4),
				RowShapeField::unconstrained("budget_base", ValueType::Uint4),
				RowShapeField::unconstrained("key_hash", ValueType::Uint8),
				RowShapeField::unconstrained("not_before", ValueType::DateTime),
				RowShapeField::unconstrained("lease_deadline", ValueType::DateTime),
				RowShapeField::unconstrained("backoff_until", ValueType::DateTime),
			],
		)
	});
}

mod partition_counters_shape {
	use super::*;

	pub(super) const DEPTH: usize = 0;
	pub(super) const IN_FLIGHT: usize = 1;
	pub(super) const BLOCKED_KEYS: usize = 2;

	pub(super) static SHAPE: LazyLock<RowShape> = LazyLock::new(|| {
		RowShape::new(
			RowFamily::Pod,
			vec![
				RowShapeField::unconstrained("depth", ValueType::Uint8),
				RowShapeField::unconstrained("in_flight", ValueType::Uint8),
				RowShapeField::unconstrained("blocked_keys", ValueType::Uint8),
			],
		)
	});
}

pub fn encode_queue_item_state(state: &QueueItemState) -> EncodedPodRowBuilder {
	let shape = &item_state_shape::SHAPE;
	let mut row = shape.allocate_pod();
	shape.set::<u8>(&mut row, item_state_shape::STATUS, state.status.tag());
	shape.set::<u32>(&mut row, item_state_shape::ATTEMPT, state.attempt);
	shape.set::<u32>(&mut row, item_state_shape::BUDGET_BASE, state.budget_base);
	shape.set::<u64>(&mut row, item_state_shape::KEY_HASH, state.key_hash);
	if let Some(not_before) = state.not_before {
		shape.set::<DateTime>(&mut row, item_state_shape::NOT_BEFORE, not_before);
	}
	if let Some(lease_deadline) = state.lease_deadline {
		shape.set::<DateTime>(&mut row, item_state_shape::LEASE_DEADLINE, lease_deadline);
	}
	if let Some(backoff_until) = state.backoff_until {
		shape.set::<DateTime>(&mut row, item_state_shape::BACKOFF_UNTIL, backoff_until);
	}
	row
}

pub fn decode_queue_item_state(row: &[u8]) -> Option<QueueItemState> {
	let shape = &item_state_shape::SHAPE;
	Some(QueueItemState {
		status: shape.get::<u8>(row, item_state_shape::STATUS).try_into().ok()?,
		attempt: shape.get::<u32>(row, item_state_shape::ATTEMPT),
		budget_base: shape.get::<u32>(row, item_state_shape::BUDGET_BASE),
		key_hash: shape.get::<u64>(row, item_state_shape::KEY_HASH),
		not_before: shape.try_get::<DateTime>(row, item_state_shape::NOT_BEFORE),
		lease_deadline: shape.try_get::<DateTime>(row, item_state_shape::LEASE_DEADLINE),
		backoff_until: shape.try_get::<DateTime>(row, item_state_shape::BACKOFF_UNTIL),
	})
}

pub fn encode_queue_partition_counters(counters: &QueuePartitionCounters) -> EncodedPodRowBuilder {
	let shape = &partition_counters_shape::SHAPE;
	let mut row = shape.allocate_pod();
	shape.set::<u64>(&mut row, partition_counters_shape::DEPTH, counters.depth);
	shape.set::<u64>(&mut row, partition_counters_shape::IN_FLIGHT, counters.in_flight);
	shape.set::<u64>(&mut row, partition_counters_shape::BLOCKED_KEYS, counters.blocked_keys);
	row
}

pub fn decode_queue_partition_counters(row: &[u8]) -> QueuePartitionCounters {
	let shape = &partition_counters_shape::SHAPE;
	QueuePartitionCounters {
		depth: shape.get::<u64>(row, partition_counters_shape::DEPTH),
		in_flight: shape.get::<u64>(row, partition_counters_shape::IN_FLIGHT),
		blocked_keys: shape.get::<u64>(row, partition_counters_shape::BLOCKED_KEYS),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_item_state_roundtrips_with_no_temporals_set() {
		// A freshly admitted item has neither a lease nor a backoff, and an immediately due
		// item has no not_before either. All three must decode back as none rather than as
		// epoch: an epoch lease_deadline would read as a lease that expired in 1970, so the
		// reaper would treat every ready item as abandoned work.
		let state = QueueItemState::ready(None);

		let decoded = decode_queue_item_state(&encode_queue_item_state(&state)).unwrap();

		assert_eq!(decoded, state);
		assert_eq!(decoded.not_before, None);
		assert_eq!(decoded.lease_deadline, None);
		assert_eq!(decoded.backoff_until, None);
	}

	#[test]
	fn test_item_state_roundtrips_with_every_field_set() {
		// The layout is fixed for the whole queue series: later steps write attempt,
		// budget_base, key_hash and the two deadlines into the same record. A field that
		// does not survive the round trip would silently reset a retry budget or a lease.
		let state = QueueItemState {
			status: QueueItemStatus::Leased,
			attempt: 3,
			budget_base: 7,
			key_hash: 0xDEAD_BEEF_CAFE_F00D,
			not_before: Some(DateTime::from_nanos(1_000)),
			lease_deadline: Some(DateTime::from_nanos(2_000)),
			backoff_until: Some(DateTime::from_nanos(3_000)),
		};

		let decoded = decode_queue_item_state(&encode_queue_item_state(&state)).unwrap();

		assert_eq!(decoded, state);
	}

	#[test]
	fn test_every_status_survives_its_tag() {
		// Status drives every transition guard. A tag that decodes to the wrong variant would
		// let a claim take an item that is already done.
		for status in [
			QueueItemStatus::Ready,
			QueueItemStatus::Leased,
			QueueItemStatus::Done,
			QueueItemStatus::Dead,
			QueueItemStatus::Parked,
		] {
			assert_eq!(QueueItemStatus::try_from(status.tag()), Ok(status));
		}
	}

	#[test]
	fn test_an_unknown_status_tag_does_not_decode() {
		// A record written by a newer version must fail loudly rather than being read as
		// Ready, which would re-deliver work that is already finished.
		let mut row = encode_queue_item_state(&QueueItemState::ready(None));
		item_state_shape::SHAPE.set::<u8>(&mut row, item_state_shape::STATUS, 99);

		assert_eq!(decode_queue_item_state(&row), None);
	}

	#[test]
	fn test_partition_counters_roundtrip() {
		// Counters are maintained transitionally and never recomputed, so a lossy codec makes
		// the queue's reported depth drift away from reality permanently.
		let counters = QueuePartitionCounters {
			depth: 42,
			in_flight: 7,
			blocked_keys: 3,
		};

		assert_eq!(decode_queue_partition_counters(&encode_queue_partition_counters(&counters)), counters);
	}

	#[test]
	fn test_an_absent_counter_row_reads_as_zero() {
		// Counter rows are created lazily on first admit, so every reader has to treat an
		// unset row as zeros rather than as garbage depth.
		let row = partition_counters_shape::SHAPE.allocate_pod();

		assert_eq!(decode_queue_partition_counters(&row), QueuePartitionCounters::default());
	}

	#[test]
	fn test_an_attempt_record_roundtrips_with_its_optional_fields_absent() {
		// A live ack carries neither a response nor an anomaly. Those two must decode back as
		// none rather than as empty strings, because the ack classifier distinguishes "no
		// anomaly" from "anomaly recorded" to decide whether an outcome ever transitioned.
		let record = QueueAttemptRecord {
			worker: "worker-1".to_string(),
			outcome: AttemptOutcome::Ok,
			response: None,
			finished_at: DateTime::from_nanos(1_234),
			lost: false,
			anomaly: None,
		};

		let decoded = decode_queue_attempt(&encode_queue_attempt(&record)).unwrap();

		assert_eq!(decoded, record);
		assert_eq!(decoded.response, None);
		assert_eq!(decoded.anomaly, None);
	}

	#[test]
	fn test_an_attempt_record_roundtrips_with_every_field_set() {
		// The stale and conflicting-ack paths write exactly this shape. Losing the anomaly would
		// erase the only evidence that a late ack was seen and deliberately not applied.
		let record = QueueAttemptRecord {
			worker: "10.0.0.1:8080".to_string(),
			outcome: AttemptOutcome::Err,
			response: Some("connection refused".to_string()),
			finished_at: DateTime::from_nanos(9_999),
			lost: true,
			anomaly: Some("stale: item is no longer leased".to_string()),
		};

		assert_eq!(decode_queue_attempt(&encode_queue_attempt(&record)).unwrap(), record);
	}

	#[test]
	fn test_every_attempt_outcome_survives_its_tag() {
		// The outcome drives the transition decision. A tag that decodes to the wrong variant
		// would retry work that was reported successful, or bury work that merely failed once.
		for outcome in [AttemptOutcome::Ok, AttemptOutcome::Err, AttemptOutcome::Dead] {
			assert_eq!(AttemptOutcome::try_from(outcome.tag()), Ok(outcome));
		}
	}

	#[test]
	fn test_an_unknown_attempt_outcome_tag_does_not_decode() {
		// A record written by a newer version must fail loudly rather than read as Ok, which
		// would report someone else's failure as a success.
		let mut row = encode_queue_attempt(&QueueAttemptRecord {
			worker: "w".to_string(),
			outcome: AttemptOutcome::Ok,
			response: None,
			finished_at: DateTime::from_nanos(1),
			lost: false,
			anomaly: None,
		});
		attempt_shape::SHAPE.set::<u8>(&mut row, attempt_shape::OUTCOME, 99);

		assert_eq!(decode_queue_attempt(&row), None);
	}

	#[test]
	fn test_an_item_with_no_not_before_is_due_at_epoch() {
		// Due time is the due-index component: epoch means "immediately claimable" and is
		// computed without a clock so hydration reproduces exactly the same key it lost.
		assert_eq!(QueueItemState::ready(None).due(), DateTime::from_nanos(0));
		assert_eq!(QueueItemState::ready(Some(DateTime::from_nanos(9))).due(), DateTime::from_nanos(9));
	}
}
