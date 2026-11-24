// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap},
	ops::Bound,
	slice,
};

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{
		operator::state::{decode_body, encode},
		pod::EncodedPodRow,
	},
};
use reifydb_core::{
	interface::{
		catalog::{flow::OperatorId, ringbuffer::EvictionVisibility},
		change::Diffs,
	},
	key::{
		any::TaggedKey,
		operator::{
			keyspace::row_ttl::{
				PartitionedRowTtlArm, PartitionedRowTtlArmKey, PartitionedRowTtlEntry,
				PartitionedRowTtlEntryKey, PartitionedRowTtlForward, PartitionedRowTtlForwardKey,
				RowTtlArm, RowTtlArmKey, RowTtlEntry, RowTtlEntryKey, RowTtlForward, RowTtlForwardKey,
			},
			state::{GroupId, GroupStateKey, GroupStateRange, KeyspaceId},
		},
		typed::{BoundedKey, direction::Asc},
	},
	state::{
		timer::TimerKind,
		typed::{SuffixBytes, typed_key},
	},
};
use reifydb_flow::{error::FlowSinkError, operator::sink::RowWrites};
use reifydb_value::{
	error::ValueError,
	value::{datetime::DateTime, duration::Duration, partition::Partition, row_number::RowNumber},
};
use serde::{Serialize, de::DeserializeOwned};

use crate::{
	error::FlowAsyncError,
	operator::TimedOutput,
	timer::{Timer, extension::TimerExtension},
	transaction::{
		FlowTransaction,
		deferred::DeferredTransaction,
		state::{StateExtension, StateRange},
	},
};

const CUT_PAGE: usize = 1024;

type RowId = (Option<Partition>, RowNumber);

pub struct RowTtl {
	operator: OperatorId,
	ttl: Duration,
	eviction: EvictionVisibility,
	partitioned: bool,
}

struct DueEntry {
	due: DateTime,
	row: RowNumber,
	key: EncodedKey,
}

impl RowTtl {
	pub fn new(operator: OperatorId, ttl: Duration, eviction: EvictionVisibility, partitioned: bool) -> Self {
		Self {
			operator,
			ttl,
			eviction,
			partitioned,
		}
	}

	pub fn track(&self, txn: &mut DeferredTransaction, writes: RowWrites) -> Result<(), ValueError> {
		if writes.is_empty() {
			return Ok(());
		}
		let mut ids: Vec<RowId> = writes.cleared.clone();
		ids.extend(writes.written.iter().map(|write| (write.partition, write.row)));
		let mut current = self.stored_dues(txn, &ids)?;
		let mut ops: HashMap<GroupStateKey, Option<EncodedPodRow>> = HashMap::new();
		let mut changes: BTreeMap<Option<Partition>, BTreeMap<DateTime, i64>> = BTreeMap::new();
		for id in &ids {
			changes.entry(id.0).or_default();
		}

		for id in writes.cleared {
			if let Some(due) = current.insert(id, None).flatten() {
				*changes.entry(id.0).or_default().entry(due).or_default() -= 1;
				ops.insert(self.entry_key(id.0, due, id.1), None);
				ops.insert(self.forward_key(id.0, id.1), None);
			}
		}
		for (index, write) in writes.written.into_iter().enumerate() {
			let id = (write.partition, write.row);
			let Some(time) = write.time else {
				return Err(ValueError::from(FlowSinkError::MissingSystemColumn {
					column: "time",
					row_idx: index,
				}));
			};
			let due = time.saturating_add(self.ttl);
			let net = changes.entry(id.0).or_default();
			if let Some(old) = current.insert(id, Some(due)).flatten() {
				*net.entry(old).or_default() -= 1;
				ops.insert(self.entry_key(id.0, old, id.1), None);
			}
			*net.entry(due).or_default() += 1;
			ops.insert(
				self.entry_key(id.0, due, id.1),
				Some(encode_state(&write.key.to_vec(), "RowTtlEntry")?),
			);
			ops.insert(self.forward_key(id.0, id.1), Some(encode_state(&due.to_nanos(), "RowTtlForward")?));
		}

		let mut sets = Vec::new();
		let mut removes = Vec::new();
		for (key, op) in ops {
			match op {
				Some(row) => sets.push((key, row)),
				None => removes.push(key),
			}
		}
		if !removes.is_empty() {
			txn.state_remove(self.operator, &removes)?;
		}
		if !sets.is_empty() {
			txn.state_set(self.operator, sets)?;
		}

		for (partition, net) in changes {
			let armed = self.armed_due(txn, partition)?;
			let added = net.iter().find(|(_, count)| **count > 0).map(|(due, _)| *due);
			let lower = match (armed, added) {
				(Some(armed), Some(added)) => Some(armed.min(added)),
				(armed, added) => armed.or(added),
			};
			let armed_removed = armed.is_some_and(|at| net.get(&at).is_some_and(|count| *count < 0));
			let earliest = match lower {
				Some(from) if armed_removed => self.earliest_from(txn, partition, from)?,
				lower => lower,
			};
			self.arm(txn, partition, armed, earliest)?;
		}
		Ok(())
	}

	pub fn on_timer(
		&self,
		txn: &mut DeferredTransaction,
		timer: &Timer,
	) -> Result<Option<TimedOutput>, ValueError> {
		if timer.kind != TimerKind::RowTtl {
			return Ok(None);
		}
		let partition = self.timer_partition(&timer.key)?;
		let due = self.due_entries(txn, partition, Some(timer.due), Some(timer.due), CUT_PAGE)?;
		let cut = !due.is_empty();
		if cut {
			let mut removes = Vec::with_capacity(due.len() * 2);
			let mut keys = Vec::with_capacity(due.len());
			for entry in due {
				removes.push(self.entry_key(partition, entry.due, entry.row));
				removes.push(self.forward_key(partition, entry.row));
				keys.push(entry.key);
			}
			txn.state_remove(self.operator, &removes)?;
			match self.eviction {
				EvictionVisibility::Visible => txn.pending_mut().remove(keys),
				EvictionVisibility::Silent => {
					for key in keys {
						txn.pending_mut().remove_silent(key);
					}
				}
			}
		}
		let armed = self.armed_due(txn, partition)?;
		let earliest = self.earliest_from(txn, partition, timer.due)?;
		self.arm(txn, partition, armed, earliest)?;
		Ok(cut.then(|| TimedOutput::new(Diffs::new(), timer.due)))
	}

	fn stored_dues(
		&self,
		txn: &mut DeferredTransaction,
		ids: &[RowId],
	) -> Result<HashMap<RowId, Option<DateTime>>, ValueError> {
		let keys: Vec<GroupStateKey> = ids.iter().map(|id| self.forward_key(id.0, id.1)).collect();
		let mut dues = HashMap::with_capacity(ids.len());
		for (id, row) in ids.iter().zip(txn.state_get(self.operator, &keys)?) {
			let due = match row {
				Some(row) => Some(DateTime::from_nanos(decode_state::<i64>(&row, "RowTtlForward")?)),
				None => None,
			};
			dues.entry(*id).or_insert(due);
		}
		Ok(dues)
	}

	fn armed_due(
		&self,
		txn: &mut DeferredTransaction,
		partition: Option<Partition>,
	) -> Result<Option<DateTime>, ValueError> {
		let arm_key = self.arm_key(partition);
		match txn.state_get(self.operator, slice::from_ref(&arm_key))?.pop().flatten() {
			Some(row) => Ok(Some(DateTime::from_nanos(decode_state::<i64>(&row, "RowTtlArm")?))),
			None => Ok(None),
		}
	}

	fn earliest_from(
		&self,
		txn: &mut DeferredTransaction,
		partition: Option<Partition>,
		from: DateTime,
	) -> Result<Option<DateTime>, ValueError> {
		Ok(self.due_entries(txn, partition, Some(from), None, 1)?.first().map(|entry| entry.due))
	}

	fn arm(
		&self,
		txn: &mut DeferredTransaction,
		partition: Option<Partition>,
		armed: Option<DateTime>,
		earliest: Option<DateTime>,
	) -> Result<(), ValueError> {
		if armed == earliest {
			return Ok(());
		}
		let arm_key = self.arm_key(partition);
		let key = self.timer_key(partition);
		if let Some(at) = armed {
			txn.disarm_timer(
				self.operator,
				&Timer {
					due: at,
					kind: TimerKind::RowTtl,
					key: key.clone(),
				},
			)?;
		}
		match earliest {
			Some(at) => {
				txn.arm_timer(
					self.operator,
					&Timer {
						due: at,
						kind: TimerKind::RowTtl,
						key,
					},
				)?;
				txn.state_set(
					self.operator,
					vec![(arm_key, encode_state(&at.to_nanos(), "RowTtlArm")?)],
				)?;
			}
			None => txn.state_remove(self.operator, &[arm_key])?,
		}
		Ok(())
	}

	fn due_entries(
		&self,
		txn: &mut DeferredTransaction,
		partition: Option<Partition>,
		from: Option<DateTime>,
		until: Option<DateTime>,
		limit: usize,
	) -> Result<Vec<DueEntry>, ValueError> {
		let (keyspace, start) = match (partition, from) {
			(Some(partition), from) => (
				KeyspaceId::PARTITIONED_ROW_TTL_ENTRY,
				Bound::Included(
					PartitionedRowTtlEntryKey {
						partition: Asc(partition),
						due: from.map_or_else(BoundedKey::low, Asc),
						row: BoundedKey::low(),
					}
					.to_suffix_bytes(),
				),
			),
			(None, Some(from)) => (
				KeyspaceId::ROW_TTL_ENTRY,
				Bound::Included(
					RowTtlEntryKey {
						due: Asc(from),
						row: BoundedKey::low(),
					}
					.to_suffix_bytes(),
				),
			),
			(None, None) => (KeyspaceId::ROW_TTL_ENTRY, Bound::Unbounded),
		};
		let range = GroupStateRange::Keyspace {
			group: GroupId::ROOT,
			keyspace,
			start,
			end: Bound::Unbounded,
		};
		let batch = txn.state_range(self.operator, StateRange::forward(range, "row_ttl::due").limit(limit))?;
		let mut due = Vec::with_capacity(batch.items.len());
		for item in batch.items {
			let Some((at, owner, row)) = self.decode_entry(&item.key) else {
				return Err(ValueError::from(FlowAsyncError::Decode {
					state: "RowTtlEntry",
					cause: "a row ttl entry key must decode as its keyspace".to_string(),
				}));
			};
			if owner != partition || until.is_some_and(|until| at > until) {
				break;
			}
			let bytes: Vec<u8> = decode_state(&EncodedPodRow::from(item.bytes), "RowTtlEntry")?;
			due.push(DueEntry {
				due: at,
				row,
				key: EncodedKey::new(bytes),
			});
		}
		Ok(due)
	}

	fn decode_entry(&self, key: &TaggedKey) -> Option<(DateTime, Option<Partition>, RowNumber)> {
		let TaggedKey::OperatorState(decoded) = key else {
			return None;
		};
		if self.partitioned {
			let entry = PartitionedRowTtlEntryKey::from_suffix_bytes(&decoded.suffix)?;
			Some((entry.due.0, Some(entry.partition.0), entry.row.0))
		} else {
			let entry = RowTtlEntryKey::from_suffix_bytes(&decoded.suffix)?;
			Some((entry.due.0, None, entry.row.0))
		}
	}

	fn entry_key(&self, partition: Option<Partition>, due: DateTime, row: RowNumber) -> GroupStateKey {
		match partition {
			Some(partition) => typed_key::<PartitionedRowTtlEntry>(
				GroupId::ROOT,
				&PartitionedRowTtlEntryKey {
					partition: Asc(partition),
					due: Asc(due),
					row: Asc(row),
				},
			),
			None => typed_key::<RowTtlEntry>(
				GroupId::ROOT,
				&RowTtlEntryKey {
					due: Asc(due),
					row: Asc(row),
				},
			),
		}
	}

	fn forward_key(&self, partition: Option<Partition>, row: RowNumber) -> GroupStateKey {
		match partition {
			Some(partition) => typed_key::<PartitionedRowTtlForward>(
				GroupId::ROOT,
				&PartitionedRowTtlForwardKey {
					partition: Asc(partition),
					row: Asc(row),
				},
			),
			None => typed_key::<RowTtlForward>(
				GroupId::ROOT,
				&RowTtlForwardKey {
					row: Asc(row),
				},
			),
		}
	}

	fn arm_key(&self, partition: Option<Partition>) -> GroupStateKey {
		match partition {
			Some(partition) => typed_key::<PartitionedRowTtlArm>(
				GroupId::ROOT,
				&PartitionedRowTtlArmKey {
					partition: Asc(partition),
				},
			),
			None => typed_key::<RowTtlArm>(GroupId::ROOT, &RowTtlArmKey {}),
		}
	}

	fn timer_key(&self, partition: Option<Partition>) -> EncodedKey {
		match partition {
			Some(partition) => EncodedKey::new(
				PartitionedRowTtlArmKey {
					partition: Asc(partition),
				}
				.to_suffix_bytes(),
			),
			None => EncodedKey::new(Vec::new()),
		}
	}

	fn timer_partition(&self, key: &EncodedKey) -> Result<Option<Partition>, ValueError> {
		if !self.partitioned {
			return Ok(None);
		}
		PartitionedRowTtlArmKey::from_suffix_bytes(key.as_slice()).map(|key| Some(key.partition.0)).ok_or_else(
			|| {
				ValueError::from(FlowAsyncError::Decode {
					state: "RowTtlArm",
					cause: "a row ttl timer key must decode as a partition".to_string(),
				})
			},
		)
	}
}

fn encode_state<T: Serialize>(value: &T, state: &'static str) -> Result<EncodedPodRow, ValueError> {
	encode(value).map_err(|e| {
		ValueError::from(FlowAsyncError::Encode {
			state,
			cause: e.to_string(),
		})
	})
}

fn decode_state<T: DeserializeOwned>(row: &EncodedPodRow, state: &'static str) -> Result<T, ValueError> {
	decode_body(row).map_err(|e| {
		ValueError::from(FlowAsyncError::Decode {
			state,
			cause: e.to_string(),
		})
	})
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		actors::pending::PendingWrite,
		delta::RemoveVisibility,
		interface::catalog::{id::ViewId, storage::StorageId},
		key::row::RowKey,
		metrics::scan::PageRequests,
	};
	use reifydb_flow::operator::sink::RowWrite;
	use reifydb_test_harness::engine::TestEngine;

	use super::*;
	use crate::transaction::mock::FlowTxn;

	const T0: i64 = 1_000_000_000_000;
	const HOUR: i64 = 3_600 * 1_000_000_000;
	const HALF: i64 = HOUR / 2;
	const VIEW: StorageId = StorageId::View(ViewId(1));

	fn row_ttl(eviction: EvictionVisibility) -> RowTtl {
		RowTtl::new(OperatorId(1), Duration::from_hours(1).unwrap(), eviction, false)
	}

	fn written(row: u64, time: i64) -> RowWrites {
		RowWrites {
			cleared: Vec::new(),
			written: vec![RowWrite {
				partition: None,
				row: RowNumber(row),
				key: RowKey::encoded(VIEW, RowNumber(row)),
				time: Some(DateTime::from_nanos(time)),
			}],
		}
	}

	fn fire(ttl: &RowTtl, txn: &mut DeferredTransaction, due: i64) -> Option<TimedOutput> {
		let timer = Timer {
			due: DateTime::from_nanos(due),
			kind: TimerKind::RowTtl,
			key: ttl.timer_key(None),
		};
		ttl.on_timer(txn, &timer).unwrap()
	}

	fn row_removes(txn: &DeferredTransaction) -> Vec<(EncodedKey, PendingWrite)> {
		txn.pending
			.rows_ordered()
			.filter(|(key, write)| {
				matches!(
					TaggedKey::decode(key),
					Some(TaggedKey::Row(RowKey {
						storage: VIEW,
						..
					}))
				) && matches!(write, PendingWrite::Remove { .. })
			})
			.map(|(key, write)| (key.clone(), write.clone()))
			.collect()
	}

	#[test]
	fn an_update_moves_the_expiry_to_the_new_time() {
		// A stale entry left by the first write would cut a row that was refreshed half an hour later.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().clock_millis(0).deferred();
		let ttl = row_ttl(EvictionVisibility::Visible);
		ttl.track(&mut txn, written(1, T0)).unwrap();
		ttl.track(&mut txn, written(1, T0 + HALF)).unwrap();

		assert!(fire(&ttl, &mut txn, T0 + HOUR).is_none(), "the first due must no longer cut the row");
		assert!(row_removes(&txn).is_empty(), "a refreshed row must survive its old due");

		assert!(fire(&ttl, &mut txn, T0 + HALF + HOUR).is_some(), "the moved due must cut the row");
		let removes = row_removes(&txn);
		assert_eq!(removes.len(), 1, "the row must be cut exactly once: {removes:?}");
		assert_eq!(removes[0].0, RowKey::encoded(VIEW, RowNumber(1)), "the cut must hit the written key");
	}

	#[test]
	fn a_cleared_row_is_never_cut() {
		// A removed row whose entry survives would be cut again later and retract a row that is gone.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().clock_millis(0).deferred();
		let ttl = row_ttl(EvictionVisibility::Visible);
		ttl.track(&mut txn, written(1, T0)).unwrap();
		ttl.track(
			&mut txn,
			RowWrites {
				cleared: vec![(None, RowNumber(1))],
				written: Vec::new(),
			},
		)
		.unwrap();

		assert!(fire(&ttl, &mut txn, T0 + HOUR).is_none(), "a cleared row must have no due left");
		assert!(row_removes(&txn).is_empty(), "a cleared row must never be cut");
	}

	fn cut_visibility(eviction: EvictionVisibility) -> Vec<PendingWrite> {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().clock_millis(0).deferred();
		let ttl = row_ttl(eviction);
		ttl.track(&mut txn, written(1, T0)).unwrap();
		ttl.track(&mut txn, written(2, T0)).unwrap();
		assert!(fire(&ttl, &mut txn, T0 + HOUR).is_some(), "both rows are due at the timer");
		row_removes(&txn).into_iter().map(|(_, write)| write).collect()
	}

	#[test]
	fn a_visible_cut_announces_every_remove() {
		// An unannounced cut here leaves the expired rows in every downstream view of a visible ttl view.
		let removes = cut_visibility(EvictionVisibility::Visible);
		assert_eq!(removes.len(), 2, "both due rows must be cut: {removes:?}");
		assert!(
			removes.iter().all(|write| matches!(
				write,
				PendingWrite::Remove {
					announce: RemoveVisibility::Announced
				}
			)),
			"a visible cut must announce every remove: {removes:?}"
		);
	}

	#[test]
	fn a_silent_cut_never_reaches_the_change_feed() {
		// A default ttl view must expire unobserved, so its cut must never travel as an announced remove.
		let removes = cut_visibility(EvictionVisibility::Silent);
		assert_eq!(removes.len(), 2, "both due rows must be cut: {removes:?}");
		assert!(
			removes.iter().all(|write| matches!(
				write,
				PendingWrite::Remove {
					announce: RemoveVisibility::Silent
				}
			)),
			"a silent cut must keep every remove silent: {removes:?}"
		);
	}

	#[test]
	fn a_row_without_time_is_refused() {
		// A row with no #time has no due, so accepting it would keep it forever under a ttl.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().clock_millis(0).deferred();
		let ttl = row_ttl(EvictionVisibility::Visible);
		let mut writes = written(1, T0);
		writes.written[0].time = None;
		assert!(ttl.track(&mut txn, writes).is_err(), "a ttl row without #time must be an error");
	}

	fn cleared(row: u64) -> RowWrites {
		RowWrites {
			cleared: vec![(None, RowNumber(row))],
			written: Vec::new(),
		}
	}

	fn armed(ttl: &RowTtl, txn: &mut DeferredTransaction) -> Option<i64> {
		let key = ttl.arm_key(None);
		txn.state_get(ttl.operator, slice::from_ref(&key))
			.unwrap()
			.pop()
			.flatten()
			.map(|row| decode_state::<i64>(&row, "RowTtlArm").unwrap())
	}

	fn assert_armed_is_earliest(ttl: &RowTtl, txn: &mut DeferredTransaction) {
		// The armed due must equal the earliest live entry due, otherwise a due row outlives its ttl.
		let earliest =
			ttl.due_entries(txn, None, None, None, 1).unwrap().first().map(|entry| entry.due.to_nanos());
		assert_eq!(armed(ttl, txn), earliest, "the armed due must be the earliest live entry due");
	}

	#[test]
	fn a_write_that_leaves_the_armed_row_alone_reads_no_range() {
		// A range read per sink write walks every dead entry in front of the earliest due, so it must never
		// happen here.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().clock_millis(0).deferred();
		let ttl = row_ttl(EvictionVisibility::Visible);
		ttl.track(&mut txn, written(1, T0)).unwrap();
		ttl.track(&mut txn, written(2, T0 + HALF)).unwrap();
		ttl.track(&mut txn, written(3, T0 + HOUR)).unwrap();

		let before = PageRequests::sample();
		ttl.track(&mut txn, written(3, T0 + HOUR + HALF)).unwrap();
		assert_eq!(before.since(), 0, "a write away from the armed row must request no range page");

		assert_eq!(armed(&ttl, &mut txn), Some(T0 + HOUR), "the timer must stay on the untouched earliest row");
		assert_armed_is_earliest(&ttl, &mut txn);
	}

	#[test]
	fn updating_the_armed_row_to_a_later_time_arms_the_next_earliest() {
		// A timer left on the moved due would fire on nothing and never cut the row that is now earliest.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().clock_millis(0).deferred();
		let ttl = row_ttl(EvictionVisibility::Visible);
		ttl.track(&mut txn, written(1, T0)).unwrap();
		ttl.track(&mut txn, written(2, T0 + HALF)).unwrap();
		assert_armed_is_earliest(&ttl, &mut txn);

		ttl.track(&mut txn, written(1, T0 + HOUR)).unwrap();
		assert_eq!(armed(&ttl, &mut txn), Some(T0 + HALF + HOUR), "the timer must move to the second row");
		assert_armed_is_earliest(&ttl, &mut txn);
	}

	#[test]
	fn clearing_the_armed_row_arms_the_next_earliest() {
		// A timer kept on a cleared row must not hide the due of the rows that are left.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().clock_millis(0).deferred();
		let ttl = row_ttl(EvictionVisibility::Visible);
		ttl.track(&mut txn, written(1, T0)).unwrap();
		ttl.track(&mut txn, written(2, T0 + HALF)).unwrap();

		ttl.track(&mut txn, cleared(1)).unwrap();
		assert_eq!(
			armed(&ttl, &mut txn),
			Some(T0 + HALF + HOUR),
			"the timer must move to the row that is left"
		);
		assert_armed_is_earliest(&ttl, &mut txn);
	}

	#[test]
	fn clearing_the_last_row_disarms() {
		// An arm value that survives the last row would make the next first write skip arming its timer.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().clock_millis(0).deferred();
		let ttl = row_ttl(EvictionVisibility::Visible);
		ttl.track(&mut txn, written(1, T0)).unwrap();

		ttl.track(&mut txn, cleared(1)).unwrap();
		assert_eq!(armed(&ttl, &mut txn), None, "no row left must mean no armed due");
		assert_armed_is_earliest(&ttl, &mut txn);

		ttl.track(&mut txn, written(2, T0 + HALF)).unwrap();
		assert_eq!(armed(&ttl, &mut txn), Some(T0 + HALF + HOUR), "the next first row must arm its due");
		assert_armed_is_earliest(&ttl, &mut txn);
	}

	#[test]
	fn a_row_due_before_the_armed_one_rearms_earlier() {
		// A timer kept on the later due would cut the new row late by the gap between the two dues.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().clock_millis(0).deferred();
		let ttl = row_ttl(EvictionVisibility::Visible);
		ttl.track(&mut txn, written(1, T0 + HALF)).unwrap();

		ttl.track(&mut txn, written(2, T0)).unwrap();
		assert_eq!(armed(&ttl, &mut txn), Some(T0 + HOUR), "the timer must move to the earlier due");
		assert_armed_is_earliest(&ttl, &mut txn);
	}

	#[test]
	fn clearing_one_of_two_rows_with_the_same_due_keeps_the_timer() {
		// Two rows may share a due, so one leaving must never move the timer off the other.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().clock_millis(0).deferred();
		let ttl = row_ttl(EvictionVisibility::Visible);
		ttl.track(&mut txn, written(1, T0)).unwrap();
		ttl.track(&mut txn, written(2, T0)).unwrap();
		ttl.track(&mut txn, written(3, T0 + HALF)).unwrap();

		ttl.track(&mut txn, cleared(1)).unwrap();
		assert_eq!(armed(&ttl, &mut txn), Some(T0 + HOUR), "the row that shares the due must keep the timer");
		assert_armed_is_earliest(&ttl, &mut txn);

		assert!(
			fire(&ttl, &mut txn, T0 + HOUR).is_some(),
			"the shared due must still cut the row that is left"
		);
		let removes = row_removes(&txn);
		assert_eq!(removes.len(), 1, "only the row that stayed must be cut: {removes:?}");
		assert_eq!(removes[0].0, RowKey::encoded(VIEW, RowNumber(2)), "the cut must hit the row that stayed");
	}

	#[test]
	fn rewriting_the_armed_row_with_the_same_time_keeps_the_timer() {
		// A rewrite with an unchanged time removes and re-adds one entry key, which must never read as a
		// removal.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().clock_millis(0).deferred();
		let ttl = row_ttl(EvictionVisibility::Visible);
		ttl.track(&mut txn, written(1, T0)).unwrap();
		ttl.track(&mut txn, written(2, T0 + HALF)).unwrap();

		ttl.track(&mut txn, written(1, T0)).unwrap();
		assert_eq!(armed(&ttl, &mut txn), Some(T0 + HOUR), "an unchanged due must keep the timer");
		assert_armed_is_earliest(&ttl, &mut txn);
	}
	const P1: Partition = Partition(1);
	const P2: Partition = Partition(2);

	fn partitioned_row_ttl() -> RowTtl {
		RowTtl::new(OperatorId(1), Duration::from_hours(1).unwrap(), EvictionVisibility::Visible, true)
	}

	fn written_in(partition: Partition, row: u64, time: i64) -> RowWrites {
		let mut writes = written(row, time);
		writes.written[0].partition = Some(partition);
		writes
	}

	fn armed_in(ttl: &RowTtl, txn: &mut DeferredTransaction, partition: Partition) -> Option<i64> {
		let key = ttl.arm_key(Some(partition));
		txn.state_get(ttl.operator, slice::from_ref(&key))
			.unwrap()
			.pop()
			.flatten()
			.map(|row| decode_state::<i64>(&row, "RowTtlArm").unwrap())
	}

	fn assert_armed_is_earliest_in(ttl: &RowTtl, txn: &mut DeferredTransaction, partition: Partition) {
		// Each partition arms its own earliest due, otherwise a neighbour's rows decide when this one is cut.
		let earliest = ttl
			.due_entries(txn, Some(partition), None, None, 1)
			.unwrap()
			.first()
			.map(|entry| entry.due.to_nanos());
		assert_eq!(
			armed_in(ttl, txn, partition),
			earliest,
			"the armed due must be the partition's earliest due"
		);
	}

	#[test]
	fn a_partitioned_write_away_from_the_armed_row_reads_no_range() {
		// A partitioned view writes far more often than it expires, so its write path must never range read.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().clock_millis(0).deferred();
		let ttl = partitioned_row_ttl();
		ttl.track(&mut txn, written_in(P1, 1, T0)).unwrap();
		ttl.track(&mut txn, written_in(P2, 2, T0 + HALF)).unwrap();
		ttl.track(&mut txn, written_in(P2, 3, T0 + HOUR)).unwrap();

		let before = PageRequests::sample();
		ttl.track(&mut txn, written_in(P2, 3, T0 + HOUR + HALF)).unwrap();
		assert_eq!(before.since(), 0, "a write away from the armed row must request no range page");

		assert_eq!(armed_in(&ttl, &mut txn, P1), Some(T0 + HOUR), "the other partition must keep its timer");
		assert_eq!(
			armed_in(&ttl, &mut txn, P2),
			Some(T0 + HALF + HOUR),
			"the written partition must keep its timer"
		);
		assert_armed_is_earliest_in(&ttl, &mut txn, P1);
		assert_armed_is_earliest_in(&ttl, &mut txn, P2);
	}

	#[test]
	fn updating_a_partitions_armed_row_arms_its_own_next_earliest() {
		// The scan from the armed due must stay inside the partition and start at that due, never at the
		// partition start.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().clock_millis(0).deferred();
		let ttl = partitioned_row_ttl();
		ttl.track(&mut txn, written_in(P1, 1, T0)).unwrap();
		ttl.track(&mut txn, written_in(P1, 2, T0 + HOUR)).unwrap();
		ttl.track(&mut txn, written_in(P2, 3, T0 + HALF)).unwrap();

		ttl.track(&mut txn, written_in(P1, 1, T0 + 2 * HOUR)).unwrap();
		assert_eq!(
			armed_in(&ttl, &mut txn, P1),
			Some(T0 + 2 * HOUR),
			"the timer must move to the partition's second row"
		);
		assert_eq!(armed_in(&ttl, &mut txn, P2), Some(T0 + HALF + HOUR), "the neighbour must keep its timer");
		assert_armed_is_earliest_in(&ttl, &mut txn, P1);
		assert_armed_is_earliest_in(&ttl, &mut txn, P2);
	}

	#[test]
	fn clearing_a_partitions_last_row_never_arms_on_a_neighbour() {
		// A scan that runs past the partition end would arm this partition on the next partition's due.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().clock_millis(0).deferred();
		let ttl = partitioned_row_ttl();
		ttl.track(&mut txn, written_in(P1, 1, T0)).unwrap();
		ttl.track(&mut txn, written_in(P2, 2, T0 + HALF)).unwrap();

		ttl.track(
			&mut txn,
			RowWrites {
				cleared: vec![(Some(P1), RowNumber(1))],
				written: Vec::new(),
			},
		)
		.unwrap();
		assert_eq!(armed_in(&ttl, &mut txn, P1), None, "an empty partition must have no armed due");
		assert_eq!(armed_in(&ttl, &mut txn, P2), Some(T0 + HALF + HOUR), "the neighbour must keep its timer");
		assert_armed_is_earliest_in(&ttl, &mut txn, P1);
		assert_armed_is_earliest_in(&ttl, &mut txn, P2);
	}

	#[test]
	fn a_partitions_timer_cuts_only_its_own_rows() {
		// A cut that starts at the timer due must never reach a neighbour that shares the same due.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().clock_millis(0).deferred();
		let ttl = partitioned_row_ttl();
		ttl.track(&mut txn, written_in(P1, 1, T0)).unwrap();
		ttl.track(&mut txn, written_in(P1, 2, T0 + HALF)).unwrap();
		ttl.track(&mut txn, written_in(P2, 3, T0)).unwrap();

		let timer = Timer {
			due: DateTime::from_nanos(T0 + HOUR),
			kind: TimerKind::RowTtl,
			key: ttl.timer_key(Some(P1)),
		};
		assert!(ttl.on_timer(&mut txn, &timer).unwrap().is_some(), "the partition's due row must be cut");

		let removes = row_removes(&txn);
		assert_eq!(removes.len(), 1, "only the fired partition's due row must be cut: {removes:?}");
		assert_eq!(
			removes[0].0,
			RowKey::encoded(VIEW, RowNumber(1)),
			"the cut must hit the fired partition's row"
		);
		assert_eq!(
			armed_in(&ttl, &mut txn, P1),
			Some(T0 + HALF + HOUR),
			"the timer must move to the row that is left"
		);
		assert_eq!(armed_in(&ttl, &mut txn, P2), Some(T0 + HOUR), "the neighbour must keep its timer");
		assert_armed_is_earliest_in(&ttl, &mut txn, P1);
		assert_armed_is_earliest_in(&ttl, &mut txn, P2);
	}
}
