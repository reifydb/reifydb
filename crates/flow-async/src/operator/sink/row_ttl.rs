// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeSet, HashMap},
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

		for id in writes.cleared {
			if let Some(due) = current.insert(id, None).flatten() {
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
			if let Some(old) = current.insert(id, Some(due)).flatten() {
				ops.insert(self.entry_key(id.0, old, id.1), None);
			}
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

		let partitions: BTreeSet<Option<Partition>> = ids.iter().map(|id| id.0).collect();
		self.sync_timers(txn, partitions)
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
		let due = self.due_entries(txn, partition, Some(timer.due), CUT_PAGE)?;
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
		self.sync_timers(txn, BTreeSet::from([partition]))?;
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

	fn sync_timers(
		&self,
		txn: &mut DeferredTransaction,
		partitions: BTreeSet<Option<Partition>>,
	) -> Result<(), ValueError> {
		for partition in partitions {
			let arm_key = self.arm_key(partition);
			let armed = match txn.state_get(self.operator, slice::from_ref(&arm_key))?.pop().flatten() {
				Some(row) => Some(DateTime::from_nanos(decode_state::<i64>(&row, "RowTtlArm")?)),
				None => None,
			};
			let earliest = self.due_entries(txn, partition, None, 1)?.first().map(|entry| entry.due);
			if armed == earliest {
				continue;
			}
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
		}
		Ok(())
	}

	fn due_entries(
		&self,
		txn: &mut DeferredTransaction,
		partition: Option<Partition>,
		until: Option<DateTime>,
		limit: usize,
	) -> Result<Vec<DueEntry>, ValueError> {
		let (keyspace, start) = match partition {
			Some(partition) => (
				KeyspaceId::PARTITIONED_ROW_TTL_ENTRY,
				Bound::Included(
					PartitionedRowTtlEntryKey {
						partition: Asc(partition),
						due: BoundedKey::low(),
						row: BoundedKey::low(),
					}
					.to_suffix_bytes(),
				),
			),
			None => (KeyspaceId::ROW_TTL_ENTRY, Bound::Unbounded),
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
}
