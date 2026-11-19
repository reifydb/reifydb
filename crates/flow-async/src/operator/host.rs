// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	borrow::Cow,
	collections::{BTreeMap, BTreeSet, HashMap},
	ops::Bound,
};

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{bytes::EncodedBytes, pod::EncodedPodRow, shape::RowShape},
};
use reifydb_core::{
	actors::pending::PendingWrite,
	common::{CommitVersion, SourceVersion},
	interface::catalog::{
		config::{ConfigKey, GetConfig},
		dictionary::Dictionary,
		flow::OperatorId,
		id::ViewId,
		storage::StorageId,
	},
	internal_err,
	key::{
		any::TaggedKey,
		operator::{
			keyspace::join::JoinRowMappingKey,
			state::{GroupId, GroupStateKey, GroupStateRange, KeyspaceId, KeyspaceMask},
		},
		row::StoragePartitionedRowKey,
	},
	state::{
		batch::StateBatch,
		timer::{GroupSweep, StateStore, TimerKind, TimerMove, TimerStore},
	},
};
use reifydb_flow::txn::{Find, Intern, Resolve};
use reifydb_transaction::multi::RangeScope;
use reifydb_value::{
	byte_size::ByteSize,
	error::ValueError,
	value::{
		Value,
		datetime::DateTime,
		dictionary::{DictionaryEntryId, DictionaryId},
		duration::Duration,
		partition::Partition,
		row_number::RowNumber,
		value_type::ValueType,
	},
};

use crate::{
	error::FlowAsyncError,
	operator::state::{
		due::{due_drop, due_get, due_set, managed_due_key, managed_latest_key},
		forget_pass::IdentityForgetPass,
		forget_pass_outcome::ForgetPassOutcome,
		seal::rule::SEAL_GATE_STEP,
	},
	timer::{Timer, extension::TimerExtension},
	transaction::{
		FlowTransaction, LookupAt,
		dictionary::DictionaryExtension,
		forget_pass::ForgetPassExtension,
		join_forget::{DueStart, JoinDueEntry, JoinDuePage, JoinRowForgetExtension},
		row_number::RowNumberExtension,
		state::{StateExtension, StateRange},
	},
	window::driver::store::WindowStore,
};

pub trait HostContext: WindowStore + Resolve {
	fn version(&self) -> CommitVersion;

	fn snapshot_version(&self) -> CommitVersion {
		self.version()
	}

	fn now(&self) -> DateTime;

	fn disarm_timer_by_key(&mut self, kind: TimerKind, key: &EncodedKey) -> Result<(), ValueError>;

	fn join_forget_arm(&mut self, side: u8, rows: &[(GroupId, RowNumber, DateTime)]) -> Result<(), ValueError>;

	fn join_forget_clear(
		&mut self,
		side: u8,
		rows: &[(GroupId, RowNumber)],
	) -> Result<Vec<Option<DateTime>>, ValueError>;

	fn join_forget_free(&mut self, entries: &[JoinDueEntry]) -> Result<(), ValueError>;

	fn join_forget_min(&mut self) -> Result<Option<DateTime>, ValueError>;

	fn join_due_page(&mut self, at: DateTime, budget: usize, start: &DueStart) -> Result<JoinDuePage, ValueError>;

	fn config_uint8(&self, key: ConfigKey) -> u64;

	fn row_shape_cache(&mut self) -> &mut HashMap<EncodedKey, RowShape>;

	fn state_range(&mut self, range: GroupStateRange) -> Result<Vec<(GroupStateKey, EncodedPodRow)>, ValueError>;

	fn state_range_limited(
		&mut self,
		range: GroupStateRange,
		limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>, ValueError>;

	fn state_range_limited_visit(
		&mut self,
		range: GroupStateRange,
		limit: Option<usize>,
		visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<(), ValueError>,
	) -> Result<(), ValueError> {
		for (key, row) in self.state_range_limited(range, limit)? {
			visit(key, row)?;
		}
		Ok(())
	}

	fn forget_group_identities(
		&mut self,
		groups: &[GroupId],
		limit: usize,
	) -> Result<Vec<ForgetPassOutcome>, ValueError>;

	fn get_join_row_numbers(&mut self, keys: &[JoinRowMappingKey]) -> Result<Vec<Option<RowNumber>>, ValueError>;

	fn get_or_create_join_row_numbers(
		&mut self,
		keys: &[JoinRowMappingKey],
	) -> Result<Vec<(RowNumber, bool)>, ValueError>;

	fn remove_join_row_numbers(&mut self, keys: &[JoinRowMappingKey]) -> Result<(), ValueError>;

	fn remove_join_row_numbers_for_left(&mut self, tag: u8, left: u64) -> Result<(), ValueError>;

	fn dictionary_id_type(&mut self, dictionary: DictionaryId) -> Option<ValueType>;

	fn dictionary_find(
		&mut self,
		dictionary: DictionaryId,
		values: &[Value],
	) -> Result<Vec<Option<DictionaryEntryId>>, ValueError>;

	fn dictionary_get(
		&mut self,
		dictionary: DictionaryId,
		ids: &[DictionaryEntryId],
	) -> Result<Vec<Option<Value>>, ValueError>;

	fn dictionary_intern(
		&mut self,
		dictionary: &Dictionary,
		values: &[Value],
	) -> Result<Vec<DictionaryEntryId>, ValueError>;

	fn lookup_read(
		&mut self,
		storage: StorageId,
		version: CommitVersion,
		partitions: &[Partition],
	) -> Result<Vec<Option<(RowNumber, EncodedBytes)>>, ValueError>;

	fn lookup_view_read(&self, view: ViewId, source: SourceVersion) -> LookupAt;

	fn lookup_read_at(
		&mut self,
		storage: StorageId,
		at: LookupAt,
		partitions: &[Partition],
	) -> Result<Vec<Option<(RowNumber, EncodedBytes)>>, ValueError> {
		match at {
			LookupAt::Commit(version) => self.lookup_read(storage, version, partitions),
			LookupAt::Layered {
				..
			} => panic!("lookup at {at:?} reads inside a layered commit, which this host cannot read"),
		}
	}

	fn lookup_floor(&self) -> Option<CommitVersion>;
}

pub struct TxnHostContext<'a, T: FlowTransaction> {
	txn: &'a mut T,
	operator: OperatorId,
	batch_time: DateTime,
	retention: Option<Duration>,
	#[cfg(test)]
	pub dictionary_resolves: usize,
}

impl<'a, T: FlowTransaction> TxnHostContext<'a, T> {
	pub fn new(txn: &'a mut T, operator: OperatorId) -> Self {
		Self::with_retention(txn, operator, None)
	}

	pub fn with_retention(txn: &'a mut T, operator: OperatorId, retention: Option<Duration>) -> Self {
		let batch_time = txn.batch_time();
		Self {
			txn,
			operator,
			batch_time,
			retention,
			#[cfg(test)]
			dictionary_resolves: 0,
		}
	}

	fn arm_forget_pass(&mut self, group: GroupId) -> Result<(), ValueError> {
		let Some(retention) = self.retention else {
			return internal_err!("operator {} wrote managed state but has no retention", self.operator);
		};
		let latest = managed_latest_key(group);
		let previous = due_get::<DateTime>(self, &latest)?;
		let due = forget_pass_due(self.batch_time, retention);
		let due = previous.map_or(due, |previous| previous.max(due));
		if let Some(previous) = previous.filter(|previous| *previous != due) {
			due_drop(self, &managed_due_key(previous, group))?;
		}
		due_set(self, managed_due_key(due, group), Vec::<u8>::new())?;
		due_set(self, latest, due)?;
		self.txn.arm_timer(
			self.operator,
			&Timer {
				due,
				kind: TimerKind::Reclaim,
				key: EncodedKey::new(Vec::new()),
			},
		)
	}
}

fn managed_group(key: &GroupStateKey) -> Option<GroupId> {
	if key.keyspace() != Some(KeyspaceId::CUSTOM_MANAGED) {
		return None;
	}
	key.group().filter(|group| !group.is_root())
}

const NANOS_PER_SECOND: i64 = 1_000_000_000;

fn forget_pass_due(batch_time: DateTime, retention: Duration) -> DateTime {
	let free = batch_time.saturating_add(retention).saturating_add(SEAL_GATE_STEP);
	let nanos = free.to_nanos();
	let ceil = nanos.div_euclid(NANOS_PER_SECOND) + i64::from(nanos.rem_euclid(NANOS_PER_SECOND) != 0);
	DateTime::from_nanos(ceil.saturating_mul(NANOS_PER_SECOND))
}

impl<T: FlowTransaction> TimerStore for TxnHostContext<'_, T> {
	fn arm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<(), ValueError> {
		self.txn.arm_timer(
			self.operator,
			&Timer {
				due,
				kind,
				key: key.clone(),
			},
		)
	}

	fn disarm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<(), ValueError> {
		self.txn.disarm_timer(
			self.operator,
			&Timer {
				due,
				kind,
				key: key.clone(),
			},
		)
	}

	fn flow_watermark(&mut self) -> Result<Option<DateTime>, ValueError> {
		Ok(self.txn.flow_watermark())
	}

	fn rearm_timers(&mut self, kind: TimerKind, moves: &[TimerMove]) -> Result<(), ValueError> {
		self.txn.rearm_timers(self.operator, kind, moves)
	}
}

impl<T: FlowTransaction> StateStore for TxnHostContext<'_, T> {
	fn state_forgets_groups(&self) -> bool {
		self.txn.operator_store().persistent().forgets_groups()
	}

	fn state_get(&mut self, keys: &[GroupStateKey]) -> Result<Vec<Option<EncodedPodRow>>, ValueError> {
		self.txn.state_get(self.operator, keys)
	}

	fn state_classify(&mut self, key: &GroupStateKey, pre: Option<ByteSize>) {
		self.txn.state_classify(self.operator, key, pre);
	}

	fn state_remove(&mut self, keys: &[GroupStateKey]) -> Result<(), ValueError> {
		self.txn.state_remove(self.operator, keys)
	}

	fn state_batch(&mut self, keys: Vec<GroupStateKey>) -> Result<StateBatch, ValueError> {
		self.txn.state_batch(self.operator, keys)
	}

	fn state_set(&mut self, rows: Vec<(GroupStateKey, EncodedPodRow)>) -> Result<(), ValueError> {
		let groups: BTreeSet<GroupId> = rows.iter().filter_map(|(key, _)| managed_group(key)).collect();
		self.txn.state_set(self.operator, rows)?;
		for group in groups {
			self.arm_forget_pass(group)?;
		}
		Ok(())
	}

	fn state_write_batch(&mut self, batch: StateBatch) -> Result<(), ValueError> {
		let groups: BTreeSet<GroupId> = (0..batch.len())
			.filter(|&slot| matches!(batch.op(slot), Some(PendingWrite::Set(_))))
			.filter_map(|slot| managed_group(batch.key(slot)))
			.collect();
		self.txn.state_write_batch(self.operator, batch)?;
		for group in groups {
			self.arm_forget_pass(group)?;
		}
		Ok(())
	}

	fn state_page_inner(
		&mut self,
		range: GroupStateRange,
		limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>, ValueError> {
		let batch = self.txn.state_range(
			self.operator,
			StateRange {
				range,
				limit,
				page: None,
				site: "operator::host_page",
			},
		)?;
		let mut out = Vec::with_capacity(batch.items.len());
		for r in batch.items {
			out.push((framed_state_key(self.operator, &r.key)?, EncodedPodRow::from(r.bytes)));
		}
		Ok(out)
	}

	fn group_sweep(
		&mut self,
		groups: &[GroupId],
		limit: usize,
		keyspaces: KeyspaceMask,
	) -> Result<GroupSweep, ValueError> {
		let batch = self.txn.state_group_range(self.operator, groups, limit, keyspaces)?;
		let mut rows = Vec::with_capacity(batch.items.len());
		for r in batch.items {
			rows.push((framed_state_key(self.operator, &r.key)?, EncodedPodRow::from(r.bytes)));
		}
		Ok(GroupSweep {
			rows,
			complete: !batch.has_more,
		})
	}

	fn state_last(&mut self, range: GroupStateRange) -> Result<Option<(GroupStateKey, EncodedPodRow)>, ValueError> {
		let Some(r) = self.txn.state_last(self.operator, range)? else {
			return Ok(None);
		};
		Ok(Some((framed_state_key(self.operator, &r.key)?, EncodedPodRow::from(r.bytes))))
	}

	fn get_or_create_row_numbers(
		&mut self,
		group: GroupId,
		keys: &[EncodedKey],
	) -> Result<Vec<(RowNumber, bool)>, ValueError> {
		self.txn.get_or_create_row_numbers(self.operator, group, keys)
	}

	fn get_or_create_row_numbers_for_groups(
		&mut self,
		groups: &[GroupId],
	) -> Result<Vec<(RowNumber, bool)>, ValueError> {
		self.txn.get_or_create_row_numbers_for_groups(self.operator, groups)
	}

	fn get_row_numbers(
		&mut self,
		group: GroupId,
		keys: &[EncodedKey],
	) -> Result<Vec<Option<RowNumber>>, ValueError> {
		self.txn.get_row_numbers(self.operator, group, keys)
	}

	fn remove_row_number(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<(), ValueError> {
		self.txn.remove_row_number(self.operator, group, keys)
	}

	fn remove_row_number_for_group(&mut self, groups: &[GroupId]) -> Result<(), ValueError> {
		self.txn.remove_row_number_for_group(self.operator, groups)
	}

	fn mint_row_numbers(&mut self, count: usize) -> Result<RowNumber, ValueError> {
		self.txn.mint_row_numbers(self.operator, count)
	}

	fn get_row_numbers_for_groups(&mut self, groups: &[GroupId]) -> Result<Vec<Option<RowNumber>>, ValueError> {
		self.txn.get_row_numbers_for_groups(self.operator, groups)
	}

	fn set_row_numbers_for_groups(&mut self, rows: &[(GroupId, RowNumber)]) -> Result<(), ValueError> {
		self.txn.set_row_numbers_for_groups(self.operator, rows)
	}

	fn batch_time(&self) -> DateTime {
		self.batch_time
	}
}

impl<T: FlowTransaction> IdentityForgetPass for TxnHostContext<'_, T> {
	fn forget_identities(
		&mut self,
		groups: &[GroupId],
		limit: usize,
	) -> Result<Vec<ForgetPassOutcome>, ValueError> {
		self.txn.forget_group_identities(self.operator, groups, limit)
	}
}

impl<T: FlowTransaction + Intern> HostContext for TxnHostContext<'_, T> {
	fn version(&self) -> CommitVersion {
		self.txn.version()
	}

	fn snapshot_version(&self) -> CommitVersion {
		self.txn.snapshot_version()
	}

	fn now(&self) -> DateTime {
		self.txn.clock().now()
	}

	fn lookup_read(
		&mut self,
		storage: StorageId,
		version: CommitVersion,
		partitions: &[Partition],
	) -> Result<Vec<Option<(RowNumber, EncodedBytes)>>, ValueError> {
		if let Some(version_floor) = self.txn.lookup_floor()
			&& version < version_floor
		{
			panic!("lookup of storage {storage:?} must read at version {version:?}, below the held lease \
				 floor {version_floor:?}: that version may be collected, so the read could return a different row");
		}
		let ctx = self.txn.context().clone();
		let mut query = self.txn.query();
		query.read_as_of_version_inclusive(version);
		partitions
			.iter()
			.map(|&partition| {
				let key = |row: u64| StoragePartitionedRowKey::new(partition, RowNumber(row));
				query.range_partitioned_row(
					&ctx,
					storage,
					Bound::Included(key(u64::MAX)),
					Bound::Included(key(0)),
					RangeScope::All,
					1,
				)
				.next()
				.map(|row| row.map(|row| (row.key.row.0, row.bytes)))
				.transpose()
			})
			.collect()
	}

	fn lookup_view_read(&self, view: ViewId, source: SourceVersion) -> LookupAt {
		let Some(version_floor) = self.txn.lookup_floor() else {
			return LookupAt::Commit(CommitVersion(source.0).min(self.version()));
		};
		let read = self.txn.lookup_view_read(view, source);
		let commit = read.map(LookupAt::commit);
		if version_floor.0 > source.0
			&& commit.is_none_or(|commit| {
				self.txn.lookup_view_commit_through(view, version_floor) != Some(commit)
			}) {
			panic!(
				"lookup of view {view:?} for source {source:?} sits below the held lease floor {version_floor:?} and its \
				 producer committed after {commit:?}: the producer commit it needs may be collected"
			);
		}
		let Some(read) = read else {
			panic!(
				"lookup of view {view:?} found no completion of its producer flow with a source at or below \
				 {source:?}: the tracker history no longer covers it"
			);
		};
		match read {
			LookupAt::Commit(commit) => LookupAt::Commit(commit.max(version_floor)),
			LookupAt::Layered {
				..
			} => read,
		}
	}

	fn lookup_read_at(
		&mut self,
		storage: StorageId,
		at: LookupAt,
		partitions: &[Partition],
	) -> Result<Vec<Option<(RowNumber, EncodedBytes)>>, ValueError> {
		let LookupAt::Layered {
			view,
			base,
			commit,
			through,
		} = at
		else {
			return self.lookup_read(storage, at.commit(), partitions);
		};
		let Some(layers) = self.txn.lookup_view_layers(view, commit) else {
			panic!(
				"lookup of view {view:?} reads inside commit {commit:?}, but the tracker holds no layers for it"
			);
		};
		let mut written: HashMap<Partition, BTreeMap<RowNumber, Option<EncodedBytes>>> = HashMap::new();
		for (key, row) in layers.rows_through(through) {
			if key.storage != storage || !partitions.contains(&key.partition) {
				continue;
			}
			written.entry(key.partition).or_default().insert(key.row, row.clone());
		}
		if let Some(version_floor) = self.txn.lookup_floor()
			&& base < version_floor
		{
			panic!(
				"lookup of view {view:?} must read below commit {commit:?} at {base:?}, below the held lease \
				 floor {version_floor:?}: that version may be collected, so the read could return a different row"
			);
		}
		let ctx = self.txn.context().clone();
		let mut query = self.txn.query();
		query.read_as_of_version_inclusive(base);
		partitions
			.iter()
			.map(|&partition| {
				let layered = written.get(&partition);
				let skip = layered.map_or(0, BTreeMap::len);
				let key = |row: u64| StoragePartitionedRowKey::new(partition, RowNumber(row));
				let mut newest_base = None;
				for row in query.range_partitioned_row(
					&ctx,
					storage,
					Bound::Included(key(u64::MAX)),
					Bound::Included(key(0)),
					RangeScope::All,
					skip + 1,
				) {
					let row = row?;
					if layered.is_some_and(|layered| layered.contains_key(&row.key.row.0)) {
						continue;
					}
					newest_base = Some((row.key.row.0, row.bytes));
					break;
				}
				let newest_layered = layered.and_then(|layered| {
					layered.iter()
						.rev()
						.find_map(|(row, bytes)| bytes.clone().map(|bytes| (*row, bytes)))
				});
				Ok(match (newest_layered, newest_base) {
					(Some(layered), Some(base)) => Some(if layered.0 >= base.0 {
						layered
					} else {
						base
					}),
					(layered, base) => layered.or(base),
				})
			})
			.collect()
	}

	fn lookup_floor(&self) -> Option<CommitVersion> {
		self.txn.lookup_floor()
	}

	fn row_shape_cache(&mut self) -> &mut HashMap<EncodedKey, RowShape> {
		self.txn.row_shape_cache(self.operator)
	}

	fn disarm_timer_by_key(&mut self, kind: TimerKind, key: &EncodedKey) -> Result<(), ValueError> {
		self.txn.disarm_timer_by_key(self.operator, kind, key)
	}

	fn join_forget_arm(&mut self, side: u8, rows: &[(GroupId, RowNumber, DateTime)]) -> Result<(), ValueError> {
		self.txn.join_forget_arm(self.operator, side, rows)
	}

	fn join_forget_clear(
		&mut self,
		side: u8,
		rows: &[(GroupId, RowNumber)],
	) -> Result<Vec<Option<DateTime>>, ValueError> {
		self.txn.join_forget_clear(self.operator, side, rows)
	}

	fn join_forget_free(&mut self, entries: &[JoinDueEntry]) -> Result<(), ValueError> {
		self.txn.join_forget_free(self.operator, entries)
	}

	fn join_forget_min(&mut self) -> Result<Option<DateTime>, ValueError> {
		self.txn.join_forget_min(self.operator)
	}

	fn join_due_page(&mut self, at: DateTime, budget: usize, start: &DueStart) -> Result<JoinDuePage, ValueError> {
		self.txn.join_due_page(self.operator, at, budget, start)
	}

	fn config_uint8(&self, key: ConfigKey) -> u64 {
		self.txn.catalog().get_config_uint8(key)
	}

	fn state_range(&mut self, range: GroupStateRange) -> Result<Vec<(GroupStateKey, EncodedPodRow)>, ValueError> {
		self.state_range_limited(range, None)
	}

	fn state_range_limited(
		&mut self,
		range: GroupStateRange,
		limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>, ValueError> {
		let mut out = Vec::new();
		self.state_range_limited_visit(range, limit, &mut |key, row| {
			out.push((key, row));
			Ok(())
		})?;
		Ok(out)
	}

	fn state_range_limited_visit(
		&mut self,
		range: GroupStateRange,
		limit: Option<usize>,
		visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<(), ValueError>,
	) -> Result<(), ValueError> {
		let site = range_site(&range);
		let mut query = StateRange::forward(range, site);
		query.limit = limit;
		let batch = self.txn.state_range(self.operator, query)?;
		for r in batch.items {
			visit(framed_state_key(self.operator, &r.key)?, EncodedPodRow::from(r.bytes))?;
		}
		Ok(())
	}

	fn forget_group_identities(
		&mut self,
		groups: &[GroupId],
		limit: usize,
	) -> Result<Vec<ForgetPassOutcome>, ValueError> {
		self.txn.forget_group_identities(self.operator, groups, limit)
	}

	fn get_join_row_numbers(&mut self, keys: &[JoinRowMappingKey]) -> Result<Vec<Option<RowNumber>>, ValueError> {
		self.txn.get_join_row_numbers(self.operator, keys)
	}

	fn get_or_create_join_row_numbers(
		&mut self,
		keys: &[JoinRowMappingKey],
	) -> Result<Vec<(RowNumber, bool)>, ValueError> {
		self.txn.get_or_create_join_row_numbers(self.operator, keys)
	}

	fn remove_join_row_numbers(&mut self, keys: &[JoinRowMappingKey]) -> Result<(), ValueError> {
		self.txn.remove_join_row_numbers(self.operator, keys)
	}

	fn remove_join_row_numbers_for_left(&mut self, tag: u8, left: u64) -> Result<(), ValueError> {
		self.txn.remove_join_row_numbers_for_left(self.operator, tag, left)
	}

	fn dictionary_id_type(&mut self, dictionary: DictionaryId) -> Option<ValueType> {
		self.txn.find_dictionary(dictionary).map(|d| d.id_type)
	}

	fn dictionary_find(
		&mut self,
		dictionary: DictionaryId,
		values: &[Value],
	) -> Result<Vec<Option<DictionaryEntryId>>, ValueError> {
		match self.txn.find_dictionary(dictionary) {
			Some(dict) => self.txn.find_in_dictionary(&dict, values),
			None => Ok(vec![None; values.len()]),
		}
	}

	fn dictionary_get(
		&mut self,
		dictionary: DictionaryId,
		ids: &[DictionaryEntryId],
	) -> Result<Vec<Option<Value>>, ValueError> {
		match self.txn.find_dictionary(dictionary) {
			Some(dict) => self.txn.get_from_dictionary(&dict, ids),
			None => Ok(vec![None; ids.len()]),
		}
	}

	fn dictionary_intern(
		&mut self,
		dictionary: &Dictionary,
		values: &[Value],
	) -> Result<Vec<DictionaryEntryId>, ValueError> {
		self.txn.intern(dictionary, values)
	}
}

impl<T: FlowTransaction> Resolve for TxnHostContext<'_, T> {
	fn dictionary(&mut self, id: DictionaryId) -> Result<Option<Dictionary>, ValueError> {
		Ok(self.txn.find_dictionary(id))
	}

	fn resolve(
		&mut self,
		dictionary: &Dictionary,
		ids: &[DictionaryEntryId],
	) -> Result<Vec<Option<Value>>, ValueError> {
		#[cfg(test)]
		{
			self.dictionary_resolves += 1;
		}
		self.txn.get_from_dictionary(dictionary, ids)
	}
}

pub(crate) struct HostFind<'a>(pub(crate) &'a mut dyn HostContext);

impl Resolve for HostFind<'_> {
	fn dictionary(&mut self, id: DictionaryId) -> Result<Option<Dictionary>, ValueError> {
		self.0.dictionary(id)
	}

	fn resolve(
		&mut self,
		dictionary: &Dictionary,
		ids: &[DictionaryEntryId],
	) -> Result<Vec<Option<Value>>, ValueError> {
		self.0.resolve(dictionary, ids)
	}
}

impl Find for HostFind<'_> {
	fn find(
		&mut self,
		dictionary: &Dictionary,
		values: &[Value],
	) -> Result<Vec<Option<DictionaryEntryId>>, ValueError> {
		self.0.dictionary_find(dictionary.id, values)
	}
}

fn framed_state_key(operator: OperatorId, key: &TaggedKey) -> Result<GroupStateKey, ValueError> {
	if let TaggedKey::OperatorState(decoded) = key
		&& let Some(inner) = GroupStateKey::from_framed(decoded.inner())
	{
		return Ok(inner);
	}
	Err(FlowAsyncError::StateKeyUnframed {
		operator,
		key: key.encode(),
	}
	.into())
}

fn range_site(range: &GroupStateRange) -> &'static str {
	match range {
		GroupStateRange::Keyspace {
			keyspace,
			..
		} => match keyspace.name() {
			Cow::Borrowed(name) => name,
			Cow::Owned(_) => "operator::host_range",
		},
		GroupStateRange::Group(_) | GroupStateRange::GroupData(_) | GroupStateRange::GroupIdentity(_) => {
			"operator::host_sweep"
		}
	}
}
