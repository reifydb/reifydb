// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	borrow::Cow,
	collections::{BTreeSet, HashMap},
	ops::Bound,
};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
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
			state::{
				GroupId, GroupStateKey, KeyspaceId, KeyspaceMask, OperatorStateKey,
				group_inner_range_split, node_prefix,
			},
		},
		row::StoragePartitionedRowKey,
	},
	state::{
		batch::StateBatch,
		timer::{GroupSweep, StateStore, TimerKind, TimerStore},
	},
};
use reifydb_flow::txn::{Find, Intern, Resolve};
use reifydb_transaction::multi::RangeScope;
use reifydb_value::{
	Result,
	byte_size::ByteSize,
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
	operator::state::{
		expiry::{expiry_drop, expiry_get, expiry_set, managed_due_key, managed_latest_key},
		iter::StateIterator,
		reaper::IdentityReclaim,
		reclaim::ReclaimOutcome,
		seal::rule::SEAL_GATE_STEP,
	},
	timer::{Timer, TimerMove, extension::TimerExtension},
	transaction::{
		FlowTransaction,
		dictionary::DictionaryExtension,
		join_expiry::{DueStart, JoinDueEntry, JoinDuePage, JoinRowExpiryExtension},
		reclaim::ReclaimExtension,
		row_number::RowNumberExtension,
		state::{StateExtension, StateRange},
	},
};

pub trait HostContext: StateStore + TimerStore + IdentityReclaim + Resolve {
	fn version(&self) -> CommitVersion;

	fn disarm_timer_by_key(&mut self, kind: TimerKind, key: &EncodedKey) -> Result<()>;

	fn rearm_timers(&mut self, kind: TimerKind, moves: &[TimerMove]) -> Result<()>;

	fn join_expiry_arm_many(&mut self, side: u8, rows: &[(GroupId, RowNumber, DateTime)]) -> Result<()>;

	fn join_expiry_clear_many(&mut self, side: u8, rows: &[(GroupId, RowNumber)]) -> Result<Vec<Option<DateTime>>>;

	fn join_expiry_free_many(&mut self, entries: &[JoinDueEntry]) -> Result<()>;

	fn join_expiry_min(&mut self) -> Result<Option<DateTime>>;

	fn join_due_page(&mut self, at: DateTime, budget: usize, start: &DueStart) -> Result<JoinDuePage>;

	fn config_uint8(&self, key: ConfigKey) -> u64;

	fn row_shape_cache(&mut self) -> &mut HashMap<EncodedKey, RowShape>;

	fn state_range(&mut self, range: EncodedKeyRange) -> Result<Vec<(GroupStateKey, EncodedPodRow)>>;

	fn state_range_limited(
		&mut self,
		range: EncodedKeyRange,
		limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>>;

	fn state_range_limited_visit(
		&mut self,
		range: EncodedKeyRange,
		limit: Option<usize>,
		visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<()>,
	) -> Result<()> {
		for (key, row) in self.state_range_limited(range, limit)? {
			visit(key, row)?;
		}
		Ok(())
	}

	fn state_range_iter(&mut self, range: EncodedKeyRange) -> StateIterator<'_>;

	fn reclaim_group_identity(&mut self, group: GroupId, limit: usize) -> Result<ReclaimOutcome>;

	fn get_row_numbers_for_groups(&mut self, groups: &[GroupId]) -> Result<Vec<Option<RowNumber>>>;

	fn get_join_row_numbers(&mut self, keys: &[JoinRowMappingKey]) -> Result<Vec<Option<RowNumber>>>;

	fn get_or_create_join_row_numbers(&mut self, keys: &[JoinRowMappingKey]) -> Result<Vec<(RowNumber, bool)>>;

	fn remove_join_row_numbers(&mut self, keys: &[JoinRowMappingKey]) -> Result<()>;

	fn remove_join_row_numbers_for_left(&mut self, tag: u8, left: u64) -> Result<()>;

	fn dictionary_id_type(&mut self, dictionary: DictionaryId) -> Option<ValueType>;

	fn dictionary_find(&mut self, dictionary: DictionaryId, value: &Value) -> Result<Option<DictionaryEntryId>>;

	fn dictionary_find_many(
		&mut self,
		dictionary: DictionaryId,
		values: &[Value],
	) -> Result<Vec<Option<DictionaryEntryId>>>;

	fn dictionary_get_many(
		&mut self,
		dictionary: DictionaryId,
		ids: &[DictionaryEntryId],
	) -> Result<Vec<Option<Value>>>;

	fn dictionary_intern_many(
		&mut self,
		dictionary: &Dictionary,
		values: &[Value],
	) -> Result<Vec<DictionaryEntryId>>;

	fn lookup_read_many(
		&mut self,
		storage: StorageId,
		version: CommitVersion,
		partitions: &[Partition],
	) -> Result<Vec<Option<(RowNumber, EncodedBytes)>>>;

	fn lookup_view_version(&self, view: ViewId, source: SourceVersion) -> CommitVersion;

	fn lookup_floor(&self) -> Option<CommitVersion>;
}

pub struct TxnHostContext<'a, T: FlowTransaction> {
	txn: &'a mut T,
	operator: OperatorId,
	now: DateTime,
	retention: Option<Duration>,
	#[cfg(test)]
	pub dictionary_resolves: usize,
}

impl<'a, T: FlowTransaction> TxnHostContext<'a, T> {
	pub fn new(txn: &'a mut T, operator: OperatorId) -> Self {
		Self::with_retention(txn, operator, None)
	}

	pub fn with_retention(txn: &'a mut T, operator: OperatorId, retention: Option<Duration>) -> Self {
		let now = txn.written_at();
		Self {
			txn,
			operator,
			now,
			retention,
			#[cfg(test)]
			dictionary_resolves: 0,
		}
	}

	fn arm_reclaim(&mut self, group: GroupId) -> Result<()> {
		let Some(retention) = self.retention else {
			return internal_err!("operator {} wrote managed state but has no retention", self.operator);
		};
		let latest = managed_latest_key(group);
		let previous = expiry_get::<DateTime>(self, &latest)?;
		let due = reclaim_due(self.now, retention);
		let due = previous.map_or(due, |previous| previous.max(due));
		if let Some(previous) = previous.filter(|previous| *previous != due) {
			expiry_drop(self, &managed_due_key(previous, group))?;
		}
		expiry_set(self, managed_due_key(due, group), Vec::<u8>::new())?;
		expiry_set(self, latest, due)?;
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

fn reclaim_due(written_at: DateTime, retention: Duration) -> DateTime {
	let free = written_at.saturating_add(retention).saturating_add(SEAL_GATE_STEP);
	let nanos = free.to_nanos();
	let ceil = nanos.div_euclid(NANOS_PER_SECOND) + i64::from(nanos.rem_euclid(NANOS_PER_SECOND) != 0);
	DateTime::from_nanos(ceil.saturating_mul(NANOS_PER_SECOND))
}

impl<T: FlowTransaction> TimerStore for TxnHostContext<'_, T> {
	fn arm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		self.txn.arm_timer(
			self.operator,
			&Timer {
				due,
				kind,
				key: key.clone(),
			},
		)
	}

	fn disarm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		self.txn.disarm_timer(
			self.operator,
			&Timer {
				due,
				kind,
				key: key.clone(),
			},
		)
	}

	fn flow_watermark(&mut self) -> Result<Option<DateTime>> {
		Ok(self.txn.flow_watermark())
	}
}

impl<T: FlowTransaction> StateStore for TxnHostContext<'_, T> {
	fn state_get(&mut self, key: &GroupStateKey) -> Result<Option<EncodedPodRow>> {
		self.txn.state_get(self.operator, key)
	}

	fn state_get_many(&mut self, keys: &[GroupStateKey]) -> Result<Vec<Option<EncodedPodRow>>> {
		self.txn.state_get_many(self.operator, keys)
	}

	fn state_classify(&mut self, key: &GroupStateKey, pre: Option<ByteSize>) {
		self.txn.state_classify(self.operator, key, pre);
	}

	fn state_set(&mut self, key: &GroupStateKey, payload: EncodedPodRow) -> Result<()> {
		self.txn.state_set(self.operator, key, payload)?;
		managed_group(key).map_or(Ok(()), |group| self.arm_reclaim(group))
	}

	fn state_remove(&mut self, key: &GroupStateKey) -> Result<()> {
		self.txn.state_remove(self.operator, key)
	}

	fn state_remove_many(&mut self, keys: &[GroupStateKey]) -> Result<()> {
		self.txn.state_remove_many(self.operator, keys)
	}

	fn state_batch(&mut self, keys: Vec<GroupStateKey>) -> Result<StateBatch> {
		self.txn.state_batch(self.operator, keys)
	}

	fn state_set_many(&mut self, rows: Vec<(GroupStateKey, EncodedPodRow)>) -> Result<()> {
		let groups: BTreeSet<GroupId> = rows.iter().filter_map(|(key, _)| managed_group(key)).collect();
		self.txn.state_set_many(self.operator, rows)?;
		for group in groups {
			self.arm_reclaim(group)?;
		}
		Ok(())
	}

	fn state_write_batch(&mut self, batch: StateBatch) -> Result<()> {
		let groups: BTreeSet<GroupId> = (0..batch.len())
			.filter(|&slot| matches!(batch.op(slot), Some(PendingWrite::Set(_))))
			.filter_map(|slot| managed_group(batch.key(slot)))
			.collect();
		self.txn.state_write_batch(self.operator, batch)?;
		for group in groups {
			self.arm_reclaim(group)?;
		}
		Ok(())
	}

	fn state_page_inner(
		&mut self,
		range: EncodedKeyRange,
		limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
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
			if let TaggedKey::OperatorState(decoded) = &r.key
				&& let Some(inner) = GroupStateKey::from_framed(decoded.inner())
			{
				out.push((inner, EncodedPodRow::from(r.bytes)));
			}
		}
		Ok(out)
	}

	fn group_sweep(
		&mut self,
		group: GroupId,
		keyspaces: KeyspaceMask,
		limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
		Ok(self.group_sweep_many(&[group], limit.unwrap_or(usize::MAX), keyspaces)?.rows)
	}

	fn group_sweep_many(
		&mut self,
		groups: &[GroupId],
		limit: usize,
		keyspaces: KeyspaceMask,
	) -> Result<GroupSweep> {
		let batch = self.txn.state_group_range(self.operator, groups, limit, keyspaces)?;
		let mut rows = Vec::with_capacity(batch.items.len());
		for r in batch.items {
			if let TaggedKey::OperatorState(decoded) = &r.key
				&& let Some(inner) = GroupStateKey::from_framed(decoded.inner())
			{
				rows.push((inner, EncodedPodRow::from(r.bytes)));
			}
		}
		Ok(GroupSweep {
			rows,
			complete: !batch.has_more,
		})
	}

	fn state_last(&mut self, range: EncodedKeyRange) -> Result<Option<(GroupStateKey, EncodedPodRow)>> {
		let Some(r) = self.txn.state_last(self.operator, range)? else {
			return Ok(None);
		};
		if let TaggedKey::OperatorState(decoded) = &r.key
			&& let Some(inner) = GroupStateKey::from_framed(decoded.inner())
		{
			return Ok(Some((inner, EncodedPodRow::from(r.bytes))));
		}
		Ok(None)
	}

	fn get_or_create_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>> {
		self.txn.get_or_create_row_numbers(self.operator, group, keys)
	}

	fn get_or_create_row_numbers_for_groups(&mut self, groups: &[GroupId]) -> Result<Vec<(RowNumber, bool)>> {
		self.txn.get_or_create_row_numbers_for_groups(self.operator, groups)
	}

	fn get_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<Option<RowNumber>>> {
		self.txn.get_row_numbers(self.operator, group, keys)
	}

	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()> {
		self.txn.remove_row_number(self.operator, group, key)
	}

	fn remove_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<()> {
		self.txn.remove_row_numbers(self.operator, group, keys)
	}

	fn remove_row_number_for_group(&mut self, group: GroupId) -> Result<()> {
		self.txn.remove_row_number_for_group(self.operator, group)
	}

	fn remove_row_numbers_for_groups(&mut self, groups: &[GroupId]) -> Result<()> {
		self.txn.remove_row_numbers_for_groups(self.operator, groups)
	}

	fn written_at(&self) -> DateTime {
		self.now
	}
}

impl<T: FlowTransaction> IdentityReclaim for TxnHostContext<'_, T> {
	fn reclaim_identity(&mut self, group: GroupId, limit: usize) -> Result<ReclaimOutcome> {
		self.txn.reclaim_group_identity(self.operator, group, limit)
	}
}

impl<T: FlowTransaction + Intern> HostContext for TxnHostContext<'_, T> {
	fn version(&self) -> CommitVersion {
		self.txn.version()
	}

	fn lookup_read_many(
		&mut self,
		storage: StorageId,
		version: CommitVersion,
		partitions: &[Partition],
	) -> Result<Vec<Option<(RowNumber, EncodedBytes)>>> {
		if let Some(floor) = self.txn.lookup_floor()
			&& version < floor
		{
			panic!("lookup of storage {storage:?} must read at version {version:?}, below the held lease \
				 floor {floor:?}: that version may be collected, so the read could return a different row");
		}
		let mut query = self.txn.query();
		query.read_as_of_version_inclusive(version);
		partitions
			.iter()
			.map(|&partition| {
				let key = |row: u64| StoragePartitionedRowKey::new(partition, RowNumber(row));
				query.range_partitioned_row(
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

	fn lookup_view_version(&self, view: ViewId, source: SourceVersion) -> CommitVersion {
		let commit = self.txn.lookup_view_version(view, source);
		let floor = self.txn.lookup_floor();
		if let Some(floor) = floor
			&& floor.0 > source.0 && commit
			.is_none_or(|commit| self.txn.lookup_view_commit_through(view, floor) != Some(commit))
		{
			panic!(
				"lookup of view {view:?} for source {source:?} sits below the held lease floor {floor:?} and its \
				 producer committed after {commit:?}: the producer commit it needs may be collected"
			);
		}
		let Some(commit) = commit else {
			panic!(
				"lookup of view {view:?} found no completion of its producer flow with a source at or below \
				 {source:?}: the tracker history no longer covers it"
			);
		};
		floor.map_or(commit, |floor| commit.max(floor))
	}

	fn lookup_floor(&self) -> Option<CommitVersion> {
		self.txn.lookup_floor()
	}

	fn row_shape_cache(&mut self) -> &mut HashMap<EncodedKey, RowShape> {
		self.txn.row_shape_cache(self.operator)
	}

	fn disarm_timer_by_key(&mut self, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		self.txn.disarm_timer_by_key(self.operator, kind, key)
	}

	fn rearm_timers(&mut self, kind: TimerKind, moves: &[TimerMove]) -> Result<()> {
		self.txn.rearm_timers(self.operator, kind, moves)
	}

	fn join_expiry_arm_many(&mut self, side: u8, rows: &[(GroupId, RowNumber, DateTime)]) -> Result<()> {
		self.txn.join_expiry_arm_many(self.operator, side, rows)
	}

	fn join_expiry_clear_many(&mut self, side: u8, rows: &[(GroupId, RowNumber)]) -> Result<Vec<Option<DateTime>>> {
		self.txn.join_expiry_clear_many(self.operator, side, rows)
	}

	fn join_expiry_free_many(&mut self, entries: &[JoinDueEntry]) -> Result<()> {
		self.txn.join_expiry_free_many(self.operator, entries)
	}

	fn join_expiry_min(&mut self) -> Result<Option<DateTime>> {
		self.txn.join_expiry_min(self.operator)
	}

	fn join_due_page(&mut self, at: DateTime, budget: usize, start: &DueStart) -> Result<JoinDuePage> {
		self.txn.join_due_page(self.operator, at, budget, start)
	}

	fn config_uint8(&self, key: ConfigKey) -> u64 {
		self.txn.catalog().get_config_uint8(key)
	}

	fn state_range(&mut self, range: EncodedKeyRange) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
		self.state_range_limited(range, None)
	}

	fn state_range_limited(
		&mut self,
		range: EncodedKeyRange,
		limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
		let mut out = Vec::new();
		self.state_range_limited_visit(range, limit, &mut |key, row| {
			out.push((key, row));
			Ok(())
		})?;
		Ok(out)
	}

	fn state_range_limited_visit(
		&mut self,
		range: EncodedKeyRange,
		limit: Option<usize>,
		visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<()>,
	) -> Result<()> {
		let site = range_site(&range);
		let mut query = StateRange::forward(range, site);
		query.limit = limit;
		let batch = self.txn.state_range(self.operator, query)?;
		for r in batch.items {
			let Some(key) = unscope(&r.key) else {
				continue;
			};
			visit(key, EncodedPodRow::from(r.bytes))?;
		}
		Ok(())
	}

	fn state_range_iter(&mut self, range: EncodedKeyRange) -> StateIterator<'_> {
		let prefixed = range.with_prefix(EncodedKey::new(node_prefix(self.operator)));
		StateIterator::new(self.txn.range(prefixed, RangeScope::All, 1024))
	}

	fn reclaim_group_identity(&mut self, group: GroupId, limit: usize) -> Result<ReclaimOutcome> {
		self.txn.reclaim_group_identity(self.operator, group, limit)
	}

	fn get_row_numbers_for_groups(&mut self, groups: &[GroupId]) -> Result<Vec<Option<RowNumber>>> {
		self.txn.get_row_numbers_for_groups(self.operator, groups)
	}

	fn get_join_row_numbers(&mut self, keys: &[JoinRowMappingKey]) -> Result<Vec<Option<RowNumber>>> {
		self.txn.get_join_row_numbers(self.operator, keys)
	}

	fn get_or_create_join_row_numbers(&mut self, keys: &[JoinRowMappingKey]) -> Result<Vec<(RowNumber, bool)>> {
		self.txn.get_or_create_join_row_numbers(self.operator, keys)
	}

	fn remove_join_row_numbers(&mut self, keys: &[JoinRowMappingKey]) -> Result<()> {
		self.txn.remove_join_row_numbers(self.operator, keys)
	}

	fn remove_join_row_numbers_for_left(&mut self, tag: u8, left: u64) -> Result<()> {
		self.txn.remove_join_row_numbers_for_left(self.operator, tag, left)
	}

	fn dictionary_id_type(&mut self, dictionary: DictionaryId) -> Option<ValueType> {
		self.txn.find_dictionary(dictionary).map(|d| d.id_type)
	}

	fn dictionary_find(&mut self, dictionary: DictionaryId, value: &Value) -> Result<Option<DictionaryEntryId>> {
		match self.txn.find_dictionary(dictionary) {
			Some(dict) => self.txn.find_in_dictionary(&dict, value),
			None => Ok(None),
		}
	}

	fn dictionary_find_many(
		&mut self,
		dictionary: DictionaryId,
		values: &[Value],
	) -> Result<Vec<Option<DictionaryEntryId>>> {
		match self.txn.find_dictionary(dictionary) {
			Some(dict) => self.txn.find_many_in_dictionary(&dict, values),
			None => Ok(vec![None; values.len()]),
		}
	}

	fn dictionary_get_many(
		&mut self,
		dictionary: DictionaryId,
		ids: &[DictionaryEntryId],
	) -> Result<Vec<Option<Value>>> {
		match self.txn.find_dictionary(dictionary) {
			Some(dict) => self.txn.get_many_from_dictionary(&dict, ids),
			None => Ok(vec![None; ids.len()]),
		}
	}

	fn dictionary_intern_many(
		&mut self,
		dictionary: &Dictionary,
		values: &[Value],
	) -> Result<Vec<DictionaryEntryId>> {
		self.txn.intern(dictionary, values)
	}
}

impl<T: FlowTransaction> Resolve for TxnHostContext<'_, T> {
	fn dictionary(&mut self, id: DictionaryId) -> Result<Option<Dictionary>> {
		Ok(self.txn.find_dictionary(id))
	}

	fn resolve(&mut self, dictionary: &Dictionary, ids: &[DictionaryEntryId]) -> Result<Vec<Option<Value>>> {
		#[cfg(test)]
		{
			self.dictionary_resolves += 1;
		}
		self.txn.get_many_from_dictionary(dictionary, ids)
	}
}

pub(crate) struct HostFind<'a>(pub(crate) &'a mut dyn HostContext);

impl Resolve for HostFind<'_> {
	fn dictionary(&mut self, id: DictionaryId) -> Result<Option<Dictionary>> {
		self.0.dictionary(id)
	}

	fn resolve(&mut self, dictionary: &Dictionary, ids: &[DictionaryEntryId]) -> Result<Vec<Option<Value>>> {
		self.0.resolve(dictionary, ids)
	}
}

impl Find for HostFind<'_> {
	fn find(&mut self, dictionary: &Dictionary, values: &[Value]) -> Result<Vec<Option<DictionaryEntryId>>> {
		self.0.dictionary_find_many(dictionary.id, values)
	}
}

fn unscope(key: &TaggedKey) -> Option<GroupStateKey> {
	let TaggedKey::OperatorState(key) = key else {
		return None;
	};
	GroupStateKey::from_framed(key.inner())
}

fn range_site(range: &EncodedKeyRange) -> &'static str {
	if group_inner_range_split(range).is_some() {
		return "operator::host_sweep";
	}
	let key = match &range.start {
		Bound::Included(key) | Bound::Excluded(key) => key,
		Bound::Unbounded => return "operator::host_range",
	};
	match OperatorStateKey::decode_inner(key.as_slice()).map(|(_, keyspace, _)| keyspace.name()) {
		Some(Cow::Borrowed(name)) => name,
		_ => "operator::host_range",
	}
}
