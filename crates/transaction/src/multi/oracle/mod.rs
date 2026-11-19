// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(not(reifydb_single_threaded))]
use std::thread::JoinHandle;
use std::{
	collections::{BTreeMap, HashMap, HashSet, VecDeque},
	ops::Deref,
	sync::Arc,
};

use cleanup::cleanup_old_windows;
use commit_queue::CommitQueue;
#[cfg(not(reifydb_single_threaded))]
use commit_queue::spawn_sequencer;
use reifydb_core::{
	common::{CommitVersion, SourceVersion},
	delta::{Delta, SetPre},
	interface::{
		catalog::config::{ConfigKey, GetConfig},
		operator::OperatorSection,
		store::{MultiVersionCommit, MultiVersionGetPrevious},
	},
	key::any::TaggedKey,
	util::bloom::hash_item,
	wal::CommitRecord,
};
use reifydb_runtime::{
	actor::system::ActorSpawner,
	context::{
		RuntimeContext,
		clock::{Clock, Instant},
		rng::Rng,
		spawner::Spawner,
		transaction::TransactionContext,
	},
	sync::rwlock::RwLock,
	version_epoch::{EpochSeconds, VersionEpoch},
};
use reifydb_store_multi::MultiStore;
use reifydb_value::{error::ValueError, reifydb_assertions, util::cowvec::CowVec, value::datetime::DateTime};
use reifydb_wal::{lsn::Lsn, sync::WalMode};
use smallvec::SmallVec;
use tracing::{Span, field, instrument};

use crate::{
	error::TransactionError,
	multi::{
		conflict::{ConflictManager, PartitionMember},
		done_until::done_until::DoneUntil,
		lease::VersionLeases,
		transaction::version::VersionProvider,
	},
	wal::{CommitWal, check_record_size},
};

pub mod cleanup;
mod commit_queue;

#[derive(Clone, Copy)]
pub(crate) struct SpanTiming {
	recording: bool,
}

impl SpanTiming {
	#[inline]
	fn current() -> Self {
		Self {
			recording: !Span::current().is_disabled(),
		}
	}

	#[inline]
	fn mark(&self, clock: &Clock) -> Option<Instant> {
		self.recording.then(|| clock.instant())
	}

	#[inline]
	fn record_micros(&self, start: Option<Instant>, field: &'static str) {
		if let Some(start) = start {
			Span::current().record(field, start.elapsed().as_micros() as u64);
		}
	}
}

pub(crate) type OracleLock = RwLock<OracleState>;

const WINDOW_KEY_CAPACITY: usize = 500;

const WINDOW_KEY_CAPACITY_MAX: usize = 1 << 22;

pub(crate) struct ModifiedKeyIndex {
	hashes: HashSet<u64>,
}

impl ModifiedKeyIndex {
	fn with_capacity(capacity: usize) -> Self {
		Self {
			hashes: HashSet::with_capacity(capacity.clamp(WINDOW_KEY_CAPACITY, WINDOW_KEY_CAPACITY_MAX)),
		}
	}

	fn len(&self) -> usize {
		self.hashes.len()
	}

	#[inline]
	fn insert(&mut self, key: &TaggedKey) {
		self.hashes.insert(hash_item(key));
	}

	fn insert_member(&mut self, member: &PartitionMember) {
		self.hashes.insert(hash_item(member));
	}

	#[inline]
	fn might_contain_hash(&self, hash: u64) -> bool {
		self.hashes.contains(&hash)
	}

	#[cfg(test)]
	pub(crate) fn contains(&self, key: &TaggedKey) -> bool {
		self.hashes.contains(&hash_item(key))
	}

	#[cfg(test)]
	pub(crate) fn capacity(&self) -> usize {
		self.hashes.capacity()
	}
}

const _: fn() = || {
	fn sendable<T: Send>() {}
	sendable::<CommittedWindow>();
};

pub(crate) struct CommittedWindow {
	transactions: Vec<CommittedTxn>,
	modified_keys: ModifiedKeyIndex,
	max_version: CommitVersion,
}

impl CommittedWindow {
	fn new(min_version: CommitVersion, key_capacity: usize) -> Self {
		Self {
			transactions: Vec::with_capacity(200),
			modified_keys: ModifiedKeyIndex::with_capacity(key_capacity),
			max_version: min_version,
		}
	}

	fn add_transaction(&mut self, txn: CommittedTxn) {
		self.max_version = self.max_version.max(txn.version);

		if let Some(ref conflicts) = txn.conflict_manager {
			for key in conflicts.get_write_keys() {
				self.modified_keys.insert(key);
			}
			for member in conflicts.get_member_writes() {
				self.modified_keys.insert_member(member);
			}
		}

		self.transactions.push(txn);
	}

	fn might_have_key_hash(&self, hash: u64) -> bool {
		self.modified_keys.might_contain_hash(hash)
	}

	pub(super) fn max_version(&self) -> CommitVersion {
		self.max_version
	}
}

pub(crate) struct OracleState {
	pub time_windows: BTreeMap<CommitVersion, CommittedWindow>,

	pub evicted_up_through: CommitVersion,

	commits_stopped: bool,

	pub last_unchecked: CommitVersion,

	pub lsns: VecDeque<(CommitVersion, Lsn)>,
}

pub(crate) struct Prepared {
	pub(crate) snapshot_keys: Vec<TaggedKey>,
	pub(crate) buf: Option<Vec<u8>>,
}

#[derive(Debug)]
pub(crate) struct CommittedTxn {
	version: CommitVersion,
	conflict_manager: Option<ConflictManager>,
}

pub(crate) type CommitRebase = Box<dyn FnOnce(&mut CowVec<Delta>, CommitVersion) -> Result<(), ValueError> + Send>;

pub(crate) enum CreateCommitResult {
	Success {
		version: CommitVersion,
		lsn: Lsn,
		at: DateTime,
	},
	Conflict(ConflictManager),
	TooOld,
}

pub(crate) struct CommitShared<L>
where
	L: VersionProvider,
{
	pub(crate) versions: L,
	pub(crate) inner: OracleLock,
	pub(crate) query: DoneUntil,
	pub(crate) command: DoneUntil,
	store: MultiStore,
	clock: Clock,
	version_epoch: VersionEpoch,
	rng: Rng,
	config: Arc<dyn GetConfig>,
	queue: CommitQueue,
	pub(crate) wal: CommitWal,
}

pub(crate) struct Oracle<L>
where
	L: VersionProvider,
{
	shared: Arc<CommitShared<L>>,
	#[cfg(not(reifydb_single_threaded))]
	sequencer_join: Option<JoinHandle<()>>,
	pub(crate) leases: Arc<VersionLeases>,
	shutdown_signal: Arc<RwLock<bool>>,
	spawner: ActorSpawner,
	rng: Rng,
}

impl<L> Deref for Oracle<L>
where
	L: VersionProvider,
{
	type Target = CommitShared<L>;

	fn deref(&self) -> &CommitShared<L> {
		&self.shared
	}
}

impl<L> Oracle<L>
where
	L: VersionProvider,
{
	#[allow(clippy::too_many_arguments)]
	pub fn new(
		versions: L,
		spawner: ActorSpawner,
		store: MultiStore,
		clock: Clock,
		version_epoch: VersionEpoch,
		rng: Rng,
		config: Arc<dyn GetConfig>,
		wal: CommitWal,
		lsns: VecDeque<(CommitVersion, Lsn)>,
	) -> Self
	where
		L: 'static,
	{
		let shared = Arc::new(CommitShared {
			versions,
			inner: OracleLock::new(OracleState {
				time_windows: BTreeMap::new(),
				evicted_up_through: CommitVersion(0),
				commits_stopped: false,
				last_unchecked: CommitVersion(0),
				lsns,
			}),
			query: DoneUntil::with_advancer("txn-mark-query".into(), &spawner),
			command: DoneUntil::with_advancer("txn-mark-cmd".into(), &spawner),
			store,
			clock,
			version_epoch,
			rng: rng.clone(),
			config,
			queue: CommitQueue::new(),
			wal,
		});

		Self {
			#[cfg(not(reifydb_single_threaded))]
			sequencer_join: Some(spawn_sequencer(&shared)),
			shared,
			leases: VersionLeases::new(),
			shutdown_signal: Arc::new(RwLock::new(false)),
			spawner,
			rng,
		}
	}

	pub fn config(&self) -> Arc<dyn GetConfig> {
		self.config.clone()
	}

	pub(crate) fn shared(&self) -> &Arc<CommitShared<L>> {
		&self.shared
	}

	pub fn spawner(&self) -> ActorSpawner {
		self.spawner.clone()
	}

	pub fn clock(&self) -> &Clock {
		&self.clock
	}

	pub fn rng(&self) -> &Rng {
		&self.rng
	}

	pub fn window_count(&self) -> usize {
		self.inner.read().time_windows.len()
	}

	#[instrument(name = "transaction::oracle::new_commit", level = "debug", skip(self, conflicts, deltas, prepared), fields(
		%version,
		read_keys = field::Empty,
		write_keys = field::Empty,
		windows_checked = field::Empty,
		txns_checked = field::Empty,
		conflict_check_us = field::Empty,
		clock_next_us = field::Empty,
		add_txn_us = field::Empty,
		cleanup_us = field::Empty,
		has_conflict = field::Empty
	))]
	pub(crate) fn new_commit(
		&self,
		version: CommitVersion,
		conflicts: ConflictManager,
		deltas: CowVec<Delta>,
		prepared: Option<Prepared>,
	) -> Result<(CreateCommitResult, CowVec<Delta>), ValueError> {
		let window_size = self.shared.config.get_config_uint8(ConfigKey::OracleWindowSize);
		let lsn_step = self.shared.config.get_config_uint8(ConfigKey::OracleLsnStep);
		self.shared.commit(version, conflicts, deltas, None, prepared, window_size, lsn_step)
	}

	#[instrument(name = "transaction::oracle::new_commit_rebased", level = "debug", skip(self, conflicts, deltas, rebase, prepared), fields(
		%version,
		read_keys = field::Empty,
		write_keys = field::Empty,
		windows_checked = field::Empty,
		txns_checked = field::Empty,
		conflict_check_us = field::Empty,
		clock_next_us = field::Empty,
		add_txn_us = field::Empty,
		cleanup_us = field::Empty,
		has_conflict = field::Empty
	))]
	pub(crate) fn new_commit_rebased(
		&self,
		version: CommitVersion,
		conflicts: ConflictManager,
		deltas: CowVec<Delta>,
		rebase: CommitRebase,
		prepared: Option<Prepared>,
	) -> Result<(CreateCommitResult, CowVec<Delta>), ValueError> {
		let window_size = self.shared.config.get_config_uint8(ConfigKey::OracleWindowSize);
		let lsn_step = self.shared.config.get_config_uint8(ConfigKey::OracleLsnStep);
		self.shared.commit(version, conflicts, deltas, Some(rebase), prepared, window_size, lsn_step)
	}
	pub(crate) fn bootstrapping_completed(&self) {
		let mut inner = self.inner.write();
		inner.time_windows.clear();
		inner.last_unchecked = self.versions.current().expect("the current version is readable");
	}

	pub(crate) fn version(&self) -> Result<CommitVersion, ValueError> {
		self.versions.current()
	}

	pub(crate) fn stop_commits(&self) -> Result<CommitVersion, ValueError> {
		let mut state = self.inner.write();
		state.commits_stopped = true;
		self.versions.current()
	}

	pub fn stop(&mut self) {
		{
			let mut shutdown = self.shutdown_signal.write();
			*shutdown = true;
		}
		#[cfg(not(reifydb_single_threaded))]
		if let Some(handle) = self.sequencer_join.take() {
			self.shared.queue.stop_sequencer();
			let _ = handle.join();
		}
		self.query.drain();
		self.command.drain();
		{
			let mut inner = self.inner.write();
			inner.time_windows.clear();
			inner.last_unchecked = self.versions.current().expect("the current version is readable");
		}
	}

	pub(crate) fn done_query(&self, version: CommitVersion) {
		self.query.mark_finished(version);
	}

	pub(crate) fn done_commit(&self, version: CommitVersion) {
		self.command.mark_finished(version);
	}

	pub(crate) fn advance_unchecked(
		&self,
		mut deltas: CowVec<Delta>,
		rebase: Option<CommitRebase>,
	) -> Result<(CreateCommitResult, CowVec<Delta>), ValueError> {
		let lsn_step = self.config.get_config_uint8(ConfigKey::OracleLsnStep);
		let mut inner = self.inner.write();
		if let Some(rebase) = rebase {
			rebase(&mut deltas, self.versions.current()?)?;
		}
		let (version, lsn, at) =
			self.allocate_commit_version(&inner, SpanTiming::current(), &mut deltas, None, None, false)?;
		inner.note_lsn(version, lsn, lsn_step);
		inner.last_unchecked = version;
		drop(inner);

		Ok((
			CreateCommitResult::Success {
				version,
				lsn,
				at,
			},
			deltas,
		))
	}

	pub(crate) fn commit_deferred_flow(
		&self,
		mut deltas: CowVec<Delta>,
		encoded: Option<Vec<u8>>,
		source: Option<SourceVersion>,
	) -> Result<(CommitVersion, Lsn, DateTime, CowVec<Delta>), ValueError> {
		reifydb_assertions! {
			assert!(
				!deltas.iter().any(|delta| matches!(delta, Delta::Set { pre: SetPre::Unknown, .. })),
				"commit_deferred_flow got a set with an unresolved pre: its old value would be read under the commit lock while every user commit waits"
			);
			assert!(
				encoded.is_some() || self.wal.mode() == WalMode::Off,
				"commit_deferred_flow got no encoded record while the WAL is on: the record would be encoded under the commit lock while every user commit waits"
			);
		}
		let lsn_step = self.config.get_config_uint8(ConfigKey::OracleLsnStep);
		let mut inner = self.inner.write();
		let (version, lsn, at) = self.allocate_commit_version(
			&inner,
			SpanTiming::current(),
			&mut deltas,
			Some(Prepared {
				snapshot_keys: Vec::new(),
				buf: encoded,
			}),
			source,
			true,
		)?;
		inner.note_lsn(version, lsn, lsn_step);
		drop(inner);
		Ok((version, lsn, at, deltas))
	}
}

impl<L> CommitShared<L>
where
	L: VersionProvider,
{
	fn service(
		&self,
		state: &mut OracleState,
		version: CommitVersion,
		conflicts: ConflictManager,
		deltas: &mut CowVec<Delta>,
		rebase: Option<CommitRebase>,
		prepared: Option<Prepared>,
		window_size: u64,
		lsn_step: u64,
		timing: SpanTiming,
		evicted: &mut Vec<CommittedWindow>,
	) -> Result<CreateCommitResult, ValueError> {
		if version < state.evicted_up_through {
			return Ok(CreateCommitResult::TooOld);
		}

		if self.detect_conflicts(state, version, &conflicts, timing) {
			return Ok(CreateCommitResult::Conflict(conflicts));
		}

		if let Some(rebase) = rebase {
			rebase(deltas, self.versions.current()?)?;
		}

		let fresh = state.last_unchecked <= version;
		let (commit_version, lsn, at) =
			self.allocate_commit_version(state, timing, deltas, prepared, None, fresh)?;
		state.note_lsn(commit_version, lsn, lsn_step);

		let add_start = timing.mark(&self.clock);
		state.add_committed_transaction(commit_version, conflicts, window_size);
		timing.record_micros(add_start, "add_txn_us");

		if state.time_windows.len() > 1 {
			let cleanup_start = timing.mark(&self.clock);
			let safe_evict_below = self.query.done_until();
			cleanup_old_windows(
				&mut state.time_windows,
				&mut state.evicted_up_through,
				safe_evict_below,
				evicted,
			);
			timing.record_micros(cleanup_start, "cleanup_us");
		}

		Ok(CreateCommitResult::Success {
			version: commit_version,
			lsn,
			at,
		})
	}

	fn detect_conflicts(
		&self,
		state: &OracleState,
		version: CommitVersion,
		conflicts: &ConflictManager,
		timing: SpanTiming,
	) -> bool {
		let read_keys = conflicts.get_read_keys();
		let write_keys = conflicts.get_write_keys();
		if timing.recording {
			Span::current().record("read_keys", read_keys.len());
			Span::current().record("write_keys", write_keys.len());
		}
		let member_reads = conflicts.get_member_reads();
		let has_keys = !read_keys.is_empty() || !write_keys.is_empty() || !member_reads.is_empty();
		let has_ranges = conflicts.has_range_operations();
		if !has_keys && !has_ranges {
			return false;
		}

		let mut key_hashes: SmallVec<[u64; 8]> = SmallVec::new();
		if !has_ranges {
			for key in read_keys.iter().chain(write_keys.iter()) {
				key_hashes.push(hash_item(key));
			}
			for member in member_reads {
				key_hashes.push(hash_item(member));
			}
		}

		let conflict_start = timing.mark(&self.clock);
		let mut windows_checked = 0u64;
		let mut txns_checked = 0u64;
		for window in state.time_windows.values() {
			if window.max_version <= version {
				continue;
			}
			if !has_ranges && !key_hashes.iter().any(|hash| window.might_have_key_hash(*hash)) {
				continue;
			}
			windows_checked += 1;

			for committed_txn in &window.transactions {
				txns_checked += 1;
				if committed_txn.version <= version {
					continue;
				}

				if let Some(old_conflicts) = &committed_txn.conflict_manager
					&& conflicts.has_conflict(old_conflicts)
				{
					timing.record_micros(conflict_start, "conflict_check_us");
					if timing.recording {
						Span::current().record("windows_checked", windows_checked);
						Span::current().record("txns_checked", txns_checked);
						Span::current().record("has_conflict", true);
					}
					return true;
				}
			}
		}
		timing.record_micros(conflict_start, "conflict_check_us");
		if timing.recording {
			Span::current().record("windows_checked", windows_checked);
			Span::current().record("txns_checked", txns_checked);
		}
		false
	}

	#[inline]
	fn allocate_commit_version(
		&self,
		state: &OracleState,
		timing: SpanTiming,
		deltas: &mut CowVec<Delta>,
		prepared: Option<Prepared>,
		source: Option<SourceVersion>,
		fresh: bool,
	) -> Result<(CommitVersion, Lsn, DateTime), ValueError> {
		if state.commits_stopped {
			return Err(TransactionError::ShuttingDown.into());
		}
		let clock_start = timing.mark(&self.clock);
		let commit_version = self.versions.reserve()?;
		let source = source.unwrap_or(SourceVersion::from(commit_version));
		let buf = match prepared {
			Some(prepared) if fresh => prepared.buf,
			Some(prepared) => {
				self.resolve_keys(deltas, &prepared.snapshot_keys, commit_version)?;
				None
			}
			None => None,
		};
		let encoded = match buf {
			Some(mut buf) => {
				CommitRecord::stamp(&mut buf, commit_version, source);
				buf
			}
			None if self.wal.mode() == WalMode::Off => {
				self.resolve_unknown_pres(deltas, commit_version)?;
				Vec::new()
			}
			None => {
				self.resolve_unknown_pres(deltas, commit_version)?;
				let persistent: Vec<&Delta> =
					deltas.iter().filter(|delta| self.store.is_persistent(delta.key())).collect();
				let mut encoded = Vec::new();
				CommitRecord::encode_refs(
					commit_version,
					source,
					&persistent,
					&OperatorSection::default(),
					&[],
					&mut encoded,
				);
				check_record_size(&self.wal, &encoded)?;
				encoded
			}
		};
		self.query.register_in_flight(commit_version);
		self.command.register_in_flight(commit_version);
		reifydb_assertions! {
			assert!(
				self.command.done_until() < commit_version,
				"the done-until mark already passed version {} before it was registered; a \
				 snapshot opened at or above it would tear, and commit log consumers checkpointed \
				 past it would permanently skip its changes",
				commit_version.0
			);
		}
		let at = self.clock.now();
		let lsn = self.wal.append_encoded(at, CommitRecord::KIND, encoded).unwrap_or_else(|err| {
			panic!("commit {} failed to append to the commit log: {err}", commit_version.0)
		});
		if let Err(err) = MultiVersionCommit::commit(&self.store, deltas.clone(), commit_version) {
			panic!(
				"commit {} is in the commit log but failed to install in the store: {err}",
				commit_version.0
			);
		}
		self.versions.publish(commit_version);
		timing.record_micros(clock_start, "clock_next_us");

		self.version_epoch.record(
			EpochSeconds::new(
				u64::try_from(self.clock.now().to_secs())
					.expect("system clock is never before the Unix epoch"),
			),
			commit_version.0,
		);

		Ok((commit_version, lsn, at))
	}

	fn system_context(&self) -> TransactionContext {
		TransactionContext::system(
			RuntimeContext::builder(Spawner::inline())
				.clock(self.clock.clone())
				.rng(self.rng.clone())
				.version_epoch(self.version_epoch.clone())
				.build(),
		)
	}

	fn resolve_keys(
		&self,
		deltas: &mut CowVec<Delta>,
		keys: &[TaggedKey],
		version: CommitVersion,
	) -> Result<(), ValueError> {
		if keys.is_empty() {
			return Ok(());
		}
		let mut previous: HashMap<&TaggedKey, SetPre> = keys
			.iter()
			.zip(self.store.get_previous(&self.system_context(), keys, version)?)
			.map(|(key, row)| {
				let pre = match row {
					Some(row) => SetPre::Present(row.bytes),
					None => SetPre::Absent,
				};
				(key, pre)
			})
			.collect();
		for delta in deltas.make_mut() {
			if let Delta::Set {
				key,
				pre,
				..
			} = delta && let Some(found) = previous.remove(key)
			{
				*pre = found;
			}
		}
		Ok(())
	}

	fn resolve_unknown_pres(&self, deltas: &mut CowVec<Delta>, version: CommitVersion) -> Result<(), ValueError> {
		let unknown: Vec<TaggedKey> = deltas
			.iter()
			.filter_map(|delta| match delta {
				Delta::Set {
					key,
					pre: SetPre::Unknown,
					..
				} => Some(key.clone()),
				_ => None,
			})
			.collect();
		if unknown.is_empty() {
			return Ok(());
		}
		let mut previous = self.store.get_previous(&self.system_context(), &unknown, version)?.into_iter();
		for delta in deltas.make_mut() {
			if let Delta::Set {
				pre,
				..
			} = delta && matches!(pre, SetPre::Unknown)
			{
				*pre = match previous.next().expect("one previous entry per unknown pre") {
					Some(row) => SetPre::Present(row.bytes),
					None => SetPre::Absent,
				};
			}
		}
		Ok(())
	}
}

pub(crate) fn note_lsn(lsns: &mut VecDeque<(CommitVersion, Lsn)>, version: CommitVersion, lsn: Lsn, step: u64) {
	if lsns.back().is_none_or(|(last, _)| last.0 / step < version.0 / step) {
		lsns.push_back((version, lsn));
	}
}

impl OracleState {
	fn note_lsn(&mut self, version: CommitVersion, lsn: Lsn, step: u64) {
		note_lsn(&mut self.lsns, version, lsn, step);
	}

	pub(crate) fn lsn_through(&mut self, version: CommitVersion) -> Option<Lsn> {
		let found = self.lsns.partition_point(|(noted, _)| *noted <= version);
		if found == 0 {
			return None;
		}
		self.lsns.drain(..found - 1);
		self.lsns.front().map(|(_, lsn)| *lsn)
	}

	fn add_committed_transaction(&mut self, version: CommitVersion, conflicts: ConflictManager, window_size: u64) {
		let window_start = CommitVersion((version.0 / window_size) * window_size);

		let key_capacity =
			self.time_windows.last_key_value().map_or(0, |(_, window)| window.modified_keys.len());
		let window = self
			.time_windows
			.entry(window_start)
			.or_insert_with(|| CommittedWindow::new(window_start, key_capacity));

		let txn = CommittedTxn {
			version,
			conflict_manager: Some(conflicts),
		};

		window.add_transaction(txn);
	}
}

impl<L> Drop for Oracle<L>
where
	L: VersionProvider,
{
	fn drop(&mut self) {
		self.stop();
	}
}

#[cfg(test)]
mod tests {
	use std::{
		mem::discriminant,
		sync::{
			Arc, Barrier,
			atomic::{AtomicU64, Ordering},
		},
		thread,
		thread::sleep,
	};

	use reifydb_codec::row::bytes::EncodedBytes;
	use reifydb_core::{default, interface::store::MultiVersionGet, testing::ProfileConfig, wal::WalRecord};
	use reifydb_runtime::{actor::system::ActorSystem, context::clock::MockClock};
	use reifydb_store_multi::MultiStore;
	use reifydb_value::{
		byte_size::ByteSize,
		value::{Value, duration::Duration},
	};

	use super::*;
	use crate::{
		multi::transaction::version::VersionProvider,
		wal::{memory_wal, nop_wal},
	};

	#[derive(Debug, Clone)]
	struct MockVersionProvider {
		current: Arc<AtomicU64>,
	}

	impl MockVersionProvider {
		fn new(start: impl Into<CommitVersion>) -> Self {
			Self {
				current: Arc::new(AtomicU64::new(start.into().0)),
			}
		}

		fn advance_to(&self, version: CommitVersion) {
			self.current.fetch_max(version.0, Ordering::Relaxed);
		}
	}

	impl VersionProvider for MockVersionProvider {
		fn next(&self) -> Result<CommitVersion, ValueError> {
			Ok(CommitVersion(self.current.fetch_add(1, Ordering::Relaxed) + 1))
		}

		fn reserve(&self) -> Result<CommitVersion, ValueError> {
			Ok(CommitVersion(self.current.load(Ordering::Relaxed) + 1))
		}

		fn publish(&self, version: CommitVersion) {
			self.current.fetch_max(version.0, Ordering::Relaxed);
		}

		fn current(&self) -> Result<CommitVersion, ValueError> {
			Ok(CommitVersion(self.current.load(Ordering::Relaxed)))
		}
	}

	use std::ops::Bound;

	use reifydb_core::{
		interface::catalog::{
			id::{IndexId, TableId},
			object::ObjectId,
		},
		key::{
			bound::{TaggedKeyBound, TaggedKeyBoundRange},
			catalog::IndexEntryKey,
		},
		value::index::encoded::EncodedIndexKey,
	};

	// IndexEntry appends its tail verbatim, so encoded order still matches the raw string order.
	fn create_test_key(s: &str) -> TaggedKey {
		IndexEntryKey::new(
			ObjectId::Table(TableId(1)),
			IndexId::primary(1u64),
			EncodedIndexKey::new(s.as_bytes()),
		)
		.into()
	}

	fn create_test_range(start: &str, end: &str) -> TaggedKeyBoundRange {
		TaggedKeyBoundRange {
			start: Bound::Included(TaggedKeyBound::Key(create_test_key(start))),
			end: Bound::Excluded(TaggedKeyBound::Key(create_test_key(end))),
		}
	}

	const PINNED_WINDOW_SIZE: u64 = 500;

	struct PinnedWindowConfig;

	impl GetConfig for PinnedWindowConfig {
		fn get_config(&self, key: ConfigKey) -> Value {
			// Window boundaries are placed by arithmetic below, so the size must never follow the profile.
			match key {
				ConfigKey::OracleWindowSize => Value::Uint8(PINNED_WINDOW_SIZE),
				other => other.default_value(),
			}
		}

		fn get_config_at(&self, key: ConfigKey, _version: CommitVersion) -> Value {
			self.get_config(key)
		}
	}

	struct SmallStepConfig;

	impl GetConfig for SmallStepConfig {
		fn get_config(&self, key: ConfigKey) -> Value {
			// the step must be far below the number of commits the test makes
			match key {
				ConfigKey::OracleLsnStep => Value::Uint8(8),
				other => other.default_value(),
			}
		}

		fn get_config_at(&self, key: ConfigKey, _version: CommitVersion) -> Value {
			self.get_config(key)
		}
	}

	fn create_test_oracle(start: impl Into<CommitVersion>) -> Oracle<MockVersionProvider> {
		build_oracle(start, Arc::new(ProfileConfig))
	}

	fn create_windowed_oracle(start: impl Into<CommitVersion>) -> Oracle<MockVersionProvider> {
		build_oracle(start, Arc::new(PinnedWindowConfig))
	}

	fn no_deltas() -> CowVec<Delta> {
		// These tests assert conflict detection and windowing only, so the store install must stay a no-op.
		CowVec::new(vec![])
	}

	fn build_oracle(start: impl Into<CommitVersion>, config: Arc<dyn GetConfig>) -> Oracle<MockVersionProvider> {
		let clock = MockVersionProvider::new(start);
		let actor_system = ActorSystem::testing(Clock::Real);
		let spawner = actor_system.spawner();

		let time = Clock::Mock(MockClock::from_millis(1000));
		Oracle::new(
			clock,
			spawner,
			MultiStore::testing_memory(),
			time.clone(),
			VersionEpoch::new(),
			Rng::seeded(42),
			config,
			memory_wal(time).unwrap(),
			VecDeque::new(),
		)
	}

	#[test]
	fn test_window_creation_and_indexing() {
		let oracle = create_test_oracle(0);

		let mut conflicts = ConflictManager::new();
		let key1 = create_test_key("key1");
		let key2 = create_test_key("key2");
		conflicts.mark_write(&key1);
		conflicts.mark_write(&key2);

		let (result, _) = oracle.new_commit(CommitVersion(1), conflicts, no_deltas(), None).unwrap();

		match result {
			CreateCommitResult::Success {
				version,
				..
			} => {
				assert!(version.0 >= 1);

				let inner = oracle.inner.read();
				assert!(!inner.time_windows.is_empty());
				let any_window_has_key1 =
					inner.time_windows.values().any(|w| w.modified_keys.contains(&key1));
				let any_window_has_key2 =
					inner.time_windows.values().any(|w| w.modified_keys.contains(&key2));
				assert!(any_window_has_key1);
				assert!(any_window_has_key2);
			}
			CreateCommitResult::Conflict(_) => panic!("Unexpected conflict for first transaction"),
			CreateCommitResult::TooOld => panic!("Unexpected TooOld for first transaction"),
		}
	}

	#[test]
	fn test_key_indexing_multiple_windows() {
		let oracle = create_test_oracle(0);

		let key1 = create_test_key("key1");
		let key2 = create_test_key("key2");

		for i in 0..3 {
			let mut conflicts = ConflictManager::new();
			if i % 2 == 0 {
				conflicts.mark_write(&key1);
			} else {
				conflicts.mark_write(&key2);
			}

			let version_start = CommitVersion(i as u64 * 500 + 1);
			let (result, _) = oracle.new_commit(version_start, conflicts, no_deltas(), None).unwrap();
			assert!(matches!(result, CreateCommitResult::Success { .. }));
		}

		let inner = oracle.inner.read();

		let key1_window_count = inner.time_windows.values().filter(|w| w.modified_keys.contains(&key1)).count();
		assert!(key1_window_count >= 1);

		let key2_window_count = inner.time_windows.values().filter(|w| w.modified_keys.contains(&key2)).count();
		assert!(key2_window_count >= 1);
	}

	#[test]
	fn test_range_operations_fallback() {
		// A range read cannot be bloom-indexed by key, so it must fall back to scanning every window.
		let oracle = create_test_oracle(1);

		let key1 = create_test_key("key1");

		let mut conflicts1 = ConflictManager::new();
		conflicts1.mark_write(&key1);

		let (result1, _) = oracle.new_commit(CommitVersion(1), conflicts1, no_deltas(), None).unwrap();
		assert!(matches!(result1, CreateCommitResult::Success { .. }));

		let mut conflicts2 = ConflictManager::new();
		let range = create_test_range("a", "z");
		conflicts2.mark_range(range);
		conflicts2.mark_write(&create_test_key("other_key"));

		let (result2, _) = oracle.new_commit(CommitVersion(1), conflicts2, no_deltas(), None).unwrap();

		assert!(matches!(result2, CreateCommitResult::Conflict(_)));
	}

	#[test]
	fn test_range_only_read_finds_conflict_in_older_window() {
		// A range-only read must still conflict with a write committed after its read version,
		// even when that write lives in a window whose start is below the read version.
		// The window size is pinned to 500, so a clock at 749 puts T1's commit (750) in window 500.
		let oracle = create_windowed_oracle(749);

		let key_k = create_test_key("k");

		let mut conflicts1 = ConflictManager::new();
		conflicts1.mark_write(&key_k);
		let (r1, _) = oracle.new_commit(CommitVersion(1), conflicts1, no_deltas(), None).unwrap();
		let commit_v1 = match r1 {
			CreateCommitResult::Success {
				version: v,
				..
			} => v,
			_ => panic!("T1 should commit"),
		};
		assert_eq!(commit_v1, CommitVersion(750));

		// If the window-size default changes, this fires before the conflict assert can pass vacuously.
		{
			let inner = oracle.inner.read();
			assert!(
				inner.time_windows.contains_key(&CommitVersion(500)),
				"expected T1's window_start to be 500 (PINNED_WINDOW_SIZE=500); \
				 test assumptions invalidated"
			);
		}

		// Read version 510 sits inside T1's window but before T1's commit, so the conflict is real.
		let mut conflicts2 = ConflictManager::new();
		conflicts2.mark_range(create_test_range("a", "z"));
		let (r2, _) = oracle.new_commit(CommitVersion(510), conflicts2, no_deltas(), None).unwrap();

		assert!(
			matches!(r2, CreateCommitResult::Conflict(_)),
			"T2's range read of 'k' must conflict with T1's write at version 750 > 510, \
			 but the !has_keys branch in oracle/mod.rs:225 skips windows whose \
			 window_start < read_version"
		);
	}

	#[test]
	fn test_range_op_with_keys_scans_all_windows_not_just_bloom_matches() {
		// A range op must scan every kept window: a bloom match on the transaction's own keys
		// would skip the window where the range conflict actually lives.
		// The window size is pinned to 500, so v=50 and v=750 land in different windows.
		let oracle = create_windowed_oracle(49);

		let key_alpha = create_test_key("alpha");
		let key_beta = create_test_key("beta");

		let mut conflicts_b = ConflictManager::new();
		conflicts_b.mark_write(&key_beta);
		let (r_b, _) = oracle.new_commit(CommitVersion(1), conflicts_b, no_deltas(), None).unwrap();
		let commit_v_b = match r_b {
			CreateCommitResult::Success {
				version: v,
				..
			} => v,
			_ => panic!("T_b should commit"),
		};
		assert_eq!(commit_v_b, CommitVersion(50));

		// Skip the clock forward so T_a lands in window @ 500 with a bloom disjoint from window @ 0.
		oracle.versions.advance_to(CommitVersion(749));

		let mut conflicts_a = ConflictManager::new();
		conflicts_a.mark_write(&key_alpha);
		let (r_a, _) = oracle.new_commit(CommitVersion(1), conflicts_a, no_deltas(), None).unwrap();
		let commit_v_a = match r_a {
			CreateCommitResult::Success {
				version: v,
				..
			} => v,
			_ => panic!("T_a should commit"),
		};
		assert_eq!(commit_v_a, CommitVersion(750));

		// If the window-size default changes, this fires before the conflict assert can pass vacuously.
		{
			let inner = oracle.inner.read();
			assert!(
				inner.time_windows.contains_key(&CommitVersion(0)),
				"expected T_b's window_start to be 0 (PINNED_WINDOW_SIZE=500); \
				 test assumptions invalidated"
			);
			assert!(
				inner.time_windows.contains_key(&CommitVersion(500)),
				"expected T_a's window_start to be 500 (PINNED_WINDOW_SIZE=500); \
				 test assumptions invalidated"
			);
		}

		// Reading at 100 makes window @ 0 skippable, so only the unmatched window @ 500 holds the conflict.
		let mut conflicts_3 = ConflictManager::new();
		conflicts_3.mark_write(&key_beta);
		conflicts_3.mark_range(create_test_range("a", "z"));
		let (r_3, _) = oracle.new_commit(CommitVersion(100), conflicts_3, no_deltas(), None).unwrap();

		assert!(
			matches!(r_3, CreateCommitResult::Conflict(_)),
			"T3's range 'a..z' overlaps T_a's write of 'alpha' (v=750 > 100), \
			 but T3's specific write key 'beta' only bloom-matches window @ 0. \
			 Range ops must force a scan of all kept windows, including window @ 500."
		);
	}

	#[test]
	fn test_empty_conflict_manager() {
		let oracle = create_test_oracle(0);

		let conflicts = ConflictManager::new();

		let (result, _) = oracle.new_commit(CommitVersion(1), conflicts, no_deltas(), None).unwrap();

		match result {
			CreateCommitResult::Success {
				..
			} => {
				let inner = oracle.inner.read();
				let total_modified: usize =
					inner.time_windows.values().map(|w| w.modified_keys.len()).sum();
				assert_eq!(total_modified, 0);
			}
			CreateCommitResult::Conflict(_) => {
				panic!("Empty conflict manager should not cause conflicts")
			}
			CreateCommitResult::TooOld => panic!("Unexpected TooOld for empty conflict manager"),
		}
	}

	#[test]
	fn test_write_write_conflict() {
		let oracle = create_test_oracle(1);

		let shared_key = create_test_key("shared_key");

		let mut conflicts1 = ConflictManager::new();
		conflicts1.mark_write(&shared_key);

		let (result1, _) = oracle.new_commit(CommitVersion(1), conflicts1, no_deltas(), None).unwrap();
		assert!(matches!(result1, CreateCommitResult::Success { .. }));

		let mut conflicts2 = ConflictManager::new();
		conflicts2.mark_write(&shared_key);

		let (result2, _) = oracle.new_commit(CommitVersion(1), conflicts2, no_deltas(), None).unwrap();

		assert!(matches!(result2, CreateCommitResult::Conflict(_)));
	}

	#[test]
	fn test_read_write_conflict() {
		let oracle = create_test_oracle(1);

		let shared_key = create_test_key("shared_key");

		let mut conflicts1 = ConflictManager::new();
		conflicts1.mark_write(&shared_key);

		let (result1, _) = oracle.new_commit(CommitVersion(1), conflicts1, no_deltas(), None).unwrap();
		assert!(matches!(result1, CreateCommitResult::Success { .. }));

		let mut conflicts2 = ConflictManager::new();
		conflicts2.mark_read(&shared_key);

		let (result2, _) = oracle.new_commit(CommitVersion(1), conflicts2, no_deltas(), None).unwrap();

		assert!(matches!(result2, CreateCommitResult::Conflict(_)));
	}

	#[test]
	fn test_concurrent_commits_dont_skip_done_until_versions() {
		// Versions must be registered on done until under the allocation lock; register out of
		// order and done_until stalls one short, reporting a commit as unapplied forever.
		const NUM_CONCURRENT: usize = 100;
		const ITERATIONS: usize = 10;

		for iteration in 0..ITERATIONS {
			let oracle = Arc::new(create_test_oracle(0));
			let mut handles = vec![];

			for i in 0..NUM_CONCURRENT {
				let oracle_clone = oracle.clone();
				// Unique keys per iteration so no commit can fail on conflict.
				let key = create_test_key(&format!("key_{}_{}", iteration, i));

				let handle = thread::spawn(move || {
					let mut conflicts = ConflictManager::new();
					conflicts.mark_write(&key);

					let (result, _) = oracle_clone
						.new_commit(CommitVersion(1), conflicts, no_deltas(), None)
						.unwrap();

					match result {
						CreateCommitResult::Success {
							version,
							..
						} => {
							// Uneven delay so commits finish out of allocation order.
							if i % 3 == 0 {
								sleep(Duration::from_microseconds(100)
									.unwrap()
									.to_std());
							}
							oracle_clone.done_commit(version);
							Some(version)
						}
						CreateCommitResult::Conflict(_) => None,
						CreateCommitResult::TooOld => None,
					}
				});
				handles.push(handle);
			}

			let mut max_version = CommitVersion(0);
			let mut success_count = 0;
			for handle in handles {
				if let Some(v) = handle.join().unwrap() {
					max_version = max_version.max(v);
					success_count += 1;
				}
			}

			assert_eq!(
				success_count, NUM_CONCURRENT,
				"Expected {} successful commits, got {}",
				NUM_CONCURRENT, success_count
			);

			// Wait on the event, not a sleep: a skipped version stalls the advance and times out.
			let reached =
				oracle.command.wait_for_mark_timeout(max_version, Duration::from_seconds(5).unwrap());
			assert!(reached, "done until did not reach {} within timeout", max_version.0);

			let done_until = oracle.command.done_until();
			assert_eq!(
				done_until, max_version,
				"Done until race condition detected! done_until={} but max_version={}. \
				 Some version was skipped.",
				done_until.0, max_version.0
			);
		}
	}

	#[test]
	fn test_version_begin_ordering() {
		// A barrier releases every commit at once; allocated versions must still come out contiguous.
		let oracle = Arc::new(create_test_oracle(0));
		let barrier = Arc::new(Barrier::new(10));

		let mut handles = vec![];

		for i in 0..10 {
			let oracle_clone = oracle.clone();
			let barrier_clone = barrier.clone();
			let key = create_test_key(&format!("order_key_{}", i));

			let handle = thread::spawn(move || {
				barrier_clone.wait();

				let mut conflicts = ConflictManager::new();
				conflicts.mark_write(&key);

				let (result, _) = oracle_clone
					.new_commit(CommitVersion(1), conflicts, no_deltas(), None)
					.unwrap();

				if let CreateCommitResult::Success {
					version,
					..
				} = result
				{
					oracle_clone.done_commit(version);
					version
				} else {
					CommitVersion(0)
				}
			});
			handles.push(handle);
		}

		let mut versions: Vec<u64> = vec![];
		for handle in handles {
			let v = handle.join().unwrap();
			if v.0 > 0 {
				versions.push(v.0);
			}
		}

		versions.sort();
		for i in 1..versions.len() {
			assert_eq!(
				versions[i],
				versions[i - 1] + 1,
				"Version gap detected: {} -> {}. Versions should be contiguous.",
				versions[i - 1],
				versions[i]
			);
		}

		// Wait on the event, not a sleep: a dropped version stalls the advance and times out.
		let expected = *versions.last().unwrap_or(&0);
		let reached = oracle
			.command
			.wait_for_mark_timeout(CommitVersion(expected), Duration::from_seconds(5).unwrap());
		assert!(reached, "done until did not reach {} within timeout", expected);

		let done_until = oracle.command.done_until();
		assert_eq!(done_until.0, expected, "Done until should be at highest committed version");
	}

	#[test]
	fn sustained_concurrent_disjoint_commits_all_reach_done_until() {
		// The commit queue parks requesters and may hand their work to the combiner or the
		// sequencer thread; a lost unpark or a dropped queue node strands a committer
		// forever and a skipped done-until registration wedges done_until, so both are
		// exercised under sustained queue pressure rather than one commit per thread.
		const THREADS: usize = 8;
		const PER_THREAD: usize = 250;

		let oracle = Arc::new(create_test_oracle(0));
		let mut handles = vec![];

		for t in 0..THREADS {
			let oracle = oracle.clone();
			handles.push(thread::spawn(move || {
				let mut max_version = CommitVersion(0);
				for i in 0..PER_THREAD {
					let key = create_test_key(&format!("key_{}_{}", t, i));
					let mut conflicts = ConflictManager::new();
					conflicts.mark_write(&key);
					match oracle
						.new_commit(CommitVersion(1), conflicts, no_deltas(), None)
						.unwrap()
						.0
					{
						CreateCommitResult::Success {
							version,
							..
						} => {
							oracle.done_commit(version);
							max_version = max_version.max(version);
						}
						other => panic!(
							"disjoint keys must never conflict, got variant {:?}",
							discriminant(&other)
						),
					}
				}
				max_version
			}));
		}

		let mut max_version = CommitVersion(0);
		for handle in handles {
			max_version = max_version.max(handle.join().unwrap());
		}

		let reached = oracle.command.wait_for_mark_timeout(max_version, Duration::from_seconds(5).unwrap());
		assert!(
			reached,
			"done_until stalled below {}; a commit was serviced without registering on \
			 done until or a waiter was never woken",
			max_version.0
		);
	}

	#[cfg(not(reifydb_single_threaded))]
	#[test]
	fn stop_joins_the_sequencer_thread_cleanly() {
		// Oracle::stop must join the sequencer thread; if the stop signal or the final
		// queue drain is lost, either this test hangs on join or a committer parked on an
		// unserviced request never wakes.
		let mut oracle = create_test_oracle(0);

		let mut conflicts = ConflictManager::new();
		conflicts.mark_write(&create_test_key("pre_stop"));
		let (result, _) = oracle.new_commit(CommitVersion(1), conflicts, no_deltas(), None).unwrap();
		assert!(matches!(result, CreateCommitResult::Success { .. }));

		oracle.stop();
	}

	#[test]
	fn test_disabled_then_new_commit_skips_conflict_registration() {
		// rollback() after set_disabled() must restore tracking, or the following mark_write is
		// silently dropped and the oracle registers an empty window for T1.
		// A clock at 1 puts T1's commit at 2, so T2's read at 1 is strictly before it.
		let oracle = create_test_oracle(1);
		let key = create_test_key("shared");

		let mut cm1 = ConflictManager::new();
		cm1.set_disabled();
		cm1.rollback();
		cm1.mark_write(&key);
		assert!(
			cm1.get_write_keys().contains(&key),
			"rollback must reset ConflictMode to Tracking; otherwise the reused \
			 manager would silently drop mark_write and the oracle would register \
			 an empty window for this transaction"
		);

		let v1 = match oracle.new_commit(CommitVersion(1), cm1, no_deltas(), None).unwrap().0 {
			CreateCommitResult::Success {
				version: v,
				..
			} => v,
			other => panic!("T1 should commit, got variant {:?}", discriminant(&other)),
		};
		assert!(v1.0 >= 2, "T1's commit version should be at least 2, got {}", v1.0);

		// An empty window for T1 would let this read+write of the same key through unnoticed.
		let mut cm2 = ConflictManager::new();
		cm2.mark_read(&key);
		cm2.mark_write(&key);
		let (r2, _) = oracle.new_commit(CommitVersion(1), cm2, no_deltas(), None).unwrap();

		assert!(
			matches!(r2, CreateCommitResult::Conflict(_)),
			"T2's read+write of `shared` (read_version=1) must conflict with T1's \
			 write at v={}",
			v1.0
		);
	}

	#[test]
	fn lsn_through_errs_low_keeps_its_answer_and_drops_only_older_entries() {
		// A floor picked above the saved version lets gc drop records a crash still needs.
		let mut state = OracleState {
			time_windows: BTreeMap::new(),
			evicted_up_through: CommitVersion(0),
			commits_stopped: false,
			last_unchecked: CommitVersion(0),
			lsns: VecDeque::new(),
		};
		for (version, lsn) in [(5u64, 1u64), (4096, 7), (4100, 8), (8192, 12)] {
			state.note_lsn(CommitVersion(version), Lsn::new(lsn), default::query::ORACLE_LSN_STEP);
		}
		assert_eq!(
			state.lsns.iter().map(|(v, _)| v.0).collect::<Vec<_>>(),
			vec![5, 4096, 8192],
			"exactly one entry per 4096 step, the first commit of each"
		);
		assert_eq!(state.lsn_through(CommitVersion(4)), None, "nothing is saved below the first entry");
		assert_eq!(
			state.lsn_through(CommitVersion(8191)),
			Some(Lsn::new(7)),
			"the last entry at or below wins"
		);
		assert_eq!(state.lsns.front().map(|(v, _)| v.0), Some(4096), "the answer stays for the next lookup");
		assert_eq!(state.lsn_through(CommitVersion(8191)), Some(Lsn::new(7)), "a repeat lookup is stable");
		assert_eq!(state.lsn_through(CommitVersion(u64::MAX)), Some(Lsn::new(12)));
		assert_eq!(state.lsns.len(), 1, "every entry before the answer is pruned");
	}

	#[test]
	fn a_smaller_step_keeps_one_entry_per_step_and_still_errs_low() {
		// a note_lsn that ignores its step keeps one entry for all four versions
		let mut state = OracleState {
			time_windows: BTreeMap::new(),
			evicted_up_through: CommitVersion(0),
			commits_stopped: false,
			last_unchecked: CommitVersion(0),
			lsns: VecDeque::new(),
		};
		for (version, lsn) in [(5u64, 1u64), (8, 2), (9, 3), (16, 4)] {
			state.note_lsn(CommitVersion(version), Lsn::new(lsn), 8);
		}
		assert_eq!(
			state.lsns.iter().map(|(v, _)| v.0).collect::<Vec<_>>(),
			vec![5, 8, 16],
			"exactly one entry per step of 8, the first commit of each"
		);
		assert_eq!(state.lsn_through(CommitVersion(15)), Some(Lsn::new(2)), "the last entry at or below wins");
	}

	#[test]
	fn a_commit_notes_its_lsn_at_the_configured_step() {
		// a commit site that passes the default step keeps one entry for all 17 commits
		let oracle = build_oracle(0, Arc::new(SmallStepConfig));
		for _ in 0..17 {
			oracle.new_commit(CommitVersion(1), ConflictManager::new(), no_deltas(), None).unwrap();
		}
		let noted: Vec<u64> = oracle.inner.read().lsns.iter().map(|(version, _)| version.0).collect();
		assert!(noted.len() >= 2, "17 commits cross at least two steps of 8, got {noted:?}");
		assert!(
			noted.windows(2).all(|pair| pair[0] / 8 < pair[1] / 8),
			"exactly one entry per step of 8, got {noted:?}"
		);
	}

	#[test]
	fn a_new_window_starts_its_key_index_at_the_previous_windows_key_count() {
		// a window that grows its key index from scratch rehashes millions of keys inside the commit hold.
		let oracle = create_test_oracle(0);
		let mut state = oracle.inner.write();
		for version in 1..10u64 {
			let mut conflicts = ConflictManager::new();
			for index in 0..1000 {
				conflicts.mark_write(&create_test_key(&format!("v{version}-k{index}")));
			}
			state.add_committed_transaction(CommitVersion(version), conflicts, 10);
		}
		let previous = state.time_windows[&CommitVersion(0)].modified_keys.len();
		state.add_committed_transaction(CommitVersion(10), ConflictManager::new(), 10);

		let opened = state.time_windows[&CommitVersion(10)].modified_keys.capacity();
		assert!(
			opened >= previous,
			"the new window holds room for {opened} keys, the last one needed {previous}"
		);
	}

	#[test]
	fn an_evicted_window_is_handed_back_instead_of_freed_inside_the_hold() {
		// freeing a full window under the oracle write lock stalls every commit for tens of milliseconds.
		let mut windows = BTreeMap::new();
		windows.insert(CommitVersion(0), CommittedWindow::new(CommitVersion(9), 0));
		windows.insert(CommitVersion(10), CommittedWindow::new(CommitVersion(19), 0));
		let mut evicted_up_through = CommitVersion(0);
		let mut evicted = Vec::new();

		cleanup_old_windows(&mut windows, &mut evicted_up_through, CommitVersion(9), &mut evicted);

		assert_eq!(evicted.len(), 1, "the window below the bound must come back to the caller");
		assert_eq!(evicted[0].max_version(), CommitVersion(9));
		assert_eq!(windows.len(), 1);
		assert_eq!(
			evicted_up_through,
			CommitVersion(9),
			"eviction must still raise the too-old bound inside the hold"
		);
	}

	#[test]
	fn a_presized_key_index_never_reserves_past_its_ceiling() {
		// one bulk window must not make the next window reserve an index sized for an unbounded number of keys.
		let index = ModifiedKeyIndex::with_capacity(4 * WINDOW_KEY_CAPACITY_MAX);

		assert!(
			index.capacity() <= 2 * WINDOW_KEY_CAPACITY_MAX,
			"a presize of four ceilings reserved {} slots, past the ceiling",
			index.capacity()
		);
	}

	#[test]
	fn a_wal_off_commit_never_builds_a_log_record() {
		// with the log off no record is read, so a row larger than a segment must still commit.
		// mutation: without the wal off arm the record is encoded and rejected as too large.
		let segment = ByteSize::from_bytes(4096);
		let time = Clock::Mock(MockClock::from_millis(1000));
		let oracle = Oracle::new(
			MockVersionProvider::new(0),
			ActorSystem::testing(Clock::Real).spawner(),
			MultiStore::testing_memory(),
			time.clone(),
			VersionEpoch::new(),
			Rng::seeded(42),
			Arc::new(ProfileConfig),
			nop_wal(segment).unwrap(),
			VecDeque::new(),
		);
		assert_eq!(oracle.wal.mode(), WalMode::Off, "the case needs a commit log that is switched off");

		let key = create_test_key("larger-than-a-segment");
		let bytes = EncodedBytes(CowVec::new(vec![7u8; 2 * segment.as_bytes() as usize]));
		let deltas = CowVec::new(vec![Delta::Set {
			key: key.clone(),
			bytes: bytes.clone(),
			pre: SetPre::Absent,
		}]);

		let (result, _) = oracle
			.advance_unchecked(deltas, None)
			.expect("a wal off commit must never be rejected for a record the log does not keep");
		let CreateCommitResult::Success {
			version,
			lsn,
			..
		} = result
		else {
			panic!("an unchecked commit must succeed");
		};
		assert_eq!(lsn, Lsn::FIRST, "the switched off log must still hand out the next lsn");

		let read = oracle.store.get(&oracle.system_context(), &[key], version).unwrap();
		assert_eq!(
			read.into_iter().next().flatten().map(|row| row.bytes),
			Some(bytes),
			"the row must be installed at the commit version even though no record was built"
		);
	}

	#[test]
	fn an_unchecked_commit_marks_the_oracle_and_a_flow_commit_does_not() {
		// a flow commit must not move the mark, otherwise every later checked commit reads its old values again
		let oracle = Oracle::new(
			MockVersionProvider::new(0),
			ActorSystem::testing(Clock::Real).spawner(),
			MultiStore::testing_memory(),
			Clock::Mock(MockClock::from_millis(1000)),
			VersionEpoch::new(),
			Rng::seeded(42),
			Arc::new(ProfileConfig),
			nop_wal(ByteSize::from_bytes(4096)).unwrap(),
			VecDeque::new(),
		);
		let set = |name: &str| {
			CowVec::new(vec![Delta::Set {
				key: create_test_key(name),
				bytes: EncodedBytes(CowVec::new(vec![1])),
				pre: SetPre::Absent,
			}])
		};

		let (result, _) = oracle.advance_unchecked(set("unchecked"), None).expect("the unchecked commit lands");
		let CreateCommitResult::Success {
			version: unchecked,
			..
		} = result
		else {
			panic!("an unchecked commit must succeed");
		};
		assert_eq!(
			oracle.inner.read().last_unchecked,
			unchecked,
			"an unchecked commit must mark the oracle, otherwise a checked commit trusts a stale old value"
		);

		let (flow, _lsn, _at, _deltas) =
			oracle.commit_deferred_flow(set("flow"), None, None).expect("the flow commit lands");
		assert!(flow > unchecked, "precondition: the flow commit is the later one");
		assert_eq!(
			oracle.inner.read().last_unchecked,
			unchecked,
			"a flow commit must leave the mark where the unchecked commit put it"
		);
	}

	fn deferred_flow_record(deltas: &CowVec<Delta>) -> Vec<u8> {
		let refs: Vec<&Delta> = deltas.iter().collect();
		let mut buf = Vec::new();
		CommitRecord::encode_refs(
			CommitVersion(0),
			SourceVersion(0),
			&refs,
			&OperatorSection::default(),
			&[],
			&mut buf,
		);
		buf
	}

	#[test]
	fn a_deferred_flow_commit_older_than_the_evicted_window_still_commits() {
		// A flow commit has no read set to validate, so a TooOld check would only fail it for nothing.
		let oracle = create_test_oracle(0);
		let (seeded, _) = oracle
			.new_commit(CommitVersion(0), ConflictManager::new(), no_deltas(), None)
			.expect("the seed commit lands");
		assert!(matches!(seeded, CreateCommitResult::Success { .. }), "the seed commit must land");
		let current = oracle.versions.current().expect("the current version is readable");
		oracle.inner.write().evicted_up_through = CommitVersion(current.0 + 1);
		let (stale, _) = oracle
			.new_commit(CommitVersion(0), ConflictManager::new(), no_deltas(), None)
			.expect("a stale checked commit returns a result");
		assert!(matches!(stale, CreateCommitResult::TooOld), "precondition: the window below is evicted");

		let deltas = no_deltas();
		let buf = deferred_flow_record(&deltas);
		let (version, _lsn, _at, _deltas) =
			oracle.commit_deferred_flow(deltas, Some(buf), None).expect("the flow commit lands");

		assert_eq!(version, CommitVersion(current.0 + 1), "the flow commit takes the next version");
	}

	#[test]
	fn a_deferred_flow_commit_installs_its_stamped_record_without_reading_the_store() {
		// A record left at version 0 replays every flow commit onto the wrong version after a crash.
		let oracle = create_test_oracle(0);
		let key = create_test_key("stored");
		let old = EncodedBytes(CowVec::new(vec![1u8; 4]));
		let new = EncodedBytes(CowVec::new(vec![2u8; 4]));
		oracle.new_commit(
			CommitVersion(0),
			ConflictManager::new(),
			CowVec::new(vec![Delta::Set {
				key: key.clone(),
				bytes: old.clone(),
				pre: SetPre::Absent,
			}]),
			None,
		)
		.expect("the seed commit lands");
		let source = SourceVersion(42);
		let deltas = CowVec::new(vec![Delta::Set {
			key: key.clone(),
			bytes: new.clone(),
			pre: SetPre::Present(old.clone()),
		}]);
		let buf = deferred_flow_record(&deltas);

		let before = oracle.inner.read().last_unchecked;
		let (version, lsn, _at, _deltas) =
			oracle.commit_deferred_flow(deltas, Some(buf), Some(source)).expect("the flow commit lands");

		let record = oracle
			.wal
			.replay(Lsn::FIRST)
			.expect("the commit log replays from its first record")
			.map(|replayed| replayed.expect("every appended record decodes"))
			.find(|replayed| replayed.lsn == lsn)
			.map(|replayed| replayed.body)
			.expect("the flow commit must reach the log");
		let WalRecord::Multi(record) = record else {
			panic!("a flow commit logs a multi record");
		};
		assert_eq!(record.version, version, "the record must carry the commit version, not 0");
		assert_eq!(record.source, source, "the record must carry the stamped source, not 0");
		assert_eq!(
			record.deltas,
			vec![Delta::Set {
				key,
				bytes: new,
				pre: SetPre::Present(old),
			}]
		);
		assert_eq!(
			oracle.inner.read().last_unchecked,
			before,
			"a flow commit must not mark the oracle, otherwise every later checked commit reads its old values again"
		);
	}

	#[cfg(reifydb_assertions)]
	#[test]
	#[should_panic(expected = "commit_deferred_flow got a set with an unresolved pre")]
	fn a_deferred_flow_commit_with_an_unresolved_pre_never_reaches_the_lock() {
		// An unresolved pre would be read under the commit lock while every user commit waits.
		let oracle = create_test_oracle(0);
		let set = |pre| Delta::Set {
			key: create_test_key("unresolved"),
			bytes: EncodedBytes(CowVec::new(vec![3u8; 4])),
			pre,
		};
		let buf = deferred_flow_record(&CowVec::new(vec![set(SetPre::Absent)]));
		let _ = oracle.commit_deferred_flow(CowVec::new(vec![set(SetPre::Unknown)]), Some(buf), None);
	}
}
