// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::path::{Path, PathBuf};

use reifydb_codec::log::{LogIndex, LogVersion, Position, Term, record::Record};
use reifydb_runtime::{
	fatal::{
		fatal, is_armed,
		report::{FatalKind, FatalReport},
	},
	io::fs::{Create, Filesystem, Mkdir, Open, OpenMut, ReadDir, Rename, SyncDir, Unlink},
	sync::{
		Arc,
		mutex::{Mutex, MutexGuard},
	},
};
use reifydb_value::clock::ClockNow;

use crate::{
	error::StoreLogError,
	partition::{Partition, log_name},
	segment::sync_path,
};

struct State<F: Filesystem, C: ClockNow> {
	partition: Partition<F, C>,
	written: Option<LogVersion>,
	durable: Option<LogVersion>,
	active: PathBuf,
	stopped: bool,
	writes: u64,
	synced: u64,
}

struct Inner<F: Filesystem, C: ClockNow> {
	fs: F,
	state: Mutex<State<F, C>>,
}

pub struct Writer<F: Filesystem, C: ClockNow> {
	inner: Arc<Inner<F, C>>,
}

impl<F, C> Writer<F, C>
where
	F: Filesystem
		+ Create
		+ Mkdir
		+ Open
		+ OpenMut
		+ ReadDir
		+ Rename
		+ SyncDir
		+ Unlink
		+ Clone
		+ Send
		+ Sync
		+ 'static,
	C: ClockNow + Send + 'static,
	Partition<F, C>: Send,
{
	pub fn detached(fs: F, partition: Partition<F, C>) -> Self {
		let active = active_of(&partition);
		let durable = partition.head();
		Self {
			inner: Arc::new(Inner {
				fs,
				state: Mutex::new(State {
					partition,
					written: durable,
					durable,
					active,
					stopped: false,
					writes: 1,
					synced: 0,
				}),
			}),
		}
	}

	pub fn commit(&self, record: &Record) -> Result<Position, StoreLogError> {
		let mut state = self.inner.state.lock();
		if state.stopped {
			return Err(stopped_error(&state));
		}
		if let Some(head) = state.partition.head()
			&& record.version <= head
		{
			return Err(StoreLogError::OutOfOrder {
				dir: state.partition.dir().to_path_buf(),
				head,
				found: record.version,
			});
		}
		state.writes += 1;
		let position = state.partition.append(record)?;
		state.written = Some(record.version);
		state.active = active_of(&state.partition);
		Ok(position)
	}

	pub fn flush(&self) -> Result<(), StoreLogError> {
		let (path, mark, writes) = {
			let state = self.inner.state.lock();
			if state.synced == state.writes && !state.stopped {
				return Ok(());
			}
			(state.active.clone(), state.written, state.writes)
		};
		if let Err(error) = sync_path(&self.inner.fs, &path) {
			terminate(&self.inner, &path, error);
			return Err(stopped_error(&self.inner.state.lock()));
		}
		let mut state = self.inner.state.lock();
		state.synced = state.synced.max(writes);
		state.durable = state.durable.max(mark);
		Ok(())
	}

	pub fn durable(&self) -> Option<LogVersion> {
		self.inner.state.lock().durable
	}

	pub fn written(&self) -> Option<LogVersion> {
		self.inner.state.lock().written
	}

	pub fn truncate_from(&self, index: LogIndex) -> Result<(), StoreLogError> {
		self.cut(|partition| partition.truncate_from(index))
	}

	pub fn rebase(&self, index: LogIndex, term: Term) -> Result<(), StoreLogError> {
		self.cut(|partition| partition.rebase(index, term))
	}

	fn cut(
		&self,
		act: impl FnOnce(&mut Partition<F, C>) -> Result<(), StoreLogError>,
	) -> Result<(), StoreLogError> {
		let mut state = self.inner.state.lock();
		if state.stopped {
			return Err(stopped_error(&state));
		}
		state.writes += 1;
		act(&mut state.partition)?;
		let head = state.partition.head();
		state.written = head;
		state.durable = state.durable.min(head);
		state.active = active_of(&state.partition);
		Ok(())
	}

	pub fn read<R>(&self, act: impl FnOnce(&Partition<F, C>) -> R) -> R {
		let state = self.inner.state.lock();
		act(&state.partition)
	}

	pub fn maintain<R>(&self, act: impl FnOnce(&mut Partition<F, C>) -> R) -> R {
		let mut state = self.inner.state.lock();
		act(&mut state.partition)
	}
}

fn terminate<F: Filesystem, C: ClockNow>(inner: &Inner<F, C>, path: &Path, error: StoreLogError) {
	let report = FatalReport::new(FatalKind::Error, format!("{error}"))
		.component("log writer")
		.with("segment", path.display().to_string());
	if is_armed() {
		fatal(report);
	}
	inner.state.lock().stopped = true;
}

fn active_of<F, C>(partition: &Partition<F, C>) -> PathBuf
where
	F: Filesystem + Create + Mkdir + Open + OpenMut + ReadDir + Rename + SyncDir + Unlink,
	C: ClockNow,
{
	partition.dir().join(log_name(partition.base()))
}

fn stopped_error<F: Filesystem, C: ClockNow>(state: &MutexGuard<'_, State<F, C>>) -> StoreLogError {
	StoreLogError::Io {
		path: state.active.clone(),
		message: "the log writer stopped after a failed flush".to_string(),
	}
}

#[cfg(test)]
mod tests {
	use std::{
		panic::{AssertUnwindSafe, catch_unwind},
		sync::atomic::{AtomicBool, Ordering},
	};

	use reifydb_codec::log::{LogIndex, RecordKind, Term};
	use reifydb_runtime::{
		context::clock::{Clock, MockClock},
		io::fs::{
			Pwrite,
			memory::{MemoryFs, SectorState},
			testing::{FileId, SyncOutcome, TestingFs, TestingHooks},
		},
	};
	use reifydb_value::{
		byte_size::ByteSize,
		value::{datetime::DateTime, duration::Duration},
	};

	use super::*;
	use crate::partition::{Config, drain};

	const DIR: &str = "/log/p0";
	const BASE: LogVersion = LogVersion::new(500);

	fn record(offset: u64) -> Record {
		Record::new(
			LogVersion::new(BASE.as_u64() + offset),
			LogIndex::new(offset + 1),
			Term::new(1),
			DateTime::from_nanos(
				i64::try_from(1000 + offset).expect("test record timestamp fits in i64 nanos"),
			),
			RecordKind::new(0),
			vec![0x10u8.wrapping_add(offset as u8); 40],
		)
	}

	fn detached() -> Writer<MemoryFs, Clock> {
		let fs = MemoryFs::new();
		fs.mkdir(Path::new("/log")).unwrap();
		let partition = Partition::create(
			fs.clone(),
			Clock::Mock(MockClock::from_millis(1_000)),
			Path::new(DIR),
			Config {
				segment_bytes: ByteSize::from_bytes(4096),
				segment_age: Duration::from_seconds_const(60),
				index_interval: ByteSize::from_bytes(64),
			},
			BASE,
			LogIndex::new(1),
		)
		.unwrap();
		Writer::detached(fs, partition)
	}

	#[test]
	fn the_durable_watermark_never_passes_the_written_one() {
		// durable is what the barrier rule reads before letting a store flush derived state, so a
		// watermark that ran ahead of the writes would let the store persist data the log lost.
		let writer = detached();
		for offset in 0..8 {
			writer.commit(&record(offset)).unwrap();
			let (durable, written) = (writer.durable(), writer.written());
			assert!(durable <= written, "durable {durable:?} passed written {written:?}");
		}

		writer.flush().unwrap();

		assert_eq!(writer.durable(), writer.written());
	}

	#[test]
	fn an_async_commit_never_blocks_on_the_flush() {
		// the cheap path exists so a caller that does not need the guarantee never pays a device
		// round trip; if commit blocked until durable there would be no async path at all.
		let writer = detached();

		writer.commit(&record(0)).unwrap();

		assert_eq!(writer.written(), Some(BASE));
		assert_eq!(writer.durable(), None);
	}

	#[test]
	fn a_version_at_or_below_the_head_is_refused() {
		// two threads committing to one partition can reach the append in either order, and a
		// version that does not increase is what the scanner treats as corruption: it truncates
		// the segment there and every record after it is gone. Refusing is the only safe answer,
		// because the log does not allocate versions and so cannot reorder them itself.
		let writer = detached();
		writer.commit(&record(3)).unwrap();

		let same = writer.commit(&record(3)).err().unwrap();
		let below = writer.commit(&record(1)).err().unwrap();

		assert!(matches!(same, StoreLogError::OutOfOrder { .. }), "{same:?}");
		assert!(matches!(below, StoreLogError::OutOfOrder { .. }), "{below:?}");
		assert_eq!(writer.written(), Some(LogVersion::new(BASE.as_u64() + 3)));
	}

	#[test]
	fn a_refused_commit_leaves_nothing_behind() {
		// the guard has to fire before the append, not after, or the bad record is already framed
		// on disk and the error is cosmetic.
		let writer = detached();
		writer.commit(&record(3)).unwrap();
		writer.flush().unwrap();
		let before = writer.read(|partition| drain(partition).unwrap());

		writer.commit(&record(2)).err().unwrap();
		writer.flush().unwrap();

		assert_eq!(writer.read(|partition| drain(partition).unwrap()), before);
		assert_eq!(writer.durable(), Some(LogVersion::new(BASE.as_u64() + 3)));
	}

	#[test]
	fn an_explicit_flush_advances_the_watermark() {
		// flush is the only way a commit ever becomes durable.
		let writer = detached();
		writer.commit(&record(0)).unwrap();

		writer.flush().unwrap();

		assert_eq!(writer.durable(), Some(BASE));
	}

	#[test]
	fn a_rebase_resets_the_watermarks_like_a_cut() {
		// a rebase empties the partition, so both marks must fall or a vanished record reads as durable.
		let writer = detached();
		writer.commit(&record(0)).unwrap();
		writer.flush().unwrap();
		assert_eq!(writer.durable(), Some(BASE));

		writer.rebase(LogIndex::new(20), Term::new(5)).unwrap();

		assert_eq!(writer.durable(), None);
		assert_eq!(writer.written(), None);
		assert_eq!(writer.read(|partition| partition.last_index()), Some(LogIndex::new(20)));
	}

	fn mounted() -> (MemoryFs, Writer<MemoryFs, Clock>) {
		let fs = MemoryFs::new();
		fs.mkdir(Path::new("/log")).unwrap();
		let partition = Partition::create(
			fs.clone(),
			Clock::Mock(MockClock::from_millis(1_000)),
			Path::new(DIR),
			Config {
				segment_bytes: ByteSize::from_bytes(4096),
				segment_age: Duration::from_seconds_const(60),
				index_interval: ByteSize::from_bytes(64),
			},
			BASE,
			LogIndex::new(1),
		)
		.unwrap();
		(fs.clone(), Writer::detached(fs, partition))
	}

	fn unsynced_sectors(fs: &MemoryFs) -> usize {
		let file = fs.open_mut(&Path::new(DIR).join(log_name(BASE))).unwrap();
		file.sector_states().iter().filter(|state| **state == SectorState::Dirty).count()
	}

	fn scribble(fs: &MemoryFs) {
		let file = fs.open_mut(&Path::new(DIR).join(log_name(BASE))).unwrap();
		file.pwrite(2048, &[0xEE]).unwrap();
		assert_eq!(unsynced_sectors(fs), 1);
	}

	#[test]
	fn a_flush_with_nothing_written_since_the_last_one_never_touches_the_device() {
		// an idle tick must not pay a device sync, so bytes that bypassed the writer stay unsynced.
		let (fs, writer) = mounted();
		writer.commit(&record(0)).unwrap();
		writer.flush().unwrap();
		scribble(&fs);

		writer.flush().unwrap();

		assert_eq!(unsynced_sectors(&fs), 1);
		assert_eq!(writer.durable(), Some(BASE));
	}

	#[test]
	fn the_first_flush_after_open_syncs_even_with_no_commit() {
		// bytes recovered at open may never have reached the device, so equal watermarks must not skip the
		// sync.
		let (fs, writer) = mounted();
		scribble(&fs);

		writer.flush().unwrap();

		assert_eq!(unsynced_sectors(&fs), 0);
	}

	#[test]
	fn a_commit_after_a_flush_is_synced_by_the_next_flush() {
		// the skip must never outlive a write, or a committed record is reported durable while still in memory.
		let (fs, writer) = mounted();
		writer.commit(&record(0)).unwrap();
		writer.flush().unwrap();

		writer.commit(&record(1)).unwrap();
		assert!(unsynced_sectors(&fs) > 0);
		writer.flush().unwrap();

		assert_eq!(unsynced_sectors(&fs), 0);
		assert_eq!(writer.durable(), Some(LogVersion::new(BASE.as_u64() + 1)));
	}

	#[test]
	fn a_flush_after_a_truncate_syncs_although_the_watermarks_are_equal() {
		// a cut rewrites the segment while lowering both marks to the head, so the marks alone cannot prove it
		// clean.
		let (fs, writer) = mounted();
		for offset in 0..3 {
			writer.commit(&record(offset)).unwrap();
		}
		writer.flush().unwrap();
		writer.truncate_from(LogIndex::new(3)).unwrap();
		assert_eq!(writer.durable(), writer.written());
		scribble(&fs);

		writer.flush().unwrap();

		assert_eq!(unsynced_sectors(&fs), 0);
	}

	struct SyncDiesWhenArmed(AtomicBool);

	impl TestingHooks for SyncDiesWhenArmed {
		fn on_sync(&self, _file: FileId) -> SyncOutcome {
			if self.0.swap(false, Ordering::SeqCst) {
				panic!("the sync died before it returned");
			}
			SyncOutcome::Honest
		}
	}

	#[test]
	fn a_sync_that_never_finishes_leaves_the_writer_unsynced() {
		// a flush that died mid-sync proved nothing, so the next one must reach the device or a checkpoint
		// trusts lost bytes.
		let hooks = Arc::new(SyncDiesWhenArmed(AtomicBool::new(false)));
		let fs = TestingFs::new(MemoryFs::new(), hooks.clone());
		fs.mkdir(Path::new("/log")).unwrap();
		let partition = Partition::create(
			fs.clone(),
			Clock::Mock(MockClock::from_millis(1_000)),
			Path::new(DIR),
			Config {
				segment_bytes: ByteSize::from_bytes(4096),
				segment_age: Duration::from_seconds_const(60),
				index_interval: ByteSize::from_bytes(64),
			},
			BASE,
			LogIndex::new(1),
		)
		.unwrap();
		let writer = Writer::detached(fs.clone(), partition);
		writer.commit(&record(0)).unwrap();
		hooks.0.store(true, Ordering::SeqCst);
		assert!(catch_unwind(AssertUnwindSafe(|| writer.flush())).is_err());
		assert_eq!(writer.durable(), None);
		let before = fs.calls();

		writer.flush().unwrap();

		assert!(fs.calls() > before, "the flush after a dead sync never reached the device");
		assert_eq!(writer.durable(), Some(BASE));
	}
}
