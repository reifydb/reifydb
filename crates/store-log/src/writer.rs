// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	path::{Path, PathBuf},
	thread,
};

use reifydb_codec::log::{LogVersion, Position, record::Record};
use reifydb_runtime::{
	fatal::{
		is_armed,
		report::{FatalKind, FatalReport},
	},
	io::fs::{Create, Filesystem, Mkdir, Open, OpenMut, ReadDir, Rename, SyncData, SyncDir, Unlink},
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
		condvar::Condvar,
		mutex::{Mutex, MutexGuard},
	},
};
use reifydb_value::clock::ClockNow;

use crate::{
	error::{LogError, Result},
	partition::{Partition, log_name},
};

struct State<F: Filesystem, C: ClockNow> {
	partition: Partition<F, C>,
	written: Option<LogVersion>,
	durable: Option<LogVersion>,
	active: PathBuf,
	stopped: bool,
}

struct Inner<F: Filesystem, C: ClockNow> {
	fs: F,
	state: Mutex<State<F, C>>,
	progress: Condvar,
	work: Condvar,
	shutdown: AtomicBool,
}

pub struct Writer<F: Filesystem, C: ClockNow> {
	inner: Arc<Inner<F, C>>,
	syncer: Option<thread::JoinHandle<()>>,
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
	pub fn new(fs: F, partition: Partition<F, C>) -> Self {
		Self::build(fs, partition, true)
	}

	pub fn detached(fs: F, partition: Partition<F, C>) -> Self {
		Self::build(fs, partition, false)
	}

	fn build(fs: F, partition: Partition<F, C>, threaded: bool) -> Self {
		let active = active_of(&partition);
		let durable = partition.head();
		let inner = Arc::new(Inner {
			fs,
			state: Mutex::new(State {
				partition,
				written: durable,
				durable,
				active,
				stopped: false,
			}),
			progress: Condvar::new(),
			work: Condvar::new(),
			shutdown: AtomicBool::new(false),
		});
		let syncer = if threaded {
			spawn(Arc::clone(&inner))
		} else {
			None
		};
		Self {
			inner,
			syncer,
		}
	}

	pub fn commit(&self, record: &Record) -> Result<Position> {
		let position = {
			let mut state = self.inner.state.lock();
			if state.stopped {
				return Err(stopped_error(&state));
			}
			if let Some(head) = state.partition.head()
				&& record.version <= head
			{
				return Err(LogError::OutOfOrder {
					dir: state.partition.dir().to_path_buf(),
					head,
					found: record.version,
				});
			}
			let position = state.partition.append(record)?;
			state.written = Some(record.version);
			state.active = active_of(&state.partition);
			position
		};
		self.inner.work.notify_one();
		Ok(position)
	}

	pub fn commit_durable(&self, record: &Record) -> Result<Position> {
		let position = self.commit(record)?;
		self.wait(record.version)?;
		Ok(position)
	}

	pub fn wait(&self, version: LogVersion) -> Result<()> {
		let mut state = self.inner.state.lock();
		while state.durable.is_none_or(|found| found < version) {
			if state.stopped {
				return Err(stopped_error(&state));
			}
			self.inner.progress.wait(&mut state);
		}
		Ok(())
	}

	pub fn flush(&self) -> Result<()> {
		let (path, mark) = {
			let state = self.inner.state.lock();
			(state.active.clone(), state.written)
		};
		if let Err(error) = sync_path(&self.inner.fs, &path) {
			terminate(&self.inner, &path, error);
			return Err(stopped_error(&self.inner.state.lock()));
		}
		let mut state = self.inner.state.lock();
		state.durable = state.durable.max(mark);
		drop(state);
		self.inner.progress.notify_all();
		Ok(())
	}

	pub fn durable(&self) -> Option<LogVersion> {
		self.inner.state.lock().durable
	}

	pub fn written(&self) -> Option<LogVersion> {
		self.inner.state.lock().written
	}

	pub fn with<R>(&self, act: impl FnOnce(&mut Partition<F, C>) -> R) -> R {
		let mut state = self.inner.state.lock();
		act(&mut state.partition)
	}
}

impl<F: Filesystem, C: ClockNow> Drop for Writer<F, C> {
	fn drop(&mut self) {
		self.inner.shutdown.store(true, Ordering::Release);
		{
			let _guard = self.inner.state.lock();
			self.inner.work.notify_all();
		}
		if let Some(handle) = self.syncer.take() {
			let _ = handle.join();
		}
	}
}

#[cfg(not(any(reifydb_single_threaded, reifydb_dst)))]
fn spawn<F, C>(inner: Arc<Inner<F, C>>) -> Option<thread::JoinHandle<()>>
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
	Some(thread::Builder::new()
		.name("log-syncer".to_string())
		.spawn(move || syncer(inner))
		.unwrap_or_else(|_| panic!("failed to spawn the log syncer thread")))
}

#[cfg(any(reifydb_single_threaded, reifydb_dst))]
fn spawn<F, C>(_inner: Arc<Inner<F, C>>) -> Option<thread::JoinHandle<()>>
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
{
	None
}

#[cfg(not(any(reifydb_single_threaded, reifydb_dst)))]
fn syncer<F, C>(inner: Arc<Inner<F, C>>)
where
	F: Filesystem + Create + Mkdir + Open + OpenMut + ReadDir + Rename + SyncDir + Unlink,
	C: ClockNow,
{
	loop {
		let (path, mark) = {
			let mut state = inner.state.lock();
			while !inner.shutdown.load(Ordering::Acquire) && state.written <= state.durable {
				inner.work.wait(&mut state);
			}
			if state.written <= state.durable {
				return;
			}
			(state.active.clone(), state.written)
		};
		match sync_path(&inner.fs, &path) {
			Ok(()) => {
				let mut state = inner.state.lock();
				state.durable = state.durable.max(mark);
				drop(state);
				inner.progress.notify_all();
			}
			Err(error) => {
				terminate(&inner, &path, error);
				return;
			}
		}
	}
}

fn terminate<F: Filesystem, C: ClockNow>(inner: &Inner<F, C>, path: &Path, error: LogError) {
	let report = FatalReport::new(FatalKind::Error, format!("{error}"))
		.component("log syncer")
		.with("segment", path.display().to_string());
	if is_armed() {
		reifydb_runtime::fatal::fatal(report);
	}
	let mut state = inner.state.lock();
	state.stopped = true;
	drop(state);
	inner.progress.notify_all();
}

fn sync_path<F: Filesystem + OpenMut>(fs: &F, path: &Path) -> Result<()> {
	Ok(fs.open_mut(path)?.sync_data()?)
}

fn active_of<F, C>(partition: &Partition<F, C>) -> PathBuf
where
	F: Filesystem + Create + Mkdir + Open + OpenMut + ReadDir + Rename + SyncDir + Unlink,
	C: ClockNow,
{
	partition.dir().join(log_name(partition.base()))
}

fn stopped_error<F: Filesystem, C: ClockNow>(state: &MutexGuard<'_, State<F, C>>) -> LogError {
	LogError::Io {
		path: state.active.clone(),
		message: "the log syncer stopped after a failed flush".to_string(),
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::log::{LogIndex, RecordKind, Term};
	use reifydb_runtime::{
		context::clock::{Clock, MockClock},
		io::fs::memory::MemoryFs,
	};
	use reifydb_value::{
		byte_size::ByteSize,
		value::{datetime::DateTime, duration::Duration},
	};

	use super::*;
	use crate::partition::Config;

	const DIR: &str = "/log/p0";
	const BASE: LogVersion = LogVersion::new(500);

	fn record(offset: u64) -> Record {
		Record::new(
			LogVersion::new(BASE.as_u64() + offset),
			LogIndex::new(offset + 1),
			Term::new(1),
			DateTime::from_bits(1000 + offset),
			RecordKind::new(0),
			vec![0x10u8.wrapping_add(offset as u8); 40],
		)
	}

	fn writer() -> Writer<MemoryFs, Clock> {
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
		Writer::new(fs, partition)
	}

	#[test]
	fn a_durable_commit_returns_only_once_its_version_is_on_the_platter() {
		// this is the whole promise of the sync path: when the call returns, a crash cannot take
		// the record back. Returning before the flush would be an acknowledged write that is lost.
		let writer = writer();

		writer.commit_durable(&record(0)).unwrap();

		assert_eq!(writer.durable(), Some(BASE));
	}

	#[test]
	fn the_durable_watermark_never_passes_the_written_one() {
		// durable is what the barrier rule reads before letting a store flush derived state, so a
		// watermark that ran ahead of the writes would let the store persist data the log lost.
		let writer = writer();
		for offset in 0..8 {
			writer.commit(&record(offset)).unwrap();
			let (durable, written) = (writer.durable(), writer.written());
			assert!(durable <= written, "durable {durable:?} passed written {written:?}");
		}

		writer.wait(LogVersion::new(BASE.as_u64() + 7)).unwrap();

		assert_eq!(writer.durable(), writer.written());
	}

	#[test]
	fn an_async_commit_never_blocks_on_the_flush() {
		// the cheap path exists so a caller that does not need the guarantee never pays a device
		// round trip; if commit blocked until durable there would be no async path at all.
		let writer = writer();

		writer.commit(&record(0)).unwrap();

		assert_eq!(writer.written(), Some(BASE));
		writer.wait(BASE).unwrap();
	}

	#[test]
	fn a_durable_commit_is_covered_when_it_returns_even_under_contention() {
		// the syncer publishes the mark it read before the flush started, so a record appended
		// while that flush is in the air must not be counted by it. The appends are serialised by
		// the allocator, as the oracle serialises them in production, but the waits overlap, which
		// is the window where a mark published after the flush would acknowledge a lost write.
		let writer = Arc::new(writer());
		let next = Arc::new(Mutex::new(0u64));
		let handles: Vec<_> = (0..4)
			.map(|_| {
				let writer = Arc::clone(&writer);
				let next = Arc::clone(&next);
				thread::spawn(move || {
					loop {
						let taken = {
							let mut guard = next.lock();
							if *guard >= 32 {
								return;
							}
							let taken = *guard;
							*guard += 1;
							writer.commit(&record(taken)).unwrap();
							taken
						};
						let version = LogVersion::new(BASE.as_u64() + taken);
						writer.wait(version).unwrap();
						assert!(
							writer.durable().is_some_and(|found| found >= version),
							"wait returned before {} was durable",
							version.as_u64()
						);
					}
				})
			})
			.collect();

		for handle in handles {
			handle.join().unwrap();
		}

		assert_eq!(writer.durable(), Some(LogVersion::new(BASE.as_u64() + 31)));
	}

	#[test]
	fn a_version_at_or_below_the_head_is_refused() {
		// two threads committing to one partition can reach the append in either order, and a
		// version that does not increase is what the scanner treats as corruption: it truncates
		// the segment there and every record after it is gone. Refusing is the only safe answer,
		// because the log does not allocate versions and so cannot reorder them itself.
		let writer = writer();
		writer.commit(&record(3)).unwrap();

		let same = writer.commit(&record(3)).err().unwrap();
		let below = writer.commit(&record(1)).err().unwrap();

		assert!(matches!(same, LogError::OutOfOrder { .. }), "{same:?}");
		assert!(matches!(below, LogError::OutOfOrder { .. }), "{below:?}");
		assert_eq!(writer.written(), Some(LogVersion::new(BASE.as_u64() + 3)));
	}

	#[test]
	fn a_refused_commit_leaves_nothing_behind() {
		// the guard has to fire before the append, not after, or the bad record is already framed
		// on disk and the error is cosmetic.
		let writer = writer();
		writer.commit_durable(&record(3)).unwrap();
		let before = writer.with(|partition| partition.records().unwrap());

		writer.commit(&record(2)).err().unwrap();
		writer.flush().unwrap();

		assert_eq!(writer.with(|partition| partition.records().unwrap()), before);
		assert_eq!(writer.durable(), Some(LogVersion::new(BASE.as_u64() + 3)));
	}

	#[test]
	fn an_explicit_flush_advances_the_watermark() {
		// the single threaded build has no syncer thread, so flush is the only way an async commit
		// ever becomes durable there.
		let writer = writer();
		writer.commit(&record(0)).unwrap();

		writer.flush().unwrap();

		assert_eq!(writer.durable(), Some(BASE));
	}

	#[test]
	fn a_wait_for_a_version_that_is_already_durable_returns_at_once() {
		let writer = writer();
		writer.commit_durable(&record(0)).unwrap();

		writer.wait(BASE).unwrap();

		assert_eq!(writer.durable(), Some(BASE));
	}
}
