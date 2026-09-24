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
	error::{LogError, Result},
	partition::{Partition, log_name},
	segment::sync_path,
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
				}),
			}),
		}
	}

	pub fn commit(&self, record: &Record) -> Result<Position> {
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
		Ok(position)
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
		Ok(())
	}

	pub fn durable(&self) -> Option<LogVersion> {
		self.inner.state.lock().durable
	}

	pub fn written(&self) -> Option<LogVersion> {
		self.inner.state.lock().written
	}

	pub fn truncate_from(&self, index: LogIndex) -> Result<()> {
		self.cut(|partition| partition.truncate_from(index))
	}

	pub fn rebase(&self, index: LogIndex, term: Term) -> Result<()> {
		self.cut(|partition| partition.rebase(index, term))
	}

	fn cut(&self, act: impl FnOnce(&mut Partition<F, C>) -> Result<()>) -> Result<()> {
		let mut state = self.inner.state.lock();
		if state.stopped {
			return Err(stopped_error(&state));
		}
		act(&mut state.partition)?;
		let head = state.partition.head();
		state.written = head;
		state.durable = head;
		state.active = active_of(&state.partition);
		Ok(())
	}

	pub fn with<R>(&self, act: impl FnOnce(&mut Partition<F, C>) -> R) -> R {
		let mut state = self.inner.state.lock();
		act(&mut state.partition)
	}
}

fn terminate<F: Filesystem, C: ClockNow>(inner: &Inner<F, C>, path: &Path, error: LogError) {
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

fn stopped_error<F: Filesystem, C: ClockNow>(state: &MutexGuard<'_, State<F, C>>) -> LogError {
	LogError::Io {
		path: state.active.clone(),
		message: "the log writer stopped after a failed flush".to_string(),
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

		assert!(matches!(same, LogError::OutOfOrder { .. }), "{same:?}");
		assert!(matches!(below, LogError::OutOfOrder { .. }), "{below:?}");
		assert_eq!(writer.written(), Some(LogVersion::new(BASE.as_u64() + 3)));
	}

	#[test]
	fn a_refused_commit_leaves_nothing_behind() {
		// the guard has to fire before the append, not after, or the bad record is already framed
		// on disk and the error is cosmetic.
		let writer = detached();
		writer.commit(&record(3)).unwrap();
		writer.flush().unwrap();
		let before = writer.with(|partition| drain(partition).unwrap());

		writer.commit(&record(2)).err().unwrap();
		writer.flush().unwrap();

		assert_eq!(writer.with(|partition| drain(partition).unwrap()), before);
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
		assert_eq!(writer.with(|partition| partition.last_index()), Some(LogIndex::new(20)));
	}
}
