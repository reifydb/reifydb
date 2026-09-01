// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::path::{Path, PathBuf};

use reifydb_codec::log::{
	LogIndex, LogVersion, Position,
	meta::{FORMAT_VERSION, MAGIC, META_BYTES, Meta},
	record::Record,
};
use reifydb_runtime::io::fs::{
	Create, Filesystem, Len, Mkdir, Open, OpenMut, Pread, ReadDir, Rename, SyncData, SyncDir, Unlink,
};
use reifydb_value::{byte_size::ByteSize, clock::ClockNow, value::duration::Duration};

use crate::{
	cursor::Cursor,
	error::{LogError, Result},
	partition::{Config, Partition, sync},
	reader::{record, register, unregister, version_of},
	segment::{Scan, discard, staging, write_all},
	writer::Writer,
};

pub const META_NAME: &str = "meta";

pub struct Log<F: Filesystem, C: ClockNow> {
	fs: F,
	dir: PathBuf,
	meta: Meta,
	writers: Vec<Writer<F, C>>,
}

impl<F, C> Log<F, C>
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
	C: ClockNow + Clone + Send + 'static,
	Partition<F, C>: Send,
{
	pub fn create(fs: F, clock: C, dir: &Path, config: Config, partitions: u32) -> Result<Self> {
		Self::created(fs, clock, dir, config, partitions, true)
	}

	pub fn create_detached(fs: F, clock: C, dir: &Path, config: Config, partitions: u32) -> Result<Self> {
		Self::created(fs, clock, dir, config, partitions, false)
	}

	fn created(fs: F, clock: C, dir: &Path, config: Config, partitions: u32, threaded: bool) -> Result<Self> {
		if partitions == 0 {
			return Err(LogError::MetaCorrupt(dir.join(META_NAME)));
		}
		fs.mkdir(dir)?;
		let meta = Meta {
			version: FORMAT_VERSION,
			partitions,
			segment_bytes: config.segment_bytes,
			index_interval: config.index_interval,
			segment_age: config.segment_age,
		};
		publish(&fs, &dir.join(META_NAME), &meta)?;
		let writers = (0..partitions)
			.map(|at| {
				let partition = Partition::create(
					fs.clone(),
					clock.clone(),
					&dir.join(partition_name(at)),
					config,
					LogVersion::ZERO,
					LogIndex::ZERO,
				)?;
				Ok(writer(fs.clone(), partition, threaded))
			})
			.collect::<Result<Vec<_>>>()?;
		Ok(Self {
			fs,
			dir: dir.to_path_buf(),
			meta,
			writers,
		})
	}

	pub fn open(fs: F, clock: C, dir: &Path) -> Result<(Self, Vec<Scan>)> {
		Self::opened(fs, clock, dir, true)
	}

	pub fn open_detached(fs: F, clock: C, dir: &Path) -> Result<(Self, Vec<Scan>)> {
		Self::opened(fs, clock, dir, false)
	}

	fn opened(fs: F, clock: C, dir: &Path, threaded: bool) -> Result<(Self, Vec<Scan>)> {
		let meta = load(&fs, &dir.join(META_NAME))?;
		let config = Config {
			segment_bytes: meta.segment_bytes,
			segment_age: meta.segment_age,
			index_interval: meta.index_interval,
		};
		let mut writers = Vec::with_capacity(meta.partitions as usize);
		let mut scans = Vec::with_capacity(meta.partitions as usize);
		for at in 0..meta.partitions {
			let (partition, scan) =
				Partition::open(fs.clone(), clock.clone(), &dir.join(partition_name(at)), config)?;
			writers.push(writer(fs.clone(), partition, threaded));
			scans.push(scan);
		}
		let log = Self {
			fs,
			dir: dir.to_path_buf(),
			meta,
			writers,
		};
		Ok((log, scans))
	}

	pub fn append(&self, partition: u32, record: &Record) -> Result<Position> {
		self.writer(partition)?.commit(record)
	}

	pub fn append_durable(&self, partition: u32, record: &Record) -> Result<Position> {
		self.writer(partition)?.commit_durable(record)
	}

	pub fn truncate_from(&self, partition: u32, index: LogIndex) -> Result<()> {
		self.writer(partition)?.truncate_from(index)
	}

	pub fn wait(&self, partition: u32, version: LogVersion) -> Result<()> {
		self.writer(partition)?.wait(version)
	}

	pub fn durable(&self, partition: u32) -> Result<Option<LogVersion>> {
		Ok(self.writer(partition)?.durable())
	}

	pub fn sync(&self) -> Result<()> {
		for writer in &self.writers {
			let (dir, base) = writer.with(|partition| (partition.dir().to_path_buf(), partition.base()));
			sync(&self.fs, &dir, base)?;
		}
		Ok(())
	}

	pub fn purge(&self, ttl: Duration) -> Result<Vec<(u32, Vec<LogVersion>)>> {
		let mut dropped = Vec::with_capacity(self.writers.len());
		for (at, writer) in self.writers.iter().enumerate() {
			dropped.push((at as u32, writer.with(|partition| partition.purge(ttl))?));
		}
		Ok(dropped)
	}

	pub fn register(&self, partition: u32, id: &str) -> Result<()> {
		register(&self.fs, &self.dir_of(partition)?, id)
	}

	pub fn unregister(&self, partition: u32, id: &str) -> Result<()> {
		unregister(&self.fs, &self.dir_of(partition)?, id)
	}

	pub fn record(&self, partition: u32, id: &str, version: LogVersion) -> Result<()> {
		record(&self.fs, &self.dir_of(partition)?, id, version)
	}

	pub fn cursor(&self, partition: u32, id: &str) -> Result<Cursor<'_, F>> {
		let dir = self.dir_of(partition)?;
		let after = version_of(&self.fs, &dir, id)?;
		Cursor::open(&self.fs, &dir, after)
	}

	pub fn head(&self) -> Option<LogVersion> {
		self.writers.iter().filter_map(Writer::written).max()
	}

	pub fn meta(&self) -> Meta {
		self.meta
	}

	pub fn dir(&self) -> &Path {
		&self.dir
	}

	pub fn dir_of(&self, partition: u32) -> Result<PathBuf> {
		self.writer(partition)?;
		Ok(self.dir.join(partition_name(partition)))
	}

	pub fn base(&self, partition: u32) -> Result<LogVersion> {
		Ok(self.writer(partition)?.with(|partition| partition.base()))
	}

	pub fn bases(&self, partition: u32) -> Result<Vec<LogVersion>> {
		Ok(self.writer(partition)?.with(|partition| partition.bases().to_vec()))
	}

	pub fn segment_bytes(&self, partition: u32) -> Result<ByteSize> {
		Ok(self.writer(partition)?.with(|partition| partition.segment().capacity()))
	}

	pub fn with<R>(&self, partition: u32, act: impl FnOnce(&mut Partition<F, C>) -> R) -> Result<R> {
		Ok(self.writer(partition)?.with(act))
	}

	fn writer(&self, partition: u32) -> Result<&Writer<F, C>> {
		self.writers.get(partition as usize).ok_or_else(|| LogError::NoSuchPartition {
			dir: self.dir.clone(),
			requested: partition,
			count: self.meta.partitions,
		})
	}
}

fn writer<F, C>(fs: F, partition: Partition<F, C>, threaded: bool) -> Writer<F, C>
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
	if threaded {
		Writer::new(fs, partition)
	} else {
		Writer::detached(fs, partition)
	}
}

pub fn partition_name(at: u32) -> String {
	format!("p{at}")
}

fn publish<F: Filesystem + Create + Open + Rename + SyncDir + Unlink>(fs: &F, path: &Path, meta: &Meta) -> Result<()>
where
	F::FileMut: SyncData,
{
	if fs.open(path).is_ok() {
		return Err(LogError::AlreadyExists(path.to_path_buf()));
	}
	let staging = staging(path);
	discard(fs, &staging)?;
	let file = fs.create(&staging, META_BYTES as u64)?;
	write_all(&file, &staging, 0, &meta.encode())?;
	file.sync_data()?;
	fs.rename(&staging, path)?;
	fs.sync_dir(path.parent().expect("a meta file always sits inside a log directory"))?;
	Ok(())
}

fn load<F: Filesystem + Open>(fs: &F, path: &Path) -> Result<Meta> {
	let file = fs.open(path)?;
	let len = file.len()?;
	if len < META_BYTES as u64 {
		return Err(LogError::MetaShort {
			path: path.to_path_buf(),
			len,
		});
	}
	let mut buf = [0u8; META_BYTES];
	let read = file.pread(0, &mut buf)?;
	if read < META_BYTES {
		return Err(LogError::MetaShort {
			path: path.to_path_buf(),
			len: read as u64,
		});
	}
	let found = u32::from_le_bytes(buf[0..4].try_into().unwrap());
	if found != MAGIC {
		return Err(LogError::MetaMagic {
			path: path.to_path_buf(),
			found,
		});
	}
	let meta = Meta::decode(&buf).ok_or_else(|| LogError::MetaCorrupt(path.to_path_buf()))?;
	if meta.version != FORMAT_VERSION {
		return Err(LogError::MetaVersion {
			path: path.to_path_buf(),
			found: meta.version,
			expected: FORMAT_VERSION,
		});
	}
	if meta.partitions == 0 {
		return Err(LogError::MetaCorrupt(path.to_path_buf()));
	}
	Ok(meta)
}

#[cfg(test)]
mod tests {
	use reifydb_codec::log::{RecordKind, Term, meta::DEFAULT_PARTITIONS};
	use reifydb_runtime::{
		context::clock::{Clock, MockClock},
		io::fs::{Pwrite, memory::MemoryFs},
	};
	use reifydb_value::{
		byte_size::ByteSize,
		value::{datetime::DateTime, duration::Duration},
	};

	use super::*;
	use crate::partition::drain;

	const DIR: &str = "/log";

	fn config() -> Config {
		Config {
			segment_bytes: ByteSize::from_bytes(512),
			segment_age: Duration::from_seconds_const(60),
			index_interval: ByteSize::from_bytes(64),
		}
	}

	fn record(version: u64, index: u64) -> Record {
		Record::new(
			LogVersion::new(version),
			LogIndex::new(index),
			Term::new(1),
			DateTime::from_bits(1000 + version),
			RecordKind::new(0),
			vec![0xab; 20],
		)
	}

	fn clock() -> Clock {
		Clock::Mock(MockClock::from_millis(1_000))
	}

	fn fixture() -> (MemoryFs, Log<MemoryFs, Clock>) {
		let fs = MemoryFs::new();
		let log = Log::create(fs.clone(), clock(), Path::new(DIR), config(), 2).unwrap();
		(fs, log)
	}

	fn versions(log: &Log<MemoryFs, Clock>, partition: u32) -> Vec<u64> {
		log.with(partition, |partition| drain(partition))
			.unwrap()
			.unwrap()
			.iter()
			.map(|r| r.version.as_u64())
			.collect()
	}

	#[test]
	fn a_created_log_writes_its_meta_and_one_directory_per_partition() {
		let (fs, log) = fixture();

		assert_eq!(log.meta().partitions, 2);
		assert_eq!(load(&fs, Path::new("/log/meta")).unwrap(), log.meta());
		assert!(fs.open(Path::new("/log/p0")).is_ok() || fs.read_dir(Path::new("/log/p0")).is_ok());
		assert!(fs.read_dir(Path::new("/log/p1")).is_ok());
	}

	#[test]
	fn meta_config_survives_a_reopen_and_drives_the_partitions() {
		// Decision 223: the segment config lives in meta and meta wins on open, so a segment
		// size cannot drift between boots and silently move every roll boundary.
		let (fs, log) = fixture();
		log.append(0, &record(1, 1)).unwrap();
		log.sync().unwrap();
		drop(log);

		let (reopened, _) = Log::open(fs, clock(), Path::new(DIR)).unwrap();

		assert_eq!(reopened.meta().segment_bytes, ByteSize::from_bytes(512));
		assert_eq!(reopened.meta().index_interval, ByteSize::from_bytes(64));
		assert_eq!(reopened.segment_bytes(0).unwrap(), ByteSize::from_bytes(512));
	}

	#[test]
	fn opening_is_a_fixed_point() {
		// Invariant I7. A second open that moves anything means the first left the directory in a
		// state it does not itself accept, which turns every restart into a slow leak.
		let (fs, log) = fixture();
		log.append(0, &record(10, 1)).unwrap();
		log.append(1, &record(20, 1)).unwrap();
		log.append(1, &record(30, 2)).unwrap();
		log.sync().unwrap();
		drop(log);

		let (once, _) = Log::open(fs.clone(), clock(), Path::new(DIR)).unwrap();
		let after_once = (versions(&once, 0), versions(&once, 1), once.head());
		drop(once);
		let (twice, _) = Log::open(fs, clock(), Path::new(DIR)).unwrap();

		assert_eq!((versions(&twice, 0), versions(&twice, 1), twice.head()), after_once);
	}

	#[test]
	fn an_idle_partition_costs_the_others_nothing() {
		// Decision 230 is exactly this case: p1 is written once and never again, and under a
		// minimum taken across partitions that pinned the whole log to version 1 and destroyed
		// every record p0 had been told was durable.
		let (fs, log) = fixture();
		log.append(1, &record(1, 1)).unwrap();
		for version in 2..=8u64 {
			log.append(0, &record(version, version)).unwrap();
		}
		log.sync().unwrap();
		drop(log);

		let (reopened, _) = Log::open(fs, clock(), Path::new(DIR)).unwrap();

		assert_eq!(versions(&reopened, 0), vec![2, 3, 4, 5, 6, 7, 8]);
		assert_eq!(versions(&reopened, 1), vec![1]);
	}

	#[test]
	fn the_head_is_the_highest_version_durable_anywhere() {
		// The allocator is seeded from this, so it has to sit above every version on the platter
		// or a restart reissues one that already names a different commit.
		let (fs, log) = fixture();
		log.append(0, &record(10, 1)).unwrap();
		log.append(1, &record(40, 1)).unwrap();
		log.append(0, &record(20, 2)).unwrap();
		log.sync().unwrap();
		drop(log);

		let (reopened, _) = Log::open(fs, clock(), Path::new(DIR)).unwrap();

		assert_eq!(reopened.head(), Some(LogVersion::new(40)));
	}

	#[test]
	fn a_commit_may_half_survive_across_partitions() {
		// Decision 230 gives this up deliberately: p1 kept commit 30 and p0 never got it, and
		// nothing puts that back together. No reader before stage 5 reads across partitions,
		// and the alternative pinned the log to whichever partition was idle.
		let (fs, log) = fixture();
		log.append(0, &record(10, 1)).unwrap();
		log.append(1, &record(30, 1)).unwrap();
		log.sync().unwrap();
		drop(log);

		let (reopened, _) = Log::open(fs, clock(), Path::new(DIR)).unwrap();

		assert_eq!(versions(&reopened, 0), vec![10]);
		assert_eq!(versions(&reopened, 1), vec![30]);
	}

	#[test]
	fn a_meta_with_a_foreign_magic_is_refused() {
		let (fs, log) = fixture();
		drop(log);
		let file = fs.open_mut(Path::new("/log/meta")).unwrap();
		file.pwrite(0, &0u32.to_le_bytes()).unwrap();
		file.sync_data().unwrap();

		let opened = Log::open(fs, clock(), Path::new(DIR)).err();

		assert!(matches!(opened, Some(LogError::MetaMagic { .. })), "got {opened:?}");
	}

	#[test]
	fn a_meta_with_a_flipped_bit_is_refused_rather_than_opened_on_the_wrong_partition_count() {
		let (fs, log) = fixture();
		drop(log);
		let file = fs.open_mut(Path::new("/log/meta")).unwrap();
		let mut buf = [0u8; META_BYTES];
		file.pread(0, &mut buf).unwrap();
		buf[12] ^= 0x01;
		file.pwrite(0, &buf).unwrap();
		file.sync_data().unwrap();

		let opened = Log::open(fs, clock(), Path::new(DIR)).err();

		assert!(matches!(opened, Some(LogError::MetaCorrupt(_))), "got {opened:?}");
	}

	#[test]
	fn a_partition_beyond_the_count_is_named_in_the_error() {
		let (_fs, log) = fixture();

		let appended = log.append(DEFAULT_PARTITIONS, &record(1, 1)).err();

		assert!(
			matches!(
				appended,
				Some(LogError::NoSuchPartition {
					count: 2,
					requested: 4,
					..
				})
			),
			"got {appended:?}"
		);
	}
}
