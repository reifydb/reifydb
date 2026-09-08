// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashSet,
	path::{Path, PathBuf},
};

use reifydb_codec::log::{
	LogIndex, LogVersion, Position, Term,
	index::{DEFAULT_INTERVAL, TimestampRange},
	record::Record,
	vote::State,
};
use reifydb_runtime::io::fs::{Create, Filesystem, FsError, Mkdir, Open, OpenMut, ReadDir, Rename, SyncDir, Unlink};
use reifydb_value::{
	byte_size::ByteSize,
	clock::ClockNow,
	value::{datetime::DateTime, duration::Duration},
};

#[cfg(test)]
use crate::cursor::drain as drain_cursor;
use crate::{
	cursor::Cursor,
	error::{LogError, Result},
	index::{Index, find_at, header, read},
	reader::{clamp, floor, record, register, unregister, version_of},
	segment::{STAGING_SUFFIX, Scan, Segment, scan, sync_path},
	vote::Vote,
};

pub const NAME_DIGITS: usize = 20;
pub const LOG_SUFFIX: &str = ".log";
pub const INDEX_SUFFIX: &str = ".index";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
	pub segment_bytes: ByteSize,
	pub segment_age: Duration,
	pub index_interval: ByteSize,
}

impl Default for Config {
	fn default() -> Self {
		Self {
			segment_bytes: ByteSize::from_mib(256),
			segment_age: Duration::from_seconds_const(60),
			index_interval: DEFAULT_INTERVAL,
		}
	}
}

pub struct Partition<F: Filesystem, C: ClockNow> {
	fs: F,
	clock: C,
	dir: PathBuf,
	config: Config,
	bases: Vec<LogVersion>,
	base_indexes: Vec<LogIndex>,
	segment: Segment<F>,
	index: Index<F>,
	vote: Vote<F>,
	timestamps: Option<TimestampRange>,
	head: Option<LogVersion>,
	last: Option<(LogIndex, Term)>,
	opened_at: DateTime,
}

impl<F: Filesystem + Create + Mkdir + Open + OpenMut + ReadDir + Rename + SyncDir + Unlink, C: ClockNow>
	Partition<F, C>
{
	pub fn create(
		fs: F,
		clock: C,
		dir: &Path,
		config: Config,
		base: LogVersion,
		base_index: LogIndex,
	) -> Result<Self> {
		fs.mkdir(dir)?;
		let opened_at = clock.now();
		let vote = Vote::create(&fs, dir)?;
		let (segment, index) = new_pair(&fs, dir, config, base, base_index)?;
		Ok(Self {
			fs,
			clock,
			dir: dir.to_path_buf(),
			config,
			bases: vec![base],
			base_indexes: vec![base_index],
			segment,
			index,
			vote,
			timestamps: None,
			head: None,
			last: None,
			opened_at,
		})
	}

	pub fn open(fs: F, clock: C, dir: &Path, config: Config) -> Result<(Self, Scan)> {
		sweep(&fs, dir)?;
		let mut bases = bases_in(&fs, dir)?;
		let opened_at = clock.now();
		loop {
			let Some(base) = bases.last().copied() else {
				return Err(LogError::NotFound(dir.to_path_buf()));
			};
			let (segment, scan) = Segment::recover(&fs, &dir.join(log_name(base)))?;
			let index = match Index::recover(&fs, &dir.join(index_name(base)), config.index_interval, &scan)
			{
				Ok((index, _)) => index,
				Err(error) if !rebuildable(&error) => return Err(error),
				Err(_) if scan.records.is_empty() => {
					drop(segment);
					discard(&fs, dir, base)?;
					bases.pop();
					continue;
				}
				Err(_) => rebuild(&fs, dir, config, base, &scan)?,
			};
			let timestamps = scan.records.iter().fold(None, |range, record| widen(range, record.timestamp));
			let tail = last_of(&fs, dir, &bases, scan.records.last())?;
			let head = tail.map(|(version, _, _)| version);
			let last = tail.map(|(_, index, term)| (index, term));
			let vote = Vote::open(&fs, dir)?;
			let base_indexes = base_indexes_of(&fs, dir, &bases, index.header().base_index)?;
			let mut partition = Self {
				fs,
				clock,
				dir: dir.to_path_buf(),
				config,
				bases,
				base_indexes,
				segment,
				index,
				vote,
				timestamps,
				head,
				last,
				opened_at,
			};
			if partition.behind_snapshot() {
				let scan = partition.empty_to(after(partition.vote.state().snapshot_index))?;
				return Ok((partition, scan));
			}
			return Ok((partition, scan));
		}
	}

	pub fn append(&mut self, record: &Record) -> Result<Position> {
		if let Some((index, _)) = self.tail()
			&& record.index != after(index)
		{
			return Err(LogError::IndexGap {
				dir: self.dir.clone(),
				expected: after(index),
				found: record.index,
			});
		}
		if let Some((_, term)) = self.tail()
			&& record.term < term
		{
			return Err(LogError::TermRegression {
				dir: self.dir.clone(),
				last: term,
				found: record.term,
			});
		}
		if self.must_roll(record) {
			self.roll(record.version, record.index)?;
		}
		let position = self.segment.append(record)?;
		self.index.append(record.version, record.index, position)?;
		self.timestamps = widen(self.timestamps, record.timestamp);
		self.head = Some(record.version);
		self.last = Some((record.index, record.term));
		Ok(position)
	}

	pub fn truncate_from(&mut self, index: LogIndex) -> Result<()> {
		let commit = self.commit_index();
		if index <= commit {
			return Err(LogError::TruncateCommitted {
				dir: self.dir.clone(),
				commit,
				found: index,
			});
		}
		if self.last_index().is_none_or(|last| index > last) {
			return Ok(());
		}
		let at = self.base_indexes.partition_point(|base| *base < index).saturating_sub(1);
		for base in self.bases[at + 1..].iter().rev() {
			remove(&self.fs, &self.dir.join(log_name(*base)))?;
			remove(&self.fs, &self.dir.join(index_name(*base)))?;
		}
		self.bases.truncate(at + 1);
		self.base_indexes.truncate(at + 1);
		let base = self.bases[at];
		let log = self.dir.join(log_name(base));
		let entries = self.dir.join(index_name(base));
		let (mut segment, scanned) = Segment::recover(&self.fs, &log)?;
		let mut cut = Position::ZERO;
		for record in &scanned.records {
			if record.index >= index {
				break;
			}
			cut = cut.advance(record.encoded_len() as u64);
		}
		if cut == Position::ZERO {
			segment.reset(self.config.segment_bytes)?;
			self.opened_at = self.clock.now();
		} else {
			segment.truncate_to(cut)?;
		}
		let kept = scan(&self.fs, &log)?;
		let (rebuilt, _) = Index::recover(&self.fs, &entries, self.config.index_interval, &kept)?;
		let kept = kept.records;
		self.fs.sync_dir(&self.dir)?;
		self.segment = segment;
		self.index = rebuilt;
		self.timestamps = kept.iter().fold(None, |range, record| widen(range, record.timestamp));
		self.head = kept.last().map(|record| record.version);
		self.last = kept.last().map(|record| (record.index, record.term));
		clamp(&self.fs, &self.dir, self.head.unwrap_or(LogVersion::ZERO))?;
		Ok(())
	}

	pub fn seal(&mut self) -> Result<()> {
		self.index.seal(self.timestamps)?;
		self.segment.seal()
	}

	pub fn purge(&mut self, ttl: Duration) -> Result<Vec<LogVersion>> {
		let Some(deadline) = self.clock.now().checked_sub(ttl) else {
			return Ok(Vec::new());
		};
		let pinned = floor(&self.fs, &self.dir)?;
		let snapshot = self.vote.state().snapshot_index;
		let mut dropped = Vec::new();
		while self.bases.len() > 1 {
			let base = self.bases[0];
			if self.base_indexes[1] > after(snapshot) {
				break;
			}
			if !expired(&self.fs, &self.dir, base, deadline)? {
				break;
			}
			if pinned.is_some_and(|low| self.bases[1] > low) {
				break;
			}
			remove(&self.fs, &self.dir.join(log_name(base)))?;
			remove(&self.fs, &self.dir.join(index_name(base)))?;
			self.bases.remove(0);
			self.base_indexes.remove(0);
			dropped.push(base);
		}
		if !dropped.is_empty() {
			self.fs.sync_dir(&self.dir)?;
		}
		Ok(dropped)
	}

	pub fn register(&self, id: &str) -> Result<()> {
		register(&self.fs, &self.dir, id)
	}

	pub fn unregister(&self, id: &str) -> Result<()> {
		unregister(&self.fs, &self.dir, id)
	}

	pub fn record(&self, id: &str, version: LogVersion) -> Result<()> {
		record(&self.fs, &self.dir, id, version)
	}

	pub fn cursor(&self, id: &str) -> Result<Cursor<'_, F>> {
		Cursor::open(&self.fs, &self.dir, version_of(&self.fs, &self.dir, id)?)
	}

	pub fn record_at(&self, index: LogIndex) -> Result<Option<Record>> {
		let at = self.base_indexes.partition_point(|base| *base <= index);
		if at == 0 {
			return Ok(None);
		}
		let base = self.bases[at - 1];
		let log = self.dir.join(log_name(base));
		if base == self.base() {
			return find_at(&self.fs, &log, self.index.entries(), index);
		}
		let (_, entries) = read(&self.fs, &self.dir.join(index_name(base)))?;
		find_at(&self.fs, &log, &entries, index)
	}

	pub fn base_indexes(&self) -> &[LogIndex] {
		&self.base_indexes
	}

	pub fn route(&self, version: LogVersion) -> PathBuf {
		let at = self.bases.partition_point(|base| *base <= version);
		self.dir.join(log_name(self.bases[at.saturating_sub(1)]))
	}

	pub fn dir(&self) -> &Path {
		&self.dir
	}

	pub fn base(&self) -> LogVersion {
		*self.bases.last().expect("a partition always has an active segment")
	}

	pub fn bases(&self) -> &[LogVersion] {
		&self.bases
	}

	pub fn segment(&self) -> &Segment<F> {
		&self.segment
	}

	pub fn index(&self) -> &Index<F> {
		&self.index
	}

	pub fn vote(&self) -> &Vote<F> {
		&self.vote
	}

	pub fn save_vote(&mut self, state: State) -> Result<()> {
		self.vote.save(state)
	}

	pub fn commit(&mut self, index: LogIndex) -> Result<()> {
		let low = self.commit_index();
		let high = self.last_index().unwrap_or(LogIndex::ZERO);
		if index < low || index > high {
			return Err(LogError::CommitOutOfRange {
				dir: self.dir.clone(),
				low,
				high,
				found: index,
			});
		}
		self.vote.advance(index);
		Ok(())
	}

	pub fn commit_index(&self) -> LogIndex {
		self.vote.state().commit_index.min(self.last_index().unwrap_or(LogIndex::ZERO))
	}

	pub fn timestamps(&self) -> Option<TimestampRange> {
		self.timestamps
	}

	pub fn head(&self) -> Option<LogVersion> {
		self.head
	}

	pub fn compact_to(&mut self, index: LogIndex) -> Result<()> {
		let mut state = self.vote.state();
		if index == state.snapshot_index {
			return Ok(());
		}
		let low = state.snapshot_index;
		let high = self.commit_index();
		let found = if index >= low && index <= high {
			self.record_at(index)?
		} else {
			None
		};
		let Some(record) = found else {
			return Err(LogError::CompactOutOfRange {
				dir: self.dir.clone(),
				low,
				high,
				found: index,
			});
		};
		state.snapshot_index = index;
		state.snapshot_term = record.term;
		self.vote.save(state)
	}

	pub fn rebase(&mut self, index: LogIndex, term: Term) -> Result<()> {
		let mut state = self.vote.state();
		state.commit_index = state.commit_index.max(index);
		state.snapshot_index = index;
		state.snapshot_term = term;
		self.vote.save(state)?;
		self.empty_to(after(index))?;
		Ok(())
	}

	pub fn last_index(&self) -> Option<LogIndex> {
		self.tail().map(|(index, _)| index)
	}

	pub fn last_term(&self) -> Option<Term> {
		self.tail().map(|(_, term)| term)
	}

	fn tail(&self) -> Option<(LogIndex, Term)> {
		self.last.or_else(|| {
			let state = self.vote.state();
			(state.snapshot_index != LogIndex::ZERO).then_some((state.snapshot_index, state.snapshot_term))
		})
	}

	fn behind_snapshot(&self) -> bool {
		let held = self.last.map(|(index, _)| index).unwrap_or(before(self.base_indexes[0]));
		held < self.vote.state().snapshot_index
	}

	fn empty_to(&mut self, base_index: LogIndex) -> Result<Scan> {
		for base in self.bases[1..].iter().rev() {
			remove(&self.fs, &self.dir.join(log_name(*base)))?;
			remove(&self.fs, &self.dir.join(index_name(*base)))?;
		}
		self.bases.truncate(1);
		self.base_indexes.truncate(1);
		let base = self.bases[0];
		let log = self.dir.join(log_name(base));
		let (mut segment, _) = Segment::recover(&self.fs, &log)?;
		segment.reset(self.config.segment_bytes)?;
		let scanned = scan(&self.fs, &log)?;
		let (mut index, _) = Index::recover(
			&self.fs,
			&self.dir.join(index_name(base)),
			self.config.index_interval,
			&scanned,
		)?;
		index.rebase(base_index)?;
		self.fs.sync_dir(&self.dir)?;
		self.segment = segment;
		self.index = index;
		self.base_indexes[0] = base_index;
		self.timestamps = None;
		self.head = None;
		self.last = None;
		self.opened_at = self.clock.now();
		clamp(&self.fs, &self.dir, LogVersion::ZERO)?;
		Ok(scanned)
	}

	fn must_roll(&self, record: &Record) -> bool {
		let head = self.segment.head();
		if head == Position::ZERO {
			return false;
		}
		if head.as_u64() + record.encoded_len() as u64 > self.segment.capacity().as_bytes() {
			return true;
		}
		self.aged()
	}

	fn aged(&self) -> bool {
		match self.clock.now().checked_sub(self.config.segment_age) {
			Some(deadline) => self.opened_at <= deadline,
			None => false,
		}
	}

	fn roll(&mut self, base: LogVersion, base_index: LogIndex) -> Result<()> {
		self.seal()?;
		let (segment, index) = new_pair(&self.fs, &self.dir, self.config, base, base_index)?;
		self.segment = segment;
		self.index = index;
		self.bases.push(base);
		self.base_indexes.push(base_index);
		self.timestamps = None;
		self.opened_at = self.clock.now();
		Ok(())
	}
}

fn base_indexes_of<F: Filesystem + Open>(
	fs: &F,
	dir: &Path,
	bases: &[LogVersion],
	active: LogIndex,
) -> Result<Vec<LogIndex>> {
	let mut out = vec![active; bases.len()];
	for at in (0..bases.len() - 1).rev() {
		out[at] = match header(fs, &dir.join(index_name(bases[at]))) {
			Ok(found) => found.base_index,
			Err(error) if !rebuildable(&error) => return Err(error),
			Err(_) => match scan(fs, &dir.join(log_name(bases[at])))?.records.first() {
				Some(record) => record.index,
				None => out[at + 1],
			},
		};
	}
	Ok(out)
}

#[cfg(test)]
pub(crate) fn drain<F: Filesystem + Open + ReadDir, C: ClockNow>(partition: &Partition<F, C>) -> Result<Vec<Record>> {
	drain_cursor(&partition.fs, &partition.dir)
}

pub fn sync<F: Filesystem + OpenMut>(fs: &F, dir: &Path, base: LogVersion) -> Result<()> {
	sync_path(fs, &dir.join(log_name(base)))?;
	sync_path(fs, &dir.join(index_name(base)))
}

pub fn log_name(base: LogVersion) -> String {
	format!("{:0width$}{}", base.as_u64(), LOG_SUFFIX, width = NAME_DIGITS)
}

pub fn index_name(base: LogVersion) -> String {
	format!("{:0width$}{}", base.as_u64(), INDEX_SUFFIX, width = NAME_DIGITS)
}

pub fn base_of(name: &str) -> Option<LogVersion> {
	base_from(name, LOG_SUFFIX)
}

fn index_base_of(name: &str) -> Option<LogVersion> {
	base_from(name, INDEX_SUFFIX)
}

fn base_from(name: &str, suffix: &str) -> Option<LogVersion> {
	let digits = name.strip_suffix(suffix)?;
	if digits.len() != NAME_DIGITS {
		return None;
	}
	digits.parse().ok().map(LogVersion::new)
}

fn name_of(path: &Path) -> Option<&str> {
	path.file_name().and_then(|name| name.to_str())
}

pub fn bases_in<F: ReadDir>(fs: &F, dir: &Path) -> Result<Vec<LogVersion>> {
	let mut bases: Vec<LogVersion> = fs
		.read_dir(dir)?
		.iter()
		.filter_map(|path| path.file_name())
		.filter_map(|name| name.to_str())
		.filter_map(base_of)
		.collect();
	bases.sort_unstable();
	Ok(bases)
}

fn sweep<F: ReadDir + Unlink>(fs: &F, dir: &Path) -> Result<()> {
	let entries = fs.read_dir(dir)?;
	let bases: HashSet<LogVersion> = entries.iter().filter_map(|path| name_of(path)).filter_map(base_of).collect();
	for path in &entries {
		if path.as_os_str().as_encoded_bytes().ends_with(STAGING_SUFFIX.as_bytes()) {
			remove(fs, path)?;
			continue;
		}
		if let Some(base) = name_of(path).and_then(index_base_of)
			&& !bases.contains(&base)
		{
			remove(fs, path)?;
		}
	}
	Ok(())
}

fn expired<F: Filesystem + Open>(fs: &F, dir: &Path, base: LogVersion, deadline: DateTime) -> Result<bool> {
	match header(fs, &dir.join(index_name(base))) {
		Ok(found) => Ok(found.timestamps.is_some_and(|range| range.max <= deadline)),
		Err(error) if rebuildable(&error) => Ok(false),
		Err(error) => Err(error),
	}
}

fn rebuildable(error: &LogError) -> bool {
	matches!(error, LogError::NotFound(_) | LogError::IndexShort { .. } | LogError::IndexMagic { .. })
}

fn remove<F: Unlink>(fs: &F, path: &Path) -> Result<()> {
	match fs.unlink(path) {
		Err(FsError::NotFound(_)) => Ok(()),
		other => Ok(other?),
	}
}

fn discard<F: Unlink>(fs: &F, dir: &Path, base: LogVersion) -> Result<()> {
	remove(fs, &dir.join(index_name(base)))?;
	remove(fs, &dir.join(log_name(base)))
}

fn rebuild<F: Filesystem + Create + Open + Rename + SyncDir + Unlink>(
	fs: &F,
	dir: &Path,
	config: Config,
	base: LogVersion,
	scan: &Scan,
) -> Result<Index<F>> {
	let first = scan.records.first().expect("a rebuild without records is discarded, never rebuilt");
	let path = dir.join(index_name(base));
	remove(fs, &path)?;
	let mut index = Index::create(fs, &path, base, first.index, config.index_interval)?;
	let mut at = Position::ZERO;
	for record in &scan.records {
		index.append(record.version, record.index, at)?;
		at = at.advance(record.encoded_len() as u64);
	}
	index.sync()?;
	Ok(index)
}

fn new_pair<F: Filesystem + Create + Open + Rename + SyncDir + Unlink>(
	fs: &F,
	dir: &Path,
	config: Config,
	base: LogVersion,
	base_index: LogIndex,
) -> Result<(Segment<F>, Index<F>)> {
	let segment = Segment::create(fs, &dir.join(log_name(base)), config.segment_bytes)?;
	let index = Index::create(fs, &dir.join(index_name(base)), base, base_index, config.index_interval)?;
	Ok((segment, index))
}

fn last_of<F: Filesystem + Open>(
	fs: &F,
	dir: &Path,
	bases: &[LogVersion],
	active: Option<&Record>,
) -> Result<Option<(LogVersion, LogIndex, Term)>> {
	if let Some(record) = active {
		return Ok(Some((record.version, record.index, record.term)));
	}
	for base in bases.iter().rev().skip(1) {
		let scanned = scan(fs, &dir.join(log_name(*base)))?;
		if let Some(record) = scanned.records.last() {
			return Ok(Some((record.version, record.index, record.term)));
		}
	}
	Ok(None)
}

fn after(index: LogIndex) -> LogIndex {
	LogIndex::new(index.as_u64() + 1)
}

fn before(index: LogIndex) -> LogIndex {
	LogIndex::new(index.as_u64().saturating_sub(1))
}

fn widen(range: Option<TimestampRange>, timestamp: DateTime) -> Option<TimestampRange> {
	Some(match range {
		None => TimestampRange {
			min: timestamp,
			max: timestamp,
		},
		Some(range) => TimestampRange {
			min: range.min.min(timestamp),
			max: range.max.max(timestamp),
		},
	})
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use reifydb_codec::log::{NodeId, RecordKind, Term, index::Header};
	use reifydb_runtime::{
		context::clock::{Clock, MockClock},
		io::fs::{
			Len, Open, Pwrite,
			memory::MemoryFs,
			testing::{NoFaults, TestingFs},
		},
	};

	use super::*;
	use crate::index::read;

	const DIR: &str = "/log/p0";
	const BASE: LogVersion = LogVersion::new(500);
	const BASE_INDEX: LogIndex = LogIndex::new(1);

	fn config() -> Config {
		Config {
			segment_bytes: ByteSize::from_bytes(512),
			segment_age: Duration::from_seconds_const(60),
			index_interval: ByteSize::from_bytes(64),
		}
	}

	fn record(version: u64, payload: &[u8]) -> Record {
		Record::new(
			LogVersion::new(version),
			LogIndex::new(version - BASE.as_u64() + BASE_INDEX.as_u64()),
			Term::new(1),
			DateTime::from_bits(1000 + version),
			RecordKind::new(0),
			payload.to_vec(),
		)
	}

	fn fixture() -> (MemoryFs, MockClock, Partition<MemoryFs, Clock>) {
		let fs = MemoryFs::new();
		fs.mkdir(Path::new("/log")).unwrap();
		let mock = MockClock::from_millis(1_000);
		let partition = Partition::create(
			fs.clone(),
			Clock::Mock(mock.clone()),
			Path::new(DIR),
			config(),
			BASE,
			BASE_INDEX,
		)
		.unwrap();
		(fs, mock, partition)
	}

	#[test]
	fn a_name_is_twenty_digits_and_round_trips_through_its_base() {
		// the width is the whole point: parse must reject a name of any other length, or a
		// stray file drops into the middle of the segment order and routing silently
		// answers with the wrong segment.
		assert_eq!(log_name(BASE), "00000000000000000500.log");
		assert_eq!(index_name(BASE), "00000000000000000500.index");

		assert_eq!(base_of("00000000000000000500.log"), Some(BASE));
		assert_eq!(base_of("500.log"), None);
		assert_eq!(base_of("00000000000000000500.index"), None);
		assert_eq!(base_of("0000000000000000050x.log"), None);
	}

	#[test]
	fn names_sort_lexically_in_numeric_order() {
		// decision 199 buys exactly this: a directory listing arrives sorted, so open never
		// reads a file to find out which segment is newest. Without the padding 1000 sorts
		// before 500 and open adopts a segment that is not the last one.
		let mut names: Vec<String> =
			[1000u64, 0, 500].into_iter().map(|base| log_name(LogVersion::new(base))).collect();
		names.sort();

		let bases: Vec<Option<LogVersion>> = names.iter().map(|name| base_of(name)).collect();

		assert_eq!(bases, vec![Some(LogVersion::new(0)), Some(BASE), Some(LogVersion::new(1000))]);
	}

	#[test]
	fn a_full_segment_rolls_and_the_sealed_index_carries_the_range() {
		// the range is the reason roll exists before retention: an index sealed without it
		// reads back as absent, and retention cannot tell how old the segment is.
		let (fs, _, mut partition) = fixture();
		let first = record(500, &[0u8; 200]);
		let second = record(501, &[0u8; 200]);
		partition.append(&first).unwrap();
		partition.append(&second).unwrap();

		partition.append(&record(502, &[0u8; 200])).unwrap();

		assert_eq!(partition.bases(), [BASE, LogVersion::new(502)]);
		let (header, _) = read(&fs, &Path::new(DIR).join(index_name(BASE))).unwrap();
		assert_eq!(
			header.timestamps,
			Some(TimestampRange {
				min: first.timestamp,
				max: second.timestamp,
			})
		);
		assert_eq!(
			partition.timestamps(),
			Some(TimestampRange {
				min: DateTime::from_bits(1502),
				max: DateTime::from_bits(1502),
			})
		);
	}

	#[test]
	fn a_sealed_log_is_truncated_to_what_it_actually_holds() {
		// decision 202: the preallocated tail goes away and capacity shrinks with it, so a
		// later append to a sealed segment is refused through SegmentFull rather than
		// quietly extending a file the partition has stopped tracking.
		let (fs, _, mut partition) = fixture();
		partition.append(&record(500, &[0u8; 200])).unwrap();
		let sealed_head = partition.segment().head();

		partition.append(&record(501, &[0u8; 400])).unwrap();

		let len = fs.open(&Path::new(DIR).join(log_name(BASE))).unwrap().len().unwrap();
		assert_eq!(len, sealed_head.as_u64());
		assert!(len < config().segment_bytes.as_bytes());
	}

	#[test]
	fn an_aged_segment_rolls_even_though_it_has_room() {
		// decision 42: without the age bound a short ttl is rounded up to a whole segment,
		// because retention can only unlink a sealed one.
		let (_, clock, mut partition) = fixture();
		partition.append(&record(500, b"small")).unwrap();

		clock.advance_millis(60_000);
		partition.append(&record(501, b"small")).unwrap();

		assert_eq!(partition.bases(), [BASE, LogVersion::new(501)]);
	}

	#[test]
	fn an_empty_segment_never_rolls_on_age() {
		// rolling an empty segment would leave a zero record segment on disk for every age
		// bound that passes with no writes, and every one of them needs a name and a seal.
		let (_, clock, mut partition) = fixture();

		clock.advance_millis(600_000);
		partition.append(&record(500, b"first")).unwrap();

		assert_eq!(partition.bases(), [BASE]);
	}

	#[test]
	fn route_picks_the_newest_segment_at_or_below_a_version() {
		let (_, _, mut partition) = fixture();
		for version in [500u64, 501, 502, 503] {
			partition.append(&record(version, &[0u8; 200])).unwrap();
		}

		let dir = Path::new(DIR);
		assert_eq!(partition.bases(), [BASE, LogVersion::new(502)]);
		assert_eq!(partition.route(LogVersion::new(501)), dir.join(log_name(BASE)));
		assert_eq!(partition.route(LogVersion::new(502)), dir.join(log_name(LogVersion::new(502))));
		assert_eq!(partition.route(LogVersion::new(900)), dir.join(log_name(LogVersion::new(502))));
		assert_eq!(partition.route(LogVersion::new(1)), dir.join(log_name(BASE)));
	}

	#[test]
	fn reopening_adopts_the_newest_pair_and_rebuilds_the_range() {
		// the range lives in memory until seal, so a restart that did not rebuild it would
		// seal the adopted segment with an absent range and lose every record's timestamp.
		let (fs, mock, mut partition) = fixture();
		partition.append(&record(500, &[0u8; 200])).unwrap();
		partition.append(&record(501, &[0u8; 200])).unwrap();
		partition.append(&record(502, b"in the new segment")).unwrap();
		sync(&fs, partition.dir(), partition.base()).unwrap();

		let (reopened, scan) =
			Partition::<MemoryFs, Clock>::open(fs, Clock::Mock(mock), Path::new(DIR), config()).unwrap();

		assert_eq!(reopened.base(), LogVersion::new(502));
		assert_eq!(scan.records, vec![record(502, b"in the new segment")]);
		assert_eq!(
			reopened.timestamps(),
			Some(TimestampRange {
				min: DateTime::from_bits(1502),
				max: DateTime::from_bits(1502),
			})
		);
	}

	#[test]
	fn a_crash_after_seal_but_before_the_next_pair_leaves_a_segment_nothing_fits_in() {
		// the seal then create order makes this the only hole a crash can leave, and it
		// needs no repair: the sealed log reads its own length back as its capacity, so the
		// next append rolls rather than writing past the end of a finished segment.
		let (fs, mock, mut partition) = fixture();
		partition.append(&record(500, &[0u8; 200])).unwrap();
		partition.seal().unwrap();

		let (mut reopened, _) =
			Partition::<MemoryFs, Clock>::open(fs, Clock::Mock(mock), Path::new(DIR), config()).unwrap();
		reopened.append(&record(501, b"after the gap")).unwrap();

		assert_eq!(reopened.bases(), [BASE, LogVersion::new(501)]);
	}

	fn versions<C: ClockNow>(partition: &Partition<MemoryFs, C>) -> Vec<u64> {
		drain(partition).unwrap().iter().map(|record| record.version.as_u64()).collect()
	}

	fn at_index(version: u64, index: u64) -> Record {
		// the replacement tail carries the leader's version, which has no relation to the
		// index it lands on, so both are set explicitly.
		let mut record = record(version, b"replacement");
		record.index = LogIndex::new(index);
		record
	}

	fn in_term(version: u64, term: u64) -> Record {
		// the term is the only field that moves, so a refusal can only be the term guard.
		let mut record = record(version, b"x");
		record.term = Term::new(term);
		record
	}

	#[test]
	fn a_term_below_the_tail_is_refused_and_changes_nothing() {
		// a term that goes backwards means this node accepted an entry from a leader it has
		// already moved past, and once it is on the platter the log claims a history raft
		// says never happened. The tail must be untouched, or the refusal has already cost
		// what it was meant to protect.
		let (_, _, mut partition) = fixture();
		partition.append(&in_term(500, 4)).unwrap();

		let error = partition.append(&in_term(501, 3)).unwrap_err();

		assert!(matches!(error, LogError::TermRegression { last, found, .. }
			if last == Term::new(4) && found == Term::new(3)));
		assert_eq!(partition.last_index(), Some(BASE_INDEX));
		assert_eq!(partition.last_term(), Some(Term::new(4)));
		assert_eq!(drain(&partition).unwrap().len(), 1);
	}

	#[test]
	fn a_term_that_holds_or_rises_is_accepted() {
		// terms are monotonic, not strictly increasing: a leader appends many entries in one
		// term, so an equal term is the common case and must not be caught by the guard.
		let (_, _, mut partition) = fixture();

		partition.append(&in_term(500, 4)).unwrap();
		partition.append(&in_term(501, 4)).unwrap();
		partition.append(&in_term(502, 9)).unwrap();

		assert_eq!(partition.last_term(), Some(Term::new(9)));
	}

	#[test]
	fn the_term_guard_survives_a_reopen() {
		// the guard reads the tail recovered at open, so a partition that forgets its term on
		// the way back in accepts the first regression after every restart.
		let (fs, mock, mut partition) = fixture();
		partition.append(&in_term(500, 4)).unwrap();
		drop(partition);

		let (mut reopened, _) =
			Partition::<MemoryFs, Clock>::open(fs, Clock::Mock(mock), Path::new(DIR), config()).unwrap();
		let error = reopened.append(&in_term(501, 1)).unwrap_err();

		assert!(matches!(error, LogError::TermRegression { .. }));
	}

	fn rolled() -> (MemoryFs, MockClock, Partition<MemoryFs, Clock>) {
		let (fs, mock, mut partition) = fixture();
		for version in 500..=507 {
			partition.append(&record(version, &[0u8; 200])).unwrap();
		}
		assert_eq!(partition.bases(), [BASE, LogVersion::new(502), LogVersion::new(504), LogVersion::new(506)]);
		(fs, mock, partition)
	}

	fn clobber(fs: &MemoryFs, base: LogVersion, bytes: &[u8]) {
		let path = PathBuf::from(DIR).join(index_name(base));
		fs.open_mut(&path).unwrap().pwrite(0, bytes).unwrap();
	}

	#[test]
	fn the_base_index_map_holds_one_entry_per_segment_and_ascends() {
		// every index keyed lookup binary searches this, so a map that drifts out of step with
		// bases routes to the wrong file and answers with a record that exists but is not the one
		// asked for, which is worse than answering nothing.
		let (_, _, partition) = rolled();

		assert_eq!(partition.bases().len(), partition.base_indexes().len());
		assert_eq!(
			partition.base_indexes(),
			[LogIndex::new(1), LogIndex::new(3), LogIndex::new(5), LogIndex::new(7)]
		);
	}

	#[test]
	fn the_base_index_map_is_rebuilt_from_disk_on_open() {
		// the map is derived, not stored as a whole, so a reopen has to reconstruct exactly what
		// the live handle held or every lookup after a restart routes differently.
		let (fs, mock, partition) = rolled();
		let live = partition.base_indexes().to_vec();
		drop(partition);

		let (reopened, _) =
			Partition::<MemoryFs, Clock>::open(fs, Clock::Mock(mock), Path::new(DIR), config()).unwrap();

		assert_eq!(reopened.base_indexes(), live.as_slice());
	}

	#[test]
	fn a_record_is_found_by_index_in_a_sealed_segment() {
		// raft addresses entries by index and never by version, so a lookup has to cross into a
		// segment that was sealed and closed long before the question was asked.
		let (_, _, partition) = rolled();

		let found = partition.record_at(LogIndex::new(3)).unwrap().unwrap();

		assert_eq!(found.version, LogVersion::new(502));
		assert_eq!(found.index, LogIndex::new(3));
	}

	#[test]
	fn a_record_is_found_by_index_in_the_active_segment() {
		// the active segment's entries live in memory rather than in a readable index file, so it
		// is the one segment the sealed path cannot answer for.
		let (_, _, partition) = rolled();

		let found = partition.record_at(LogIndex::new(8)).unwrap().unwrap();

		assert_eq!(found.version, LogVersion::new(507));
	}

	#[test]
	fn an_index_above_the_tail_is_absent_rather_than_an_error() {
		// a follower routinely asks for an index this node has not reached yet, and raft reads
		// absent as "send me from here"; an error would turn ordinary replication into a fault.
		let (_, _, partition) = rolled();

		assert!(partition.record_at(LogIndex::new(9)).unwrap().is_none());
	}

	#[test]
	fn an_index_below_the_oldest_segment_is_absent_rather_than_misrouted() {
		// after a purge the entry is genuinely gone, and the binary search must not clamp to the
		// first segment and hand back an unrelated record as if it were the one asked for.
		let (_, _, mut partition) = rolled();
		let tail = partition.last_index().unwrap();
		partition.commit(tail).unwrap();
		partition.compact_to(tail).unwrap();
		partition.purge(EXPIRED).unwrap();

		assert_eq!(partition.base_indexes(), [LogIndex::new(7)]);
		assert!(partition.record_at(LogIndex::new(3)).unwrap().is_none());
	}

	#[test]
	fn an_index_below_the_oldest_segment_costs_no_syscall_at_all() {
		// answering absent is not enough: without the floor check the lookup clamps to the oldest
		// segment and scans it whole, because an index below every entry resolves to position zero.
		// A lagging follower probes exactly this range, so the cost would be a full segment read per
		// probe, and the answer would be identical either way.
		let fs = TestingFs::new(MemoryFs::new(), Arc::new(NoFaults));
		fs.mkdir(Path::new("/log")).unwrap();
		let mock = MockClock::from_millis(1_000);
		let mut partition = Partition::create(
			fs.clone(),
			Clock::Mock(mock.clone()),
			Path::new(DIR),
			config(),
			BASE,
			BASE_INDEX,
		)
		.unwrap();
		for version in 500..=507 {
			partition.append(&record(version, &[0u8; 200])).unwrap();
		}
		let tail = partition.last_index().unwrap();
		partition.commit(tail).unwrap();
		partition.compact_to(tail).unwrap();
		partition.purge(EXPIRED).unwrap();
		let before = fs.calls();

		assert!(partition.record_at(LogIndex::new(3)).unwrap().is_none());

		assert_eq!(fs.calls(), before, "a lookup below the floor reached the filesystem");
	}

	#[test]
	fn a_commit_index_advances_and_rides_out_on_the_next_vote() {
		// the commit index advances far more often than a vote does, so it is not made durable on
		// its own: it is a hint that may lag, and it reaches disk whenever the vote next does.
		let (fs, mock, mut partition) = fixture();
		for version in 500..=503 {
			partition.append(&record(version, b"x")).unwrap();
		}

		partition.commit(LogIndex::new(3)).unwrap();
		partition
			.save_vote(State {
				term: Term::new(2),
				voted_for: Some(NodeId::new(7)),
				commit_index: partition.commit_index(),
				..State::EMPTY
			})
			.unwrap();
		drop(partition);

		let (reopened, _) =
			Partition::<MemoryFs, Clock>::open(fs, Clock::Mock(mock), Path::new(DIR), config()).unwrap();

		assert_eq!(reopened.commit_index(), LogIndex::new(3));
	}

	#[test]
	fn a_commit_index_above_the_tail_is_refused() {
		// committing past the tail claims entries this node never wrote, and a follower that reports
		// it would have the leader believe those entries are replicated here.
		let (_, _, mut partition) = fixture();
		partition.append(&record(500, b"x")).unwrap();

		let error = partition.commit(LogIndex::new(2)).unwrap_err();

		assert!(matches!(error, LogError::CommitOutOfRange { high, found, .. }
			if high == LogIndex::new(1) && found == LogIndex::new(2)));
		assert_eq!(partition.commit_index(), LogIndex::ZERO);
	}

	#[test]
	fn a_commit_index_that_goes_backwards_is_refused_rather_than_ignored() {
		// raft only ever raises the commit index, so a decrease is a confused caller; taking the
		// maximum silently would leave that caller believing it had rewound and carry on.
		let (_, _, mut partition) = fixture();
		for version in 500..=502 {
			partition.append(&record(version, b"x")).unwrap();
		}
		partition.commit(LogIndex::new(3)).unwrap();

		let error = partition.commit(LogIndex::new(1)).unwrap_err();

		assert!(matches!(error, LogError::CommitOutOfRange { low, .. } if low == LogIndex::new(3)));
		assert_eq!(partition.commit_index(), LogIndex::new(3));
	}

	#[test]
	fn a_stored_commit_index_above_the_recovered_tail_is_clamped_when_read() {
		// the vote and the log sync separately, so a crash can leave a vote naming entries the log
		// never kept. The clamp sits on the accessor and not on the file: the vote must keep
		// reporting exactly what was written, or a crash sweep sees a state the group never cast.
		let (fs, mock, mut partition) = fixture();
		partition.append(&record(500, b"x")).unwrap();
		partition
			.save_vote(State {
				term: Term::new(2),
				voted_for: None,
				commit_index: LogIndex::new(9),
				..State::EMPTY
			})
			.unwrap();
		drop(partition);

		let (reopened, _) =
			Partition::<MemoryFs, Clock>::open(fs, Clock::Mock(mock), Path::new(DIR), config()).unwrap();

		assert_eq!(reopened.commit_index(), LogIndex::new(1));
	}

	const RECENT: Duration = Duration::from_seconds_const(1);
	const EXPIRED: Duration = Duration::from_milliseconds_const(1);

	#[test]
	fn purge_unlinks_a_sealed_pair_older_than_the_ttl() {
		// this is the whole point of the crate: retention is an unlink of a whole segment, never a
		// delete inside a file, so both halves of the pair have to leave the directory.
		let (fs, _, mut partition) = rolled();

		let tail = partition.last_index().unwrap();
		partition.commit(tail).unwrap();
		partition.compact_to(tail).unwrap();
		let dropped = partition.purge(EXPIRED).unwrap();

		assert_eq!(dropped, [BASE, LogVersion::new(502), LogVersion::new(504)]);
		assert_eq!(partition.bases(), [LogVersion::new(506)]);
		for base in dropped {
			assert!(fs.open(&PathBuf::from(DIR).join(log_name(base))).is_err());
			assert!(fs.open(&PathBuf::from(DIR).join(index_name(base))).is_err());
		}
	}

	#[test]
	fn purge_never_unlinks_the_active_segment() {
		// the active segment holds the records nothing has read yet and is the only one still open
		// for write; unlinking it loses acknowledged records and leaves the partition unopenable.
		let (_, _, mut partition) = fixture();
		partition.append(&record(500, b"only")).unwrap();

		assert_eq!(partition.purge(EXPIRED).unwrap(), []);
		assert_eq!(partition.bases(), [BASE]);
	}

	#[test]
	fn a_sealed_segment_that_is_still_the_active_one_is_never_purged() {
		// a crash between the seal and the pair that should have followed it leaves the newest segment
		// sealed and carrying a range, so it looks exactly like a purge candidate. The rule that the
		// last base is never a candidate is the only thing between retention and the head of the log.
		let (fs, mock, mut partition) = fixture();
		partition.append(&record(500, b"only")).unwrap();
		partition.seal().unwrap();
		drop(partition);
		let (mut reopened, _) =
			Partition::open(fs.clone(), Clock::Mock(mock), Path::new(DIR), config()).unwrap();

		assert_eq!(reopened.purge(EXPIRED).unwrap(), []);
		assert_eq!(reopened.bases(), [BASE]);
		assert_eq!(drain(&reopened).unwrap().len(), 1);
	}

	#[test]
	fn a_segment_inside_the_ttl_is_kept() {
		// without the age test purge would unlink on the reader floor alone, and a log with no
		// readers registered would drop every sealed segment the moment it rolled.
		let (_, _, mut partition) = rolled();

		let tail = partition.last_index().unwrap();
		partition.commit(tail).unwrap();
		partition.compact_to(tail).unwrap();
		assert_eq!(partition.purge(RECENT).unwrap(), []);
		assert_eq!(partition.bases().len(), 4);
	}

	#[test]
	fn purge_stops_at_the_first_segment_a_reader_still_needs() {
		// decision 232: it drops a leading run, never a set. Skipping over a pinned segment to
		// unlink an older one behind it would leave a hole, and a hole in the middle is worse than
		// a short log because a reader cannot tell it from the end.
		let (_, _, mut partition) = rolled();
		partition.register("flow-3").unwrap();
		partition.record("flow-3", LogVersion::new(503)).unwrap();

		let tail = partition.last_index().unwrap();
		partition.commit(tail).unwrap();
		partition.compact_to(tail).unwrap();
		let dropped = partition.purge(EXPIRED).unwrap();

		assert_eq!(dropped, [BASE]);
		assert_eq!(partition.bases(), [LogVersion::new(502), LogVersion::new(504), LogVersion::new(506)]);
	}

	#[test]
	fn a_reader_that_has_recorded_nothing_holds_every_segment() {
		// a registered reader pins at zero, so registering before reading must not be a window in
		// which retention races ahead and unlinks what the reader was about to read.
		let (_, _, mut partition) = rolled();
		partition.register("flow-3").unwrap();

		let tail = partition.last_index().unwrap();
		partition.commit(tail).unwrap();
		partition.compact_to(tail).unwrap();
		assert_eq!(partition.purge(EXPIRED).unwrap(), []);
		assert_eq!(partition.bases().len(), 4);
	}

	#[test]
	fn a_sealed_index_with_no_timestamp_range_is_never_purged() {
		// decision 188 lets a range be absent, and absent means the age of the segment was never
		// written down. Purging on a guess is not recoverable, so every "I cannot tell" answers keep.
		let (fs, _, mut partition) = rolled();
		clobber(&fs, BASE, &Header::new(BASE, BASE_INDEX).encode());

		let tail = partition.last_index().unwrap();
		partition.commit(tail).unwrap();
		partition.compact_to(tail).unwrap();
		assert_eq!(partition.purge(EXPIRED).unwrap(), []);
		assert_eq!(partition.bases().len(), 4);
	}

	#[test]
	fn a_sealed_index_that_cannot_be_read_is_never_purged() {
		// same answer one file over: without the index header there is no range, and unlinking a
		// segment whose age is unknown throws away records no scan can bring back.
		let (fs, _, mut partition) = rolled();
		clobber(&fs, BASE, &[0u8; 4]);

		let tail = partition.last_index().unwrap();
		partition.commit(tail).unwrap();
		partition.compact_to(tail).unwrap();
		assert_eq!(partition.purge(EXPIRED).unwrap(), []);
		assert_eq!(partition.bases().len(), 4);
	}

	#[test]
	fn purging_twice_drops_nothing_the_second_time() {
		// purge is an explicit call a caller can make on any schedule, so a second pass over a
		// directory it already trimmed must be a no op rather than reaching into the active segment.
		let (_, _, mut partition) = rolled();
		let tail = partition.last_index().unwrap();
		partition.commit(tail).unwrap();
		partition.compact_to(tail).unwrap();
		partition.purge(EXPIRED).unwrap();

		assert_eq!(partition.purge(EXPIRED).unwrap(), []);
		assert_eq!(partition.bases(), [LogVersion::new(506)]);
	}

	#[test]
	fn an_index_whose_log_is_gone_is_swept_on_open() {
		// decision 235: purge unlinks the log first, so a crash between the two leaves an orphan
		// index. Nothing lists it, so it would sit there forever unless open removes it.
		let (fs, mock, partition) = rolled();
		drop(partition);
		remove(&fs, &PathBuf::from(DIR).join(log_name(BASE))).unwrap();

		let (reopened, _) = Partition::open(fs.clone(), Clock::Mock(mock), Path::new(DIR), config()).unwrap();

		assert_eq!(reopened.bases(), [LogVersion::new(502), LogVersion::new(504), LogVersion::new(506)]);
		assert!(fs.open(&PathBuf::from(DIR).join(index_name(BASE))).is_err());
	}

	#[test]
	fn a_truncation_inside_the_active_segment_drops_only_its_tail() {
		// decision 180: a new leader overwrites a conflicting tail on purpose, so the cut is a
		// first class operation and not a recovery side effect. only the records at or above
		// the cut may go, and the segment list must not move when the cut lands inside the
		// active segment.
		let (_, _, mut partition) = rolled();

		partition.truncate_from(LogIndex::new(8)).unwrap();

		assert_eq!(partition.bases(), [BASE, LogVersion::new(502), LogVersion::new(504), LogVersion::new(506)]);
		assert_eq!(partition.last_index(), Some(LogIndex::new(7)));
		assert_eq!(partition.head(), Some(LogVersion::new(506)));
		assert_eq!(versions(&partition), (500..=506).collect::<Vec<_>>());
	}

	#[test]
	fn a_truncation_drops_every_segment_that_starts_at_or_above_the_cut() {
		// a cut two segments down must unlink the whole segments above it, not just forget
		// them in memory, or a reopen would scan them back in and undo the truncation.
		let (fs, _, mut partition) = rolled();

		partition.truncate_from(LogIndex::new(5)).unwrap();

		assert_eq!(partition.bases(), [BASE, LogVersion::new(502)]);
		assert_eq!(partition.last_index(), Some(LogIndex::new(4)));
		assert_eq!(versions(&partition), vec![500, 501, 502, 503]);
		for base in [LogVersion::new(504), LogVersion::new(506)] {
			assert!(fs.open(&PathBuf::from(DIR).join(log_name(base))).is_err());
			assert!(fs.open(&PathBuf::from(DIR).join(index_name(base))).is_err());
		}
	}

	#[test]
	fn a_cut_on_a_segment_boundary_keeps_the_segment_below_it_whole() {
		// the cut is the first index that goes, so a segment whose every record sits below it
		// survives untouched even though the cut names the base of the segment after it.
		let (_, _, mut partition) = rolled();

		partition.truncate_from(LogIndex::new(7)).unwrap();

		assert_eq!(partition.bases(), [BASE, LogVersion::new(502), LogVersion::new(504)]);
		assert_eq!(partition.last_index(), Some(LogIndex::new(6)));
		assert_eq!(versions(&partition), (500..=505).collect::<Vec<_>>());
	}

	#[test]
	fn a_truncation_below_the_oldest_surviving_index_is_refused() {
		// the records the caller wants gone are already gone, so the request cannot be
		// honoured: a follower this far behind needs a snapshot, and emptying the log would
		// hide that behind something that looks healthy. Retention only ever raises the floor
		// past entries the compaction point covers, and the point never passes the commit
		// index, so a cut below the floor is always a cut into the committed prefix and it is
		// the commit guard that refuses it.
		let (_, _, mut partition) = rolled();
		let tail = partition.last_index().unwrap();
		partition.commit(tail).unwrap();
		partition.compact_to(tail).unwrap();
		partition.purge(EXPIRED).unwrap();

		let error = partition.truncate_from(LogIndex::new(6)).unwrap_err();

		assert!(matches!(error, LogError::TruncateCommitted { .. }), "{error}");
		assert_eq!(partition.last_index(), Some(LogIndex::new(8)));
		assert_eq!(versions(&partition), (506..=507).collect::<Vec<_>>());
	}

	#[test]
	fn a_cut_at_the_oldest_index_empties_the_partition() {
		// a follower whose whole uncommitted log disagrees with its leader is told to drop all of
		// it, and that cut has to land rather than be refused as if a snapshot were needed.
		let (fs, _, mut partition) = rolled();

		partition.truncate_from(LogIndex::new(1)).unwrap();

		assert_eq!(partition.bases(), [BASE]);
		assert_eq!(partition.base_indexes(), [LogIndex::new(1)]);
		assert_eq!(partition.last_index(), None);
		assert_eq!(partition.head(), None);
		assert!(versions(&partition).is_empty());
		for base in [LogVersion::new(502), LogVersion::new(504), LogVersion::new(506)] {
			assert!(fs.open(&PathBuf::from(DIR).join(log_name(base))).is_err());
		}
	}

	#[test]
	fn an_emptied_partition_takes_records_again() {
		// truncating in place leaves the capacity at the cut, so an emptied segment would report
		// itself full at zero bytes and never roll: every later append is refused and the follower
		// the cut was meant to repair can never be repaired.
		let (_, _, mut partition) = rolled();
		partition.truncate_from(LogIndex::new(1)).unwrap();

		partition.append(&at_index(600, 1)).unwrap();

		assert_eq!(partition.last_index(), Some(LogIndex::new(1)));
		assert_eq!(versions(&partition), vec![600]);
	}

	#[test]
	fn an_emptied_partition_reopens_empty_rather_than_missing() {
		// recovery discards a base whose segment holds no records, so an emptied partition left
		// with a zero byte pair would lose its last base and reopen as a log that does not exist.
		let (fs, mock, mut partition) = rolled();
		partition.truncate_from(LogIndex::new(1)).unwrap();
		drop(partition);

		let (reopened, _) = Partition::open(fs, Clock::Mock(mock), Path::new(DIR), config()).unwrap();

		assert_eq!(reopened.bases(), [BASE]);
		assert_eq!(reopened.last_index(), None);
		assert_eq!(reopened.head(), None);
	}

	#[test]
	fn a_reader_pinned_when_the_partition_empties_is_pulled_back_to_the_start() {
		// there is no surviving head to clamp to, so the ceiling has to fall to the start sentinel
		// register publishes; leaving the reader where it was skips everything the leader sends
		// next.
		let (fs, _, mut partition) = rolled();
		partition.register("sub").unwrap();
		partition.record("sub", LogVersion::new(507)).unwrap();

		partition.truncate_from(LogIndex::new(1)).unwrap();

		assert_eq!(version_of(&fs, Path::new(DIR), "sub").unwrap(), LogVersion::ZERO);
	}

	#[test]
	fn a_truncation_at_or_below_the_commit_index_is_refused() {
		// a committed entry is one a quorum has already acknowledged, so deleting it would
		// let two leaders disagree about a decided value. the guard fires at the commit index
		// itself, not one below it.
		let (_, _, mut partition) = rolled();
		partition.commit(LogIndex::new(4)).unwrap();

		let at = partition.truncate_from(LogIndex::new(4)).unwrap_err();
		let below = partition.truncate_from(LogIndex::new(3)).unwrap_err();

		assert!(matches!(at, LogError::TruncateCommitted { .. }), "{at}");
		assert!(matches!(below, LogError::TruncateCommitted { .. }), "{below}");
		assert_eq!(versions(&partition), (500..=507).collect::<Vec<_>>());
		partition.truncate_from(LogIndex::new(5)).unwrap();
	}

	#[test]
	fn a_truncation_above_the_tail_changes_nothing() {
		// a leader that repeats a cut this node has already applied must not be treated as an
		// error, and must not unlink a segment on the way through.
		let (_, _, mut partition) = rolled();

		partition.truncate_from(LogIndex::new(9)).unwrap();

		assert_eq!(partition.bases(), [BASE, LogVersion::new(502), LogVersion::new(504), LogVersion::new(506)]);
		assert_eq!(partition.last_index(), Some(LogIndex::new(8)));
		assert_eq!(versions(&partition), (500..=507).collect::<Vec<_>>());
	}

	#[test]
	fn a_truncated_partition_takes_the_replacement_records() {
		// the point of the cut is to make room for the leader's version of the tail, so the
		// index guard must accept the very index that was just deleted.
		let (_, _, mut partition) = rolled();
		partition.truncate_from(LogIndex::new(7)).unwrap();

		partition.append(&at_index(600, 7)).unwrap();

		assert_eq!(partition.last_index(), Some(LogIndex::new(7)));
		assert_eq!(versions(&partition), vec![500, 501, 502, 503, 504, 505, 600]);
	}

	#[test]
	fn a_truncation_holds_across_a_reopen() {
		// the cut has to reach the files, not only the in memory lists, or recovery scans the
		// deleted tail back in.
		let (fs, mock, mut partition) = rolled();
		partition.truncate_from(LogIndex::new(5)).unwrap();
		drop(partition);

		let (reopened, _) = Partition::open(fs, Clock::Mock(mock), Path::new(DIR), config()).unwrap();

		assert_eq!(reopened.bases(), [BASE, LogVersion::new(502)]);
		assert_eq!(reopened.last_index(), Some(LogIndex::new(4)));
		assert_eq!(reopened.head(), Some(LogVersion::new(503)));
	}

	#[test]
	fn a_reader_pinned_above_the_cut_is_pulled_down_to_it() {
		// a reader left above the cut would open a cursor past everything that survives and
		// silently skip the replacement records the leader sends next, which is data loss for
		// the subscriber rather than a stale position.
		let (fs, _, mut partition) = rolled();
		partition.register("sub").unwrap();
		partition.record("sub", LogVersion::new(507)).unwrap();

		partition.truncate_from(LogIndex::new(6)).unwrap();

		assert_eq!(version_of(&fs, Path::new(DIR), "sub").unwrap(), LogVersion::new(504));
	}

	#[test]
	fn a_reader_below_the_cut_is_left_where_it_is() {
		// clamping is a ceiling, not an assignment: a reader that has not reached the cut has
		// lost nothing and must keep its place.
		let (fs, _, mut partition) = rolled();
		partition.register("sub").unwrap();
		partition.record("sub", LogVersion::new(501)).unwrap();

		partition.truncate_from(LogIndex::new(6)).unwrap();

		assert_eq!(version_of(&fs, Path::new(DIR), "sub").unwrap(), LogVersion::new(501));
	}

	#[test]
	fn a_truncated_segment_is_full_so_the_next_append_rolls() {
		// decision 254: the cut leaves capacity at the cut rather than growing the file back,
		// so the segment reports itself full and the existing roll path takes over. without
		// this the next append would extend a file recovery believes was sealed.
		let (_, _, mut partition) = rolled();
		partition.truncate_from(LogIndex::new(8)).unwrap();
		let before = partition.bases().len();

		partition.append(&at_index(600, 8)).unwrap();

		assert_eq!(partition.bases().len(), before + 1);
		assert_eq!(partition.base(), LogVersion::new(600));
	}

	fn termed() -> (MemoryFs, MockClock, Partition<MemoryFs, Clock>) {
		// indexes 1..=4 in terms 1, 2, 3, 4 with index 3 committed: a compaction point has to pick
		// up the term of the record at it, not the tail's and not the first one's.
		let (fs, mock, mut partition) = fixture();
		for (version, term) in [(500, 1), (501, 2), (502, 3), (503, 4)] {
			partition.append(&in_term(version, term)).unwrap();
		}
		partition.commit(LogIndex::new(3)).unwrap();
		(fs, mock, partition)
	}

	fn reopen(fs: MemoryFs, mock: MockClock) -> (Partition<MemoryFs, Clock>, Scan) {
		Partition::<MemoryFs, Clock>::open(fs, Clock::Mock(mock), Path::new(DIR), config()).unwrap()
	}

	#[test]
	fn a_compaction_point_carries_the_term_of_the_record_it_names() {
		// once the entry itself is unlinked, this term is all the leader's consistency check can be
		// answered with, so it has to be the term of that exact record.
		let (_, _, mut partition) = termed();

		partition.compact_to(LogIndex::new(2)).unwrap();

		let state = partition.vote().state();
		assert_eq!(state.snapshot_index, LogIndex::new(2));
		assert_eq!(state.snapshot_term, Term::new(2));
	}

	#[test]
	fn a_compaction_point_survives_a_reopen() {
		// the point decides what purge may unlink; a restart that forgot it would either hold every
		// segment forever or, worse, trust a stale one and drop entries the store never applied.
		let (fs, mock, mut partition) = termed();
		partition.compact_to(LogIndex::new(3)).unwrap();
		drop(partition);

		let (reopened, _) = reopen(fs, mock);

		assert_eq!(reopened.vote().state().snapshot_index, LogIndex::new(3));
		assert_eq!(reopened.vote().state().snapshot_term, Term::new(3));
	}

	#[test]
	fn a_compaction_point_above_the_commit_index_is_refused() {
		// an uncommitted entry can still be overwritten by a leader; a snapshot that already covers
		// it would make that overwrite impossible to apply.
		let (_, _, mut partition) = termed();

		let error = partition.compact_to(LogIndex::new(4)).unwrap_err();

		assert!(matches!(error, LogError::CompactOutOfRange { low, high, found, .. }
			if low == LogIndex::ZERO && high == LogIndex::new(3) && found == LogIndex::new(4)));
		assert_eq!(partition.vote().state().snapshot_index, LogIndex::ZERO);
	}

	#[test]
	fn a_compaction_point_never_moves_backwards() {
		// the entries below the point may already be unlinked, so a lower point would name a term
		// the log can no longer look up.
		let (_, _, mut partition) = termed();
		partition.compact_to(LogIndex::new(3)).unwrap();

		let error = partition.compact_to(LogIndex::new(2)).unwrap_err();

		assert!(matches!(error, LogError::CompactOutOfRange { low, found, .. }
			if low == LogIndex::new(3) && found == LogIndex::new(2)));
		assert_eq!(partition.vote().state().snapshot_index, LogIndex::new(3));
	}

	#[test]
	fn purge_drops_nothing_until_a_compaction_point_exists() {
		// age alone is no reason to unlink: an entry is needed until the state machine holds it, and
		// only the compaction point says that it does.
		let (_, _, mut partition) = rolled();
		partition.commit(LogIndex::new(8)).unwrap();

		assert_eq!(partition.purge(EXPIRED).unwrap(), []);
		assert_eq!(partition.bases().len(), 4);
	}

	#[test]
	fn purge_drops_only_the_segments_wholly_below_the_compaction_point() {
		// segment 502 holds indexes 3 and 4 and segment 504 holds 5 and 6: a point at 4 releases
		// the first two segments and pins the third although its age alone would let it go, and a
		// point at 5 still pins it because index 6 sits above the point.
		let (_, _, mut partition) = rolled();
		partition.commit(LogIndex::new(8)).unwrap();
		partition.compact_to(LogIndex::new(4)).unwrap();

		assert_eq!(partition.purge(EXPIRED).unwrap(), [BASE, LogVersion::new(502)]);
		assert_eq!(partition.base_indexes(), [LogIndex::new(5), LogIndex::new(7)]);

		partition.compact_to(LogIndex::new(5)).unwrap();
		assert_eq!(partition.purge(EXPIRED).unwrap(), []);

		partition.compact_to(LogIndex::new(6)).unwrap();
		assert_eq!(partition.purge(EXPIRED).unwrap(), [LogVersion::new(504)]);
		assert_eq!(partition.base_indexes(), [LogIndex::new(7)]);
	}

	#[test]
	fn an_emptied_log_answers_its_tail_from_the_compaction_point() {
		// with everything at or below the point unlinked and the rest cut away the log holds no
		// record, yet raft still needs the (index, term) of the last entry for its consistency
		// check, and the compaction point is exactly that entry.
		let (_, _, mut partition) = rolled();
		partition.commit(LogIndex::new(4)).unwrap();
		partition.compact_to(LogIndex::new(4)).unwrap();
		partition.purge(EXPIRED).unwrap();
		partition.truncate_from(LogIndex::new(5)).unwrap();

		assert_eq!(versions(&partition), Vec::<u64>::new());
		assert_eq!(partition.last_index(), Some(LogIndex::new(4)));
		assert_eq!(partition.last_term(), Some(Term::new(1)));
		assert_eq!(partition.commit_index(), LogIndex::new(4));
	}

	#[test]
	fn a_rebase_empties_the_log_and_restarts_it_above_the_snapshot() {
		// a follower installing a snapshot at 20 has no use for anything it holds, and the next
		// entry its leader sends is 21: the floor, the tail and the commit index all move there.
		let (fs, _, mut partition) = rolled();

		partition.rebase(LogIndex::new(20), Term::new(5)).unwrap();

		assert_eq!(versions(&partition), Vec::<u64>::new());
		assert_eq!(partition.bases(), [BASE]);
		assert_eq!(partition.base_indexes(), [LogIndex::new(21)]);
		assert_eq!(partition.last_index(), Some(LogIndex::new(20)));
		assert_eq!(partition.last_term(), Some(Term::new(5)));
		assert_eq!(partition.commit_index(), LogIndex::new(20));
		assert_eq!(partition.head(), None);
		for base in [502, 504, 506] {
			assert!(fs.open(&PathBuf::from(DIR).join(log_name(LogVersion::new(base)))).is_err());
			assert!(fs.open(&PathBuf::from(DIR).join(index_name(LogVersion::new(base)))).is_err());
		}
	}

	#[test]
	fn a_rebased_partition_takes_the_entry_after_the_snapshot_and_nothing_else() {
		// the index and term guards have to keep working with no record to read them from, or a
		// leader could slip an entry in below the snapshot or from an older term.
		let (_, _, mut partition) = rolled();
		partition.rebase(LogIndex::new(20), Term::new(5)).unwrap();

		let error = partition.append(&at_index(600, 22)).unwrap_err();
		assert!(matches!(error, LogError::IndexGap { expected, .. } if expected == LogIndex::new(21)));
		let mut stale = at_index(600, 21);
		stale.term = Term::new(4);
		let error = partition.append(&stale).unwrap_err();
		assert!(matches!(error, LogError::TermRegression { last, .. } if last == Term::new(5)));

		let mut next = at_index(600, 21);
		next.term = Term::new(5);
		partition.append(&next).unwrap();

		assert_eq!(versions(&partition), [600]);
		assert_eq!(
			partition.record_at(LogIndex::new(21)).unwrap().map(|found| found.version),
			Some(LogVersion::new(600))
		);
		assert_eq!(partition.record_at(LogIndex::new(20)).unwrap(), None);
	}

	#[test]
	fn a_rebase_survives_a_reopen() {
		// the new floor lives in the index header and the tail in the vote file; a reopen that read
		// either from the old segment would demand index 1 next and refuse the leader's 21. The
		// entry appended first is what makes this a real reopen: an empty log behind its snapshot
		// is healed on open, so it would hide a floor that never reached the header.
		let (fs, mock, mut partition) = rolled();
		partition.rebase(LogIndex::new(20), Term::new(5)).unwrap();
		let mut next = at_index(600, 21);
		next.term = Term::new(5);
		partition.append(&next).unwrap();
		drop(partition);

		let (reopened, scan) = reopen(fs, mock);

		assert_eq!(scan.records.len(), 1);
		assert_eq!(reopened.bases(), [BASE]);
		assert_eq!(reopened.base_indexes(), [LogIndex::new(21)]);
		assert_eq!(reopened.last_index(), Some(LogIndex::new(21)));
		assert_eq!(reopened.last_term(), Some(Term::new(5)));
		assert_eq!(reopened.commit_index(), LogIndex::new(20));
		assert_eq!(reopened.record_at(LogIndex::new(20)).unwrap(), None);
	}

	#[test]
	fn a_snapshot_saved_ahead_of_the_log_empties_it_on_open() {
		// a rebase writes the vote first and cuts the log second, so a crash between the two leaves
		// a snapshot the log has not caught up with; open has to finish the cut, or the next append
		// lands on a tail the snapshot already covers.
		let (fs, mock, mut partition) = rolled();
		partition
			.save_vote(State {
				commit_index: LogIndex::new(20),
				snapshot_index: LogIndex::new(20),
				snapshot_term: Term::new(5),
				..State::EMPTY
			})
			.unwrap();
		drop(partition);

		let (reopened, scan) = reopen(fs.clone(), mock);

		assert!(scan.records.is_empty());
		assert_eq!(versions(&reopened), Vec::<u64>::new());
		assert_eq!(reopened.bases(), [BASE]);
		assert_eq!(reopened.base_indexes(), [LogIndex::new(21)]);
		assert_eq!(reopened.last_index(), Some(LogIndex::new(20)));
		assert_eq!(reopened.last_term(), Some(Term::new(5)));
		for base in [502, 504, 506] {
			assert!(fs.open(&PathBuf::from(DIR).join(log_name(LogVersion::new(base)))).is_err());
		}
	}

	#[test]
	fn a_snapshot_the_log_already_covers_leaves_it_alone_on_open() {
		// a point below the tail is the ordinary state after every compaction; healing that would
		// throw away live entries on every restart.
		let (fs, mock, mut partition) = rolled();
		partition.commit(LogIndex::new(8)).unwrap();
		partition.compact_to(LogIndex::new(4)).unwrap();
		drop(partition);

		let (reopened, _) = reopen(fs, mock);

		assert_eq!(versions(&reopened), (500..=507).collect::<Vec<_>>());
		assert_eq!(reopened.bases().len(), 4);
		assert_eq!(reopened.last_index(), Some(LogIndex::new(8)));
	}

	#[test]
	fn a_snapshot_at_the_tail_leaves_the_log_alone_on_open() {
		// a point exactly at the tail is what a store that keeps up produces; it is not behind, and
		// emptying it would only cost the last segment's records for nothing.
		let (fs, mock, mut partition) = rolled();
		partition.commit(LogIndex::new(8)).unwrap();
		partition.compact_to(LogIndex::new(8)).unwrap();
		drop(partition);

		let (reopened, _) = reopen(fs, mock);

		assert_eq!(versions(&reopened), (500..=507).collect::<Vec<_>>());
		assert_eq!(reopened.last_index(), Some(LogIndex::new(8)));
	}

	#[test]
	fn a_rebase_pulls_a_pinned_reader_back_to_the_start() {
		// the reader's version points at a record that no longer exists, and the next records carry
		// the leader's versions, which need not be higher; leaving it would skip all of them.
		let (fs, _, mut partition) = rolled();
		partition.register("sub").unwrap();
		partition.record("sub", LogVersion::new(507)).unwrap();

		partition.rebase(LogIndex::new(20), Term::new(5)).unwrap();

		assert_eq!(version_of(&fs, Path::new(DIR), "sub").unwrap(), LogVersion::ZERO);
	}
}
