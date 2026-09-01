// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::path::{Path, PathBuf};

use reifydb_codec::log::{
	LogIndex, LogVersion, Position,
	index::{DEFAULT_INTERVAL, TimestampRange},
	record::Record,
};
use reifydb_runtime::io::fs::{
	Create, Filesystem, FsError, Len, Mkdir, Open, OpenMut, ReadDir, Rename, SyncDir, Unlink,
};
use reifydb_value::{
	byte_size::ByteSize,
	clock::ClockNow,
	value::{datetime::DateTime, duration::Duration},
};

use crate::{
	error::{LogError, Result},
	index::Index,
	segment::{STAGING_SUFFIX, Scan, Segment, scan},
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
	segment: Segment<F>,
	index: Index<F>,
	timestamps: Option<TimestampRange>,
	head: Option<LogVersion>,
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
		let (segment, index) = new_pair(&fs, dir, config, base, base_index)?;
		Ok(Self {
			fs,
			clock,
			dir: dir.to_path_buf(),
			config,
			bases: vec![base],
			segment,
			index,
			timestamps: None,
			head: None,
			opened_at,
		})
	}

	pub fn open(fs: F, clock: C, dir: &Path, config: Config) -> Result<(Self, Scan)> {
		sweep_staging(&fs, dir)?;
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
			let head = head_of(&fs, dir, &bases, scan.records.last().map(|record| record.version))?;
			let partition = Self {
				fs,
				clock,
				dir: dir.to_path_buf(),
				config,
				bases,
				segment,
				index,
				timestamps,
				head,
				opened_at,
			};
			return Ok((partition, scan));
		}
	}

	pub fn append(&mut self, record: &Record) -> Result<Position> {
		if self.must_roll(record) {
			self.roll(record.version, record.index)?;
		}
		let position = self.segment.append(record)?;
		self.index.append(record.version, record.index, position)?;
		self.timestamps = widen(self.timestamps, record.timestamp);
		self.head = Some(record.version);
		Ok(position)
	}

	pub fn seal(&mut self) -> Result<()> {
		self.index.seal(self.timestamps)?;
		self.segment.seal()
	}

	pub fn sync(&self) -> Result<()> {
		self.segment.sync()?;
		self.index.sync()
	}

	pub fn records(&self) -> Result<Vec<Record>> {
		let mut out: Vec<Record> = Vec::new();
		let active = self.bases.len() - 1;
		for (at, base) in self.bases.iter().enumerate() {
			let path = self.dir.join(log_name(*base));
			let scanned = scan(&self.fs, &path)?;
			if at < active {
				let len = self.fs.open(&path)?.len()?;
				if scanned.end.as_u64() != len {
					return Err(LogError::SegmentIncomplete {
						path,
						end: scanned.end,
						len,
					});
				}
			}
			if let (Some(previous), Some(first)) = (out.last(), scanned.records.first())
				&& first.version <= previous.version
			{
				return Err(LogError::SegmentOutOfOrder {
					path,
					previous: previous.version,
					found: first.version,
				});
			}
			out.extend(scanned.records);
		}
		Ok(out)
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

	pub fn timestamps(&self) -> Option<TimestampRange> {
		self.timestamps
	}

	pub fn head(&self) -> Option<LogVersion> {
		self.head
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
		self.timestamps = None;
		self.opened_at = self.clock.now();
		Ok(())
	}
}

pub fn log_name(base: LogVersion) -> String {
	format!("{:0width$}{}", base.as_u64(), LOG_SUFFIX, width = NAME_DIGITS)
}

pub fn index_name(base: LogVersion) -> String {
	format!("{:0width$}{}", base.as_u64(), INDEX_SUFFIX, width = NAME_DIGITS)
}

pub fn base_of(name: &str) -> Option<LogVersion> {
	let digits = name.strip_suffix(LOG_SUFFIX)?;
	if digits.len() != NAME_DIGITS {
		return None;
	}
	digits.parse().ok().map(LogVersion::new)
}

fn bases_in<F: ReadDir>(fs: &F, dir: &Path) -> Result<Vec<LogVersion>> {
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

fn sweep_staging<F: ReadDir + Unlink>(fs: &F, dir: &Path) -> Result<()> {
	for path in fs.read_dir(dir)? {
		if path.as_os_str().as_encoded_bytes().ends_with(STAGING_SUFFIX.as_bytes()) {
			remove(fs, &path)?;
		}
	}
	Ok(())
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

fn head_of<F: Filesystem + Open>(
	fs: &F,
	dir: &Path,
	bases: &[LogVersion],
	active: Option<LogVersion>,
) -> Result<Option<LogVersion>> {
	if active.is_some() {
		return Ok(active);
	}
	for base in bases.iter().rev().skip(1) {
		let scanned = scan(fs, &dir.join(log_name(*base)))?;
		if let Some(record) = scanned.records.last() {
			return Ok(Some(record.version));
		}
	}
	Ok(None)
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
	use reifydb_codec::log::{RecordKind, Term};
	use reifydb_runtime::{
		context::clock::{Clock, MockClock},
		io::fs::{Len, Open, memory::MemoryFs},
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
		partition.sync().unwrap();

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
}
