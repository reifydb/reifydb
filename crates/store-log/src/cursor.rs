// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::path::{Path, PathBuf};

use reifydb_codec::log::{LogVersion, Position, record::Record};
use reifydb_runtime::io::fs::{Filesystem, Len, Open, ReadDir};

use crate::{
	error::{LogError, Result},
	index::{position_of, read},
	partition::{bases_in, index_name, log_name},
	segment::{Stop, scan_upto},
};

const SEEK_BATCH: usize = 64;

pub struct Cursor<'a, F: Filesystem> {
	fs: &'a F,
	dir: PathBuf,
	bases: Vec<LogVersion>,
	at: usize,
	position: Position,
	last: Option<LogVersion>,
}

impl<'a, F: Filesystem + Open + ReadDir> Cursor<'a, F> {
	pub fn open(fs: &'a F, dir: &Path, after: LogVersion) -> Result<Self> {
		let bases = bases_in(fs, dir)?;
		let Some(oldest) = bases.first().copied() else {
			return Err(LogError::NotFound(dir.to_path_buf()));
		};
		if after != LogVersion::ZERO && after < oldest {
			return Err(LogError::Purged {
				dir: dir.to_path_buf(),
				requested: after,
				oldest,
			});
		}
		let at = bases.partition_point(|base| *base <= after).saturating_sub(1);
		let path = dir.join(log_name(bases[at]));
		let (_, entries) = read(fs, &dir.join(index_name(bases[at])))?;
		let (position, last) = seek(fs, &path, position_of(&entries, after), after)?;
		Ok(Self {
			fs,
			dir: dir.to_path_buf(),
			bases,
			at,
			position,
			last,
		})
	}

	pub fn next_batch(&mut self, max: usize) -> Result<Vec<Record>> {
		let mut out = Vec::new();
		while out.len() < max {
			let path = self.dir.join(log_name(self.bases[self.at]));
			let scan = scan_upto(self.fs, &path, self.position, self.last, max - out.len())?;
			for record in &scan.records {
				self.position = self.position.advance(record.encoded_len() as u64);
				self.last = Some(record.version);
			}
			out.extend(scan.records);
			match scan.stop {
				Stop::Limit => continue,
				Stop::Unwritten | Stop::Eof => {
					if !self.followed()? {
						break;
					}
					self.at += 1;
					self.position = Position::ZERO;
				}
				Stop::Corrupt(_) | Stop::Stale(_) => {
					if self.followed()? {
						return Err(LogError::SegmentIncomplete {
							end: scan.end,
							len: self.fs.open(&path)?.len()?,
							path,
						});
					}
					break;
				}
			}
		}
		Ok(out)
	}

	pub fn version(&self) -> Option<LogVersion> {
		self.last
	}

	pub fn base(&self) -> LogVersion {
		self.bases[self.at]
	}

	pub fn position(&self) -> Position {
		self.position
	}

	fn followed(&mut self) -> Result<bool> {
		if self.at + 1 < self.bases.len() {
			return Ok(true);
		}
		let bases = bases_in(self.fs, &self.dir)?;
		let base = self.bases[self.at];
		let Some(at) = bases.iter().position(|found| *found == base) else {
			return Err(LogError::Purged {
				dir: self.dir.clone(),
				requested: self.last.unwrap_or(base),
				oldest: bases.first().copied().unwrap_or(base),
			});
		};
		self.bases = bases;
		self.at = at;
		Ok(self.at + 1 < self.bases.len())
	}
}

fn seek<F: Filesystem + Open>(
	fs: &F,
	path: &Path,
	from: Position,
	after: LogVersion,
) -> Result<(Position, Option<LogVersion>)> {
	let mut at = from;
	let mut last = None;
	loop {
		let scan = scan_upto(fs, path, at, last, SEEK_BATCH)?;
		for record in &scan.records {
			if record.version > after {
				return Ok((at, last));
			}
			at = at.advance(record.encoded_len() as u64);
			last = Some(record.version);
		}
		if scan.stop != Stop::Limit {
			return Ok((at, last));
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::log::{LogIndex, RecordKind, Term};
	use reifydb_runtime::{
		context::clock::{Clock, MockClock},
		io::fs::{Mkdir, memory::MemoryFs},
	};
	use reifydb_value::{
		byte_size::ByteSize,
		value::{datetime::DateTime, duration::Duration},
	};

	use super::*;
	use crate::partition::{Config, Partition};

	const DIR: &str = "/log/p0";
	const BASE: LogVersion = LogVersion::new(500);

	fn config() -> Config {
		Config {
			segment_bytes: ByteSize::from_bytes(4096),
			segment_age: Duration::from_seconds_const(60),
			index_interval: ByteSize::from_bytes(64),
		}
	}

	fn record(version: u64, payload: &[u8]) -> Record {
		record_at(version, version - BASE.as_u64() + 1, payload)
	}

	fn record_at(version: u64, index: u64, payload: &[u8]) -> Record {
		Record::new(
			LogVersion::new(version),
			LogIndex::new(index),
			Term::new(1),
			DateTime::from_bits(1000 + version),
			RecordKind::new(0),
			payload.to_vec(),
		)
	}

	fn versions(records: &[Record]) -> Vec<u64> {
		records.iter().map(|record| record.version.as_u64()).collect()
	}

	fn fixture(count: u64) -> (MemoryFs, Partition<MemoryFs, Clock>) {
		let fs = MemoryFs::new();
		fs.mkdir(Path::new("/log")).unwrap();
		let mut partition = Partition::create(
			fs.clone(),
			Clock::Mock(MockClock::from_millis(1_000)),
			Path::new(DIR),
			config(),
			BASE,
			LogIndex::new(1),
		)
		.unwrap();
		for step in 0..count {
			partition.append(&record(BASE.as_u64() + step, &[step as u8; 40])).unwrap();
		}
		partition.sync().unwrap();
		(fs, partition)
	}

	#[test]
	fn a_cursor_from_zero_reads_every_record_in_order() {
		let (fs, _partition) = fixture(6);

		let mut cursor = Cursor::open(&fs, Path::new(DIR), LogVersion::ZERO).unwrap();

		assert_eq!(versions(&cursor.next_batch(10).unwrap()), vec![500, 501, 502, 503, 504, 505]);
	}

	#[test]
	fn a_batch_never_returns_more_than_it_was_asked_for() {
		// the cap is what keeps a reader off a 256 MiB segment in one allocation, so a batch
		// that quietly overruns it defeats the whole point of reading in batches.
		let (fs, _partition) = fixture(6);
		let mut cursor = Cursor::open(&fs, Path::new(DIR), LogVersion::ZERO).unwrap();

		let first = cursor.next_batch(2).unwrap();
		let second = cursor.next_batch(2).unwrap();
		let rest = cursor.next_batch(10).unwrap();

		assert_eq!(versions(&first), vec![500, 501]);
		assert_eq!(versions(&second), vec![502, 503]);
		assert_eq!(versions(&rest), vec![504, 505]);
	}

	#[test]
	fn a_cursor_resumes_strictly_above_the_version_it_was_opened_at() {
		// the hint records the last version the reader finished, so reopening at it must not
		// hand that record back; a reader that reprocesses its last record on every restart
		// is a duplicate-delivery bug.
		let (fs, _partition) = fixture(6);

		let mut cursor = Cursor::open(&fs, Path::new(DIR), LogVersion::new(502)).unwrap();

		assert_eq!(versions(&cursor.next_batch(10).unwrap()), vec![503, 504, 505]);
	}

	#[test]
	fn a_caught_up_cursor_returns_nothing_and_stays_where_it_is() {
		let (fs, _partition) = fixture(3);
		let mut cursor = Cursor::open(&fs, Path::new(DIR), LogVersion::ZERO).unwrap();
		cursor.next_batch(10).unwrap();

		let again = cursor.next_batch(10).unwrap();

		assert!(again.is_empty());
		assert_eq!(cursor.version(), Some(LogVersion::new(502)));
	}

	#[test]
	fn a_cursor_picks_up_records_appended_after_it_caught_up() {
		// a reader that has drained the log must not need reopening to see the next write,
		// or every idle reader stalls until something else restarts it.
		let (fs, mut partition) = fixture(3);
		let mut cursor = Cursor::open(&fs, Path::new(DIR), LogVersion::ZERO).unwrap();
		assert_eq!(versions(&cursor.next_batch(10).unwrap()), vec![500, 501, 502]);

		partition.append(&record(503, b"later")).unwrap();
		partition.sync().unwrap();

		assert_eq!(versions(&cursor.next_batch(10).unwrap()), vec![503]);
	}

	#[test]
	fn a_cursor_walks_from_a_sealed_segment_into_the_one_after_it() {
		// crossing the boundary is the only place the cursor changes files, and a reader that
		// stops at the seal silently loses every record written after the roll.
		let (fs, mut partition) = fixture(6);
		partition.seal().unwrap();
		partition.append(&record_at(600, 7, b"next")).unwrap();
		partition.sync().unwrap();
		assert_eq!(partition.bases().len(), 2);

		let mut cursor = Cursor::open(&fs, Path::new(DIR), LogVersion::ZERO).unwrap();

		assert_eq!(versions(&cursor.next_batch(20).unwrap()), vec![500, 501, 502, 503, 504, 505, 600]);
	}

	#[test]
	fn a_batch_boundary_inside_a_sealed_segment_still_crosses_it() {
		// the roll happens inside next_batch, so a cap that lands exactly on the seal must not
		// leave the cursor parked on a segment that will never grow again.
		let (fs, mut partition) = fixture(6);
		partition.seal().unwrap();
		partition.append(&record_at(600, 7, b"next")).unwrap();
		partition.sync().unwrap();
		let mut cursor = Cursor::open(&fs, Path::new(DIR), LogVersion::ZERO).unwrap();

		assert_eq!(versions(&cursor.next_batch(6).unwrap()), vec![500, 501, 502, 503, 504, 505]);
		assert_eq!(versions(&cursor.next_batch(6).unwrap()), vec![600]);
	}

	#[test]
	fn a_version_below_the_oldest_surviving_segment_is_refused() {
		// answering with whatever survived would hide the gap, and the reader would carry on
		// believing it had seen every record between its hint and the first one it got.
		let (fs, mut partition) = fixture(6);
		partition.seal().unwrap();
		partition.append(&record_at(600, 7, b"next")).unwrap();
		partition.sync().unwrap();
		partition.purge(Duration::from_seconds_const(0)).unwrap();
		assert_eq!(partition.bases(), [LogVersion::new(600)]);

		let error = Cursor::open(&fs, Path::new(DIR), LogVersion::new(502)).err().unwrap();

		assert!(
			matches!(
				error,
				LogError::Purged {
					requested,
					oldest,
					..
				} if requested == LogVersion::new(502) && oldest == LogVersion::new(600)
			),
			"{error:?}"
		);
	}

	#[test]
	fn a_cursor_opened_from_a_readers_hint_starts_where_that_reader_stopped() {
		let (_fs, partition) = fixture(6);
		partition.register("flow-3").unwrap();
		partition.record("flow-3", LogVersion::new(503)).unwrap();

		let mut cursor = partition.cursor("flow-3").unwrap();

		assert_eq!(versions(&cursor.next_batch(10).unwrap()), vec![504, 505]);
	}
}
