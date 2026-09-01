// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::path::{Path, PathBuf};

use reifydb_codec::log::{
	LogVersion, Position,
	record::{HEADER_BYTES, Header, Record},
};
use reifydb_runtime::io::fs::{
	Create, Filesystem, FsError, Len, Open, OpenMut, Pread, Pwrite, Rename, SyncData, SyncDir, Truncate, Unlink,
};
use reifydb_value::byte_size::ByteSize;

use crate::error::{LogError, Result};

pub const STAGING_SUFFIX: &str = ".staging";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stop {
	Unwritten,
	Eof,
	Corrupt(Position),
	Stale(Position),
	Limit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scan {
	pub records: Vec<Record>,
	pub end: Position,
	pub stop: Stop,
}

pub struct Segment<F: Filesystem> {
	path: PathBuf,
	file: F::FileMut,
	capacity: ByteSize,
	head: Position,
}

impl<F: Filesystem> Segment<F> {
	pub fn create(fs: &F, path: &Path, capacity: ByteSize) -> Result<Self>
	where
		F: Create + Open + Rename + SyncDir + Unlink,
	{
		if fs.open(path).is_ok() {
			return Err(LogError::AlreadyExists(path.to_path_buf()));
		}
		let staging = staging(path);
		discard(fs, &staging)?;
		let file = fs.create(&staging, capacity.as_bytes())?;
		file.sync_data()?;
		fs.rename(&staging, path)?;
		fs.sync_dir(parent(path))?;
		Ok(Self {
			path: path.to_path_buf(),
			file,
			capacity,
			head: Position::ZERO,
		})
	}

	pub fn recover(fs: &F, path: &Path) -> Result<(Self, Scan)>
	where
		F: OpenMut,
	{
		let file = fs.open_mut(path)?;
		let capacity = ByteSize::from_bytes(file.len()?);
		let scan = walk(&file, capacity, Position::ZERO, None, None)?;
		let segment = Self {
			path: path.to_path_buf(),
			file,
			capacity,
			head: scan.end,
		};
		Ok((segment, scan))
	}

	pub fn append(&mut self, record: &Record) -> Result<Position> {
		let bytes = record.encode();
		let needed = bytes.len() as u64;
		let remaining = self.capacity.as_bytes() - self.head.as_u64();
		if remaining < needed {
			return Err(LogError::SegmentFull {
				path: self.path.clone(),
				needed,
				remaining,
			});
		}
		let offset = self.head;
		write_all(&self.file, &self.path, offset.as_u64(), &bytes)?;
		self.head = self.head.advance(needed);
		Ok(offset)
	}

	pub fn reset(&mut self, capacity: ByteSize) -> Result<()> {
		self.file.truncate(0)?;
		self.file.truncate(capacity.as_bytes())?;
		self.file.sync_data()?;
		self.head = Position::ZERO;
		self.capacity = capacity;
		Ok(())
	}

	pub fn truncate_to(&mut self, position: Position) -> Result<()> {
		self.file.truncate(position.as_u64())?;
		self.file.sync_data()?;
		self.head = position;
		self.capacity = ByteSize::from_bytes(position.as_u64());
		Ok(())
	}

	pub fn seal(&mut self) -> Result<()> {
		self.capacity = ByteSize::from_bytes(self.head.as_u64());
		self.file.truncate(self.head.as_u64())?;
		Ok(self.file.sync_data()?)
	}

	pub fn path(&self) -> &Path {
		&self.path
	}

	pub fn head(&self) -> Position {
		self.head
	}

	pub fn capacity(&self) -> ByteSize {
		self.capacity
	}
}

pub fn scan<F: Open>(fs: &F, path: &Path) -> Result<Scan> {
	scan_from(fs, path, Position::ZERO)
}

pub fn scan_from<F: Open>(fs: &F, path: &Path, from: Position) -> Result<Scan> {
	let file = fs.open(path)?;
	let capacity = ByteSize::from_bytes(file.len()?);
	walk(&file, capacity, from, None, None)
}

pub fn scan_upto<F: Open>(
	fs: &F,
	path: &Path,
	from: Position,
	after: Option<LogVersion>,
	limit: usize,
) -> Result<Scan> {
	let file = fs.open(path)?;
	let capacity = ByteSize::from_bytes(file.len()?);
	walk(&file, capacity, from, after, Some(limit))
}

fn walk<H: Pread>(
	file: &H,
	capacity: ByteSize,
	from: Position,
	after: Option<LogVersion>,
	limit: Option<usize>,
) -> Result<Scan> {
	let mut records = Vec::new();
	let mut position = from;
	let mut previous = after;
	loop {
		if limit.is_some_and(|max| records.len() >= max) {
			return Ok(stopped(records, position, Stop::Limit));
		}
		if capacity.as_bytes().saturating_sub(position.as_u64()) < HEADER_BYTES as u64 {
			return Ok(stopped(records, position, Stop::Eof));
		}
		let mut header = [0u8; HEADER_BYTES];
		if !read_exact(file, position.as_u64(), &mut header)? {
			return Ok(stopped(records, position, Stop::Eof));
		}
		let header = Header::decode(&header);
		if header.is_end() {
			return Ok(stopped(records, position, Stop::Unwritten));
		}
		let Some(payload_len) = header.payload_len() else {
			return Ok(stopped(records, position, Stop::Corrupt(position)));
		};
		let total = HEADER_BYTES as u64 + payload_len as u64;
		if capacity.as_bytes().saturating_sub(position.as_u64()) < total {
			return Ok(stopped(records, position, Stop::Corrupt(position)));
		}
		let mut payload = vec![0u8; payload_len];
		read_exact(file, position.as_u64() + HEADER_BYTES as u64, &mut payload)?;
		if !header.verify(&payload) {
			return Ok(stopped(records, position, Stop::Corrupt(position)));
		}
		let record = header.into_record(payload);
		if previous.is_some_and(|earlier| record.version <= earlier) {
			return Ok(stopped(records, position, Stop::Stale(position)));
		}
		previous = Some(record.version);
		records.push(record);
		position = position.advance(total);
	}
}

fn stopped(records: Vec<Record>, end: Position, stop: Stop) -> Scan {
	Scan {
		records,
		end,
		stop,
	}
}

pub(crate) fn read_exact<H: Pread>(file: &H, mut offset: u64, buf: &mut [u8]) -> Result<bool> {
	let mut read = 0;
	while read < buf.len() {
		let n = file.pread(offset, &mut buf[read..])?;
		if n == 0 {
			return Ok(false);
		}
		read += n;
		offset += n as u64;
	}
	Ok(true)
}

pub(crate) fn write_all<H: Pwrite>(file: &H, path: &Path, mut offset: u64, buf: &[u8]) -> Result<()> {
	let mut written = 0;
	while written < buf.len() {
		let n = file.pwrite(offset, &buf[written..])?;
		if n == 0 {
			return Err(LogError::Io {
				path: path.to_path_buf(),
				message: format!("write made no progress at offset {offset}"),
			});
		}
		written += n;
		offset += n as u64;
	}
	Ok(())
}

pub fn sync_path<F: Filesystem + OpenMut>(fs: &F, path: &Path) -> Result<()> {
	Ok(fs.open_mut(path)?.sync_data()?)
}

pub fn staging(path: &Path) -> PathBuf {
	let mut name = path.as_os_str().to_os_string();
	name.push(STAGING_SUFFIX);
	PathBuf::from(name)
}

pub fn discard<F: Unlink>(fs: &F, path: &Path) -> Result<()> {
	match fs.unlink(path) {
		Err(FsError::NotFound(_)) => Ok(()),
		other => Ok(other?),
	}
}

fn parent(path: &Path) -> &Path {
	match path.parent() {
		Some(parent) if !parent.as_os_str().is_empty() => parent,
		_ => Path::new("."),
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::log::{LogIndex, LogVersion, RecordKind, Term};
	use reifydb_runtime::io::fs::{Mkdir, memory::MemoryFs};
	use reifydb_value::value::datetime::DateTime;

	use super::*;

	const CAPACITY: ByteSize = ByteSize::from_bytes(4096);

	fn fixture() -> (MemoryFs, PathBuf) {
		let fs = MemoryFs::new();
		fs.mkdir(Path::new("/log")).unwrap();
		(fs, PathBuf::from("/log/0.log"))
	}

	fn record(version: u64, payload: &[u8]) -> Record {
		Record::new(
			LogVersion::new(version),
			LogIndex::new(version),
			Term::new(1),
			DateTime::from_bits(1000 + version),
			RecordKind::new(0),
			payload.to_vec(),
		)
	}

	fn poke(fs: &MemoryFs, path: &Path, offset: u64, bytes: &[u8]) {
		let file = fs.open_mut(path).unwrap();
		assert_eq!(file.pwrite(offset, bytes).unwrap(), bytes.len());
	}

	#[test]
	fn a_fresh_segment_scans_as_empty_and_terminates_on_unwritten_bytes() {
		// create preallocates zeros, so the very first header must read as the end of the
		// written region rather than as a record or as a corruption.
		let (fs, path) = fixture();
		Segment::create(&fs, &path, CAPACITY).unwrap();

		let scan = scan(&fs, &path).unwrap();

		assert_eq!(scan.records, vec![]);
		assert_eq!(scan.end, Position::ZERO);
		assert_eq!(scan.stop, Stop::Unwritten);
	}

	#[test]
	fn appended_records_scan_back_in_order_and_byte_identical() {
		let (fs, path) = fixture();
		let mut segment = Segment::create(&fs, &path, CAPACITY).unwrap();
		let written = vec![record(1, b"alpha"), record(2, b""), record(3, &[0xffu8; 300])];
		for entry in &written {
			segment.append(entry).unwrap();
		}
		sync_path(&fs, &path).unwrap();

		let scan = scan(&fs, &path).unwrap();

		assert_eq!(scan.records, written);
		assert_eq!(scan.stop, Stop::Unwritten);
	}

	#[test]
	fn a_flipped_payload_bit_stops_the_scan_at_that_record() {
		let (fs, path) = fixture();
		let mut segment = Segment::create(&fs, &path, CAPACITY).unwrap();
		let first = record(1, b"survivor");
		segment.append(&first).unwrap();
		let second_at = segment.append(&record(2, b"victim")).unwrap();
		sync_path(&fs, &path).unwrap();
		let mut byte = [0u8; 1];
		fs.open(&path).unwrap().pread(second_at.as_u64() + HEADER_BYTES as u64, &mut byte).unwrap();
		poke(&fs, &path, second_at.as_u64() + HEADER_BYTES as u64, &[byte[0] ^ 0x01]);

		let scan = scan(&fs, &path).unwrap();

		assert_eq!(scan.records, vec![first]);
		assert_eq!(scan.stop, Stop::Corrupt(second_at));
	}

	#[test]
	fn recover_is_a_fixed_point() {
		// I7: a second recovery pass must not move the head, or a crash during recovery
		// would eat one more record on every restart.
		let (fs, path) = fixture();
		let mut segment = Segment::create(&fs, &path, CAPACITY).unwrap();
		segment.append(&record(1, b"a")).unwrap();
		segment.append(&record(2, b"b")).unwrap();
		let torn_at = segment.append(&record(3, b"c")).unwrap();
		sync_path(&fs, &path).unwrap();
		poke(&fs, &path, torn_at.as_u64() + 8, &[0xff]);

		let (first_pass, first_scan) = Segment::<MemoryFs>::recover(&fs, &path).unwrap();
		let (second_pass, second_scan) = Segment::<MemoryFs>::recover(&fs, &path).unwrap();

		assert_eq!(first_scan.records, second_scan.records);
		assert_eq!(first_scan.end, second_scan.end);
		assert_eq!(first_pass.head(), second_pass.head());
		assert_eq!(first_pass.head(), torn_at);
		assert_eq!(second_scan.stop, Stop::Corrupt(torn_at));
	}
}
