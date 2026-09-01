// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashSet,
	path::{Path, PathBuf},
};

use reifydb_codec::log::{
	LogIndex, LogVersion, Position,
	index::{ENTRY_BYTES, Entry, HEADER_BYTES, Header, MAGIC, TimestampRange, decode_entry, encode_entry},
	record::Record,
};
use reifydb_runtime::io::fs::{
	Create, Filesystem, Len, Open, OpenMut, Pread, Rename, SyncData, SyncDir, Truncate, Unlink,
};
use reifydb_value::byte_size::ByteSize;

use crate::{
	error::{LogError, Result},
	segment::{Scan, discard, read_exact, scan_from, staging, write_all},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Recovery {
	pub read: usize,
	pub kept: usize,
	pub truncated: bool,
}

pub struct Index<F: Filesystem> {
	path: PathBuf,
	file: F::FileMut,
	header: Header,
	interval: ByteSize,
	entries: Vec<Entry>,
}

impl<F: Filesystem> Index<F> {
	pub fn create(
		fs: &F,
		path: &Path,
		base_version: LogVersion,
		base_index: LogIndex,
		interval: ByteSize,
	) -> Result<Self>
	where
		F: Create + Open + Rename + SyncDir + Unlink,
	{
		if fs.open(path).is_ok() {
			return Err(LogError::AlreadyExists(path.to_path_buf()));
		}
		let header = Header::new(base_version, base_index);
		let staging = staging(path);
		discard(fs, &staging)?;
		let file = fs.create(&staging, HEADER_BYTES as u64)?;
		write_all(&file, &staging, 0, &header.encode())?;
		file.sync_data()?;
		fs.rename(&staging, path)?;
		fs.sync_dir(parent(path))?;
		Ok(Self {
			path: path.to_path_buf(),
			file,
			header,
			interval,
			entries: Vec::new(),
		})
	}

	pub fn recover(fs: &F, path: &Path, interval: ByteSize, log: &Scan) -> Result<(Self, Recovery)>
	where
		F: OpenMut,
	{
		let file = fs.open_mut(path)?;
		let len = file.len()?;
		let (header, mut entries) = load(&file, path, len)?;
		let truncated = !(len - HEADER_BYTES as u64).is_multiple_of(ENTRY_BYTES as u64);
		let read = entries.len();
		let anchors = anchors(log);
		entries.retain(|entry| anchors.contains(&(entry.version, entry.index, entry.position)));
		let recovery = Recovery {
			read,
			kept: entries.len(),
			truncated,
		};
		let index = Self {
			path: path.to_path_buf(),
			file,
			header,
			interval,
			entries,
		};
		index.file.truncate(index.end())?;
		index.file.sync_data()?;
		Ok((index, recovery))
	}

	pub fn append(&mut self, version: LogVersion, index: LogIndex, position: Position) -> Result<bool> {
		if !self.due(position) {
			return Ok(false);
		}
		let entry = Entry {
			version,
			index,
			position,
		};
		let at = self.end();
		write_all(&self.file, &self.path, at, &encode_entry(&self.header, entry))?;
		self.entries.push(entry);
		Ok(true)
	}

	pub fn truncate_to(&mut self, index: LogIndex) -> Result<()> {
		self.entries.retain(|entry| entry.index < index);
		self.file.truncate(self.end())?;
		Ok(self.file.sync_data()?)
	}

	pub fn seal(&mut self, timestamps: Option<TimestampRange>) -> Result<()> {
		self.header.timestamps = timestamps;
		write_all(&self.file, &self.path, 0, &self.header.encode())?;
		Ok(self.file.sync_data()?)
	}

	pub fn sync(&self) -> Result<()> {
		Ok(self.file.sync_data()?)
	}

	pub fn lookup(&self, version: LogVersion) -> Position {
		position_of(&self.entries, version)
	}

	pub fn entries(&self) -> &[Entry] {
		&self.entries
	}

	pub fn header(&self) -> Header {
		self.header
	}

	pub fn path(&self) -> &Path {
		&self.path
	}

	fn due(&self, position: Position) -> bool {
		match self.entries.last() {
			None => true,
			Some(last) => position.distance_from(last.position) >= self.interval.as_bytes(),
		}
	}

	fn end(&self) -> u64 {
		HEADER_BYTES as u64 + (self.entries.len() * ENTRY_BYTES) as u64
	}
}

pub fn read<F: Open>(fs: &F, path: &Path) -> Result<(Header, Vec<Entry>)> {
	let file = fs.open(path)?;
	let len = file.len()?;
	load(&file, path, len)
}

pub fn header<F: Open>(fs: &F, path: &Path) -> Result<Header> {
	let file = fs.open(path)?;
	let len = file.len()?;
	head(&file, path, len)
}

pub fn find<F: Open>(fs: &F, path: &Path, entries: &[Entry], version: LogVersion) -> Result<Option<Record>> {
	let scan = scan_from(fs, path, position_of(entries, version))?;
	Ok(scan.records.into_iter().find(|record| record.version == version))
}

pub fn position_of(entries: &[Entry], version: LogVersion) -> Position {
	at_or_before(entries.partition_point(|entry| entry.version <= version), entries)
}

pub fn find_at<F: Open>(fs: &F, path: &Path, entries: &[Entry], index: LogIndex) -> Result<Option<Record>> {
	let scan = scan_from(fs, path, position_of_index(entries, index))?;
	Ok(scan.records.into_iter().find(|record| record.index == index))
}

pub fn position_of_index(entries: &[Entry], index: LogIndex) -> Position {
	at_or_before(entries.partition_point(|entry| entry.index <= index), entries)
}

fn at_or_before(at: usize, entries: &[Entry]) -> Position {
	if at == 0 {
		Position::ZERO
	} else {
		entries[at - 1].position
	}
}

fn head<H: Pread>(file: &H, path: &Path, len: u64) -> Result<Header> {
	if len < HEADER_BYTES as u64 {
		return Err(LogError::IndexShort {
			path: path.to_path_buf(),
			len,
		});
	}
	let mut raw = [0u8; HEADER_BYTES];
	if !read_exact(file, 0, &mut raw)? {
		return Err(LogError::IndexShort {
			path: path.to_path_buf(),
			len,
		});
	}
	let header = Header::decode(&raw);
	if header.magic != MAGIC {
		return Err(LogError::IndexMagic {
			path: path.to_path_buf(),
			found: header.magic,
		});
	}
	Ok(header)
}

fn load<H: Pread>(file: &H, path: &Path, len: u64) -> Result<(Header, Vec<Entry>)> {
	let header = head(file, path, len)?;
	let count = ((len - HEADER_BYTES as u64) / ENTRY_BYTES as u64) as usize;
	let mut entries = Vec::with_capacity(count);
	for slot in 0..count {
		let mut raw = [0u8; ENTRY_BYTES];
		let at = HEADER_BYTES as u64 + (slot * ENTRY_BYTES) as u64;
		if !read_exact(file, at, &mut raw)? {
			break;
		}
		entries.push(decode_entry(&header, &raw));
	}
	Ok((header, entries))
}

fn anchors(log: &Scan) -> HashSet<(LogVersion, LogIndex, Position)> {
	let mut out = HashSet::with_capacity(log.records.len());
	let mut at = Position::ZERO;
	for record in &log.records {
		out.insert((record.version, record.index, at));
		at = at.advance(record.encoded_len() as u64);
	}
	out
}

fn parent(path: &Path) -> &Path {
	match path.parent() {
		Some(parent) if !parent.as_os_str().is_empty() => parent,
		_ => Path::new("."),
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::log::{RecordKind, Term};
	use reifydb_runtime::io::fs::{Mkdir, memory::MemoryFs};
	use reifydb_value::value::datetime::DateTime;

	use super::*;
	use crate::segment::{Segment, scan, sync_path};

	const BASE: LogVersion = LogVersion::new(100);
	const BASE_INDEX: LogIndex = LogIndex::new(1);
	const INTERVAL: ByteSize = ByteSize::from_bytes(64);

	fn fixture() -> (MemoryFs, PathBuf) {
		let fs = MemoryFs::new();
		fs.mkdir(Path::new("/log")).unwrap();
		(fs, PathBuf::from("/log/0.index"))
	}

	fn version(offset: u64) -> LogVersion {
		LogVersion::new(BASE.as_u64() + offset)
	}

	fn index_at(offset: u64) -> LogIndex {
		LogIndex::new(BASE_INDEX.as_u64() + offset)
	}

	#[test]
	fn a_fresh_index_carries_the_magic_and_its_base_version() {
		// the header is how a later build recognises the file at all, so it is asserted by value.
		let (fs, path) = fixture();
		let index = Index::create(&fs, &path, BASE, BASE_INDEX, INTERVAL).unwrap();

		assert_eq!(index.header(), Header::new(BASE, BASE_INDEX));
		assert_eq!(read(&fs, &path).unwrap(), (Header::new(BASE, BASE_INDEX), vec![]));
	}

	#[test]
	fn an_appended_entry_survives_a_reread() {
		// an entry is stored as a delta against the base, so a roundtrip is the only proof it decodes back.
		let (fs, path) = fixture();
		let mut index = Index::create(&fs, &path, BASE, BASE_INDEX, INTERVAL).unwrap();
		assert!(index.append(BASE, BASE_INDEX, Position::ZERO).unwrap());
		assert!(index.append(version(9), index_at(1), Position::new(512)).unwrap());
		index.sync().unwrap();

		let (_, entries) = read(&fs, &path).unwrap();

		assert_eq!(
			entries,
			vec![
				Entry {
					version: BASE,
					index: BASE_INDEX,
					position: Position::ZERO
				},
				Entry {
					version: version(9),
					index: index_at(1),
					position: Position::new(512)
				}
			]
		);
	}

	#[test]
	fn a_lookup_never_lands_above_the_version_it_was_given() {
		// landing above the target skips the record, and the forward scan can only move forwards.
		let entries = pair();

		assert_eq!(position_of(&entries, LogVersion::new(5)), Position::ZERO);
		assert_eq!(position_of(&entries, LogVersion::new(10)), Position::ZERO);
		assert_eq!(position_of(&entries, LogVersion::new(19)), Position::ZERO);
		assert_eq!(position_of(&entries, LogVersion::new(20)), Position::new(800));
		assert_eq!(position_of(&entries, LogVersion::new(999)), Position::new(800));
	}

	#[test]
	fn recovery_drops_an_entry_the_log_does_not_back() {
		// an entry written before the record it points at survived the crash would seek into a hole.
		let fs = MemoryFs::new();
		fs.mkdir(Path::new("/log")).unwrap();
		let log = PathBuf::from("/log/0.log");
		let path = PathBuf::from("/log/0.index");
		let mut segment = Segment::create(&fs, &log, ByteSize::from_bytes(4096)).unwrap();
		segment.append(&Record::new(
			BASE,
			BASE_INDEX,
			Term::new(1),
			DateTime::from_bits(1),
			RecordKind::new(0),
			b"a".to_vec(),
		))
		.unwrap();
		sync_path(&fs, &log).unwrap();
		let mut index = Index::create(&fs, &path, BASE, BASE_INDEX, INTERVAL).unwrap();
		index.append(BASE, BASE_INDEX, Position::ZERO).unwrap();
		index.append(version(1), index_at(1), Position::new(2048)).unwrap();
		index.sync().unwrap();

		let scanned = scan(&fs, &log).unwrap();
		let (recovered, recovery) = Index::<MemoryFs>::recover(&fs, &path, INTERVAL, &scanned).unwrap();

		assert_eq!(recovery.read, 2);
		assert_eq!(recovery.kept, 1);
		assert_eq!(
			recovered.entries(),
			[Entry {
				version: BASE,
				index: BASE_INDEX,
				position: Position::ZERO
			}]
		);
	}

	fn pair() -> Vec<Entry> {
		vec![
			Entry {
				version: LogVersion::new(10),
				index: LogIndex::new(1),
				position: Position::ZERO,
			},
			Entry {
				version: LogVersion::new(20),
				index: LogIndex::new(7),
				position: Position::new(800),
			},
		]
	}
}
