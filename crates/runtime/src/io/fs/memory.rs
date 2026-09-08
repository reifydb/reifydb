// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet},
	path::{Path, PathBuf},
	sync::Arc,
};

use crate::{
	io::fs::{
		Create, Filesystem, FsError, Len, Mkdir, Open, OpenMut, Pread, Pwrite, ReadDir, Rename, Result,
		SyncData, SyncDir, Truncate, Unlink,
	},
	sync::mutex::Mutex,
};

pub const SECTOR_BYTES: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SectorState {
	Unwritten,
	Dirty,
	Durable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SectorMask(Vec<bool>);

impl SectorMask {
	pub fn full(sectors: usize) -> Self {
		Self(vec![true; sectors])
	}

	pub fn empty(sectors: usize) -> Self {
		Self(vec![false; sectors])
	}

	pub fn set(&mut self, sector: usize) {
		if sector < self.0.len() {
			self.0[sector] = true;
		}
	}

	pub fn clear(&mut self, sector: usize) {
		if sector < self.0.len() {
			self.0[sector] = false;
		}
	}

	pub fn is_set(&self, sector: usize) -> bool {
		self.0.get(sector).copied().unwrap_or(false)
	}

	pub fn sectors(&self) -> usize {
		self.0.len()
	}
}

pub(crate) fn sector_span(offset: u64, len: usize, sector_bytes: usize) -> usize {
	if len == 0 {
		return 0;
	}
	let first = offset as usize / sector_bytes;
	let last = (offset as usize + len - 1) / sector_bytes;
	last - first + 1
}

struct FileData {
	bytes: Vec<u8>,
	sectors: Vec<SectorState>,
	shadow: BTreeMap<usize, (SectorState, Vec<u8>)>,
}

impl FileData {
	fn new(len: usize, sector_bytes: usize) -> Self {
		Self {
			bytes: vec![0u8; len],
			sectors: vec![SectorState::Unwritten; len.div_ceil(sector_bytes)],
			shadow: BTreeMap::new(),
		}
	}

	fn resize(&mut self, len: usize, sector_bytes: usize) {
		self.bytes.resize(len, 0);
		self.sectors.resize(len.div_ceil(sector_bytes), SectorState::Unwritten);
		let sectors = self.sectors.len();
		self.shadow.retain(|sector, _| *sector < sectors);
	}

	fn dirty(&mut self, sector: usize, sector_bytes: usize) {
		if !self.shadow.contains_key(&sector) {
			let lo = sector * sector_bytes;
			let hi = (lo + sector_bytes).min(self.bytes.len());
			self.shadow.insert(sector, (self.sectors[sector], self.bytes[lo..hi].to_vec()));
		}
		self.sectors[sector] = SectorState::Dirty;
	}

	fn sync(&mut self) {
		for state in self.sectors.iter_mut() {
			if *state == SectorState::Dirty {
				*state = SectorState::Durable;
			}
		}
		self.shadow.clear();
	}

	#[cfg(any(test, feature = "testing"))]
	fn revert(&mut self, sector_bytes: usize) {
		while let Some((sector, (state, bytes))) = self.shadow.pop_first() {
			let lo = sector * sector_bytes;
			if lo >= self.bytes.len() {
				continue;
			}
			let hi = (lo + bytes.len()).min(self.bytes.len());
			self.bytes[lo..hi].copy_from_slice(&bytes[..hi - lo]);
			if sector < self.sectors.len() {
				self.sectors[sector] = state;
			}
		}
	}
}

pub(crate) struct MemoryFileState {
	sector_bytes: usize,
	data: Mutex<FileData>,
}

impl MemoryFileState {
	fn new(len: usize, sector_bytes: usize) -> Self {
		Self {
			sector_bytes,
			data: Mutex::new(FileData::new(len, sector_bytes)),
		}
	}

	fn pread(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
		let data = self.data.lock();
		let offset = offset as usize;
		if offset >= data.bytes.len() {
			return Ok(0);
		}
		let read = buf.len().min(data.bytes.len() - offset);
		buf[..read].copy_from_slice(&data.bytes[offset..offset + read]);
		Ok(read)
	}

	fn write_masked(&self, offset: u64, buf: &[u8], mask: &SectorMask) -> Result<usize> {
		if buf.is_empty() {
			return Ok(0);
		}
		let sector_bytes = self.sector_bytes;
		let offset = offset as usize;
		let end = offset + buf.len();
		let mut data = self.data.lock();
		if end > data.bytes.len() {
			data.resize(end, sector_bytes);
		}
		let first = offset / sector_bytes;
		let last = (end - 1) / sector_bytes;
		for sector in first..=last {
			if !mask.is_set(sector - first) {
				continue;
			}
			let lo = (sector * sector_bytes).max(offset);
			let hi = (sector * sector_bytes + sector_bytes).min(end);
			data.dirty(sector, sector_bytes);
			data.bytes[lo..hi].copy_from_slice(&buf[lo - offset..hi - offset]);
		}
		Ok(buf.len())
	}

	fn sync_data(&self) -> Result<()> {
		self.data.lock().sync();
		Ok(())
	}

	fn truncate(&self, len: u64) -> Result<()> {
		self.data.lock().resize(len as usize, self.sector_bytes);
		Ok(())
	}

	fn len(&self) -> Result<u64> {
		Ok(self.data.lock().bytes.len() as u64)
	}

	#[cfg(any(test, feature = "testing"))]
	fn revert(&self) {
		self.data.lock().revert(self.sector_bytes);
	}

	#[cfg(any(test, feature = "testing"))]
	fn sector_states(&self) -> Vec<SectorState> {
		self.data.lock().sectors.clone()
	}
}

struct State {
	files: BTreeMap<PathBuf, Arc<MemoryFileState>>,
	dirs: BTreeSet<PathBuf>,
	sector_bytes: usize,
}

impl Default for State {
	fn default() -> Self {
		Self {
			files: BTreeMap::new(),
			dirs: BTreeSet::from([PathBuf::from("/")]),
			sector_bytes: SECTOR_BYTES,
		}
	}
}

impl State {
	fn exists(&self, path: &Path) -> bool {
		self.files.contains_key(path) || self.dirs.contains(path)
	}

	fn parent_is_dir(&self, path: &Path) -> bool {
		match path.parent() {
			Some(parent) => self.dirs.contains(parent),
			None => false,
		}
	}
}

#[derive(Default)]
struct Inner {
	state: Mutex<State>,
}

#[derive(Clone, Default)]
pub struct MemoryFs(Arc<Inner>);

impl MemoryFs {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn with_sector_bytes(sector_bytes: usize) -> Self {
		Self(Arc::new(Inner {
			state: Mutex::new(State {
				sector_bytes,
				..State::default()
			}),
		}))
	}

	#[cfg(any(test, feature = "testing"))]
	pub fn crash(&self) {
		let files: Vec<Arc<MemoryFileState>> = self.0.state.lock().files.values().cloned().collect();
		for file in files {
			file.revert();
		}
	}

	#[cfg(feature = "testing")]
	pub(crate) fn detach(&self, path: &Path) -> Result<Arc<MemoryFileState>> {
		self.0.state.lock().files.remove(path).ok_or_else(|| FsError::NotFound(path.to_path_buf()))
	}

	#[cfg(feature = "testing")]
	pub(crate) fn attach(&self, path: &Path, state: Arc<MemoryFileState>) {
		self.0.state.lock().files.insert(path.to_path_buf(), state);
	}
}

pub struct MemoryFile {
	path: PathBuf,
	state: Arc<MemoryFileState>,
}

pub struct MemoryFileMut {
	path: PathBuf,
	state: Arc<MemoryFileState>,
}

impl MemoryFileMut {
	pub fn write_masked(&self, offset: u64, buf: &[u8], mask: &SectorMask) -> Result<usize> {
		self.state.write_masked(offset, buf, mask)
	}

	pub fn path(&self) -> &Path {
		&self.path
	}

	pub fn sector_bytes(&self) -> usize {
		self.state.sector_bytes
	}

	#[cfg(any(test, feature = "testing"))]
	pub fn sector_states(&self) -> Vec<SectorState> {
		self.state.sector_states()
	}
}

impl MemoryFile {
	pub fn path(&self) -> &Path {
		&self.path
	}

	pub fn sector_bytes(&self) -> usize {
		self.state.sector_bytes
	}

	#[cfg(any(test, feature = "testing"))]
	pub fn sector_states(&self) -> Vec<SectorState> {
		self.state.sector_states()
	}
}

impl Filesystem for MemoryFs {
	type File = MemoryFile;
	type FileMut = MemoryFileMut;
}

impl Mkdir for MemoryFs {
	fn mkdir(&self, path: &Path) -> Result<()> {
		let mut state = self.0.state.lock();
		if !state.parent_is_dir(path) {
			return Err(FsError::NotFound(path.to_path_buf()));
		}
		if state.exists(path) {
			return Err(FsError::AlreadyExists(path.to_path_buf()));
		}
		state.dirs.insert(path.to_path_buf());
		Ok(())
	}
}

impl Create for MemoryFs {
	fn create(&self, path: &Path, len: u64) -> Result<MemoryFileMut> {
		let mut state = self.0.state.lock();
		if !state.parent_is_dir(path) {
			return Err(FsError::NotFound(path.to_path_buf()));
		}
		if state.exists(path) {
			return Err(FsError::AlreadyExists(path.to_path_buf()));
		}
		let file = Arc::new(MemoryFileState::new(len as usize, state.sector_bytes));
		state.files.insert(path.to_path_buf(), Arc::clone(&file));
		Ok(MemoryFileMut {
			path: path.to_path_buf(),
			state: file,
		})
	}
}

impl Open for MemoryFs {
	fn open(&self, path: &Path) -> Result<MemoryFile> {
		let state = self.0.state.lock();
		state.files
			.get(path)
			.map(|file| MemoryFile {
				path: path.to_path_buf(),
				state: Arc::clone(file),
			})
			.ok_or_else(|| FsError::NotFound(path.to_path_buf()))
	}
}

impl OpenMut for MemoryFs {
	fn open_mut(&self, path: &Path) -> Result<MemoryFileMut> {
		let state = self.0.state.lock();
		state.files
			.get(path)
			.map(|file| MemoryFileMut {
				path: path.to_path_buf(),
				state: Arc::clone(file),
			})
			.ok_or_else(|| FsError::NotFound(path.to_path_buf()))
	}
}

impl ReadDir for MemoryFs {
	fn read_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
		let state = self.0.state.lock();
		if !state.dirs.contains(path) {
			if state.files.contains_key(path) {
				return Err(FsError::NotADirectory(path.to_path_buf()));
			}
			return Err(FsError::NotFound(path.to_path_buf()));
		}
		let mut entries: Vec<PathBuf> = state
			.files
			.keys()
			.chain(state.dirs.iter())
			.filter(|entry| entry.parent() == Some(path))
			.cloned()
			.collect();
		entries.sort();
		Ok(entries)
	}
}

impl Rename for MemoryFs {
	fn rename(&self, from: &Path, to: &Path) -> Result<()> {
		let mut state = self.0.state.lock();
		let Some(file) = state.files.remove(from) else {
			return Err(FsError::NotFound(from.to_path_buf()));
		};
		if !state.parent_is_dir(to) {
			state.files.insert(from.to_path_buf(), file);
			return Err(FsError::NotFound(to.to_path_buf()));
		}
		state.files.insert(to.to_path_buf(), file);
		Ok(())
	}
}

impl Unlink for MemoryFs {
	fn unlink(&self, path: &Path) -> Result<()> {
		let mut state = self.0.state.lock();
		state.files.remove(path).map(|_| ()).ok_or_else(|| FsError::NotFound(path.to_path_buf()))
	}
}

impl SyncDir for MemoryFs {
	fn sync_dir(&self, path: &Path) -> Result<()> {
		let state = self.0.state.lock();
		if !state.dirs.contains(path) {
			return Err(FsError::NotFound(path.to_path_buf()));
		}
		Ok(())
	}
}

impl Pread for MemoryFile {
	fn pread(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
		self.state.pread(offset, buf)
	}
}

impl Len for MemoryFile {
	fn len(&self) -> Result<u64> {
		self.state.len()
	}
}

impl Pread for MemoryFileMut {
	fn pread(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
		self.state.pread(offset, buf)
	}
}

impl Pwrite for MemoryFileMut {
	fn pwrite(&self, offset: u64, buf: &[u8]) -> Result<usize> {
		let mask = SectorMask::full(sector_span(offset, buf.len(), self.state.sector_bytes));
		self.state.write_masked(offset, buf, &mask)
	}
}

impl SyncData for MemoryFileMut {
	fn sync_data(&self) -> Result<()> {
		self.state.sync_data()
	}
}

impl Truncate for MemoryFileMut {
	fn truncate(&self, len: u64) -> Result<()> {
		self.state.truncate(len)
	}
}

impl Len for MemoryFileMut {
	fn len(&self) -> Result<u64> {
		self.state.len()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn read_all(file: &MemoryFileMut) -> Vec<u8> {
		let len = file.len().unwrap() as usize;
		let mut buf = vec![0u8; len];
		assert_eq!(file.pread(0, &mut buf).unwrap(), len);
		buf
	}

	#[test]
	fn create_leaves_every_sector_unwritten_and_zeroed() {
		// a fresh file owns no durable and no dirty state, so a crash must leave it exactly as created.
		let fs = MemoryFs::new();
		let file = fs.create(Path::new("/a"), 1024).unwrap();
		assert_eq!(file.sector_states(), vec![SectorState::Unwritten; 2]);
		assert_eq!(read_all(&file), vec![0u8; 1024]);
	}

	#[test]
	fn write_dirties_every_grazed_sector_whole() {
		// a device cannot make half a sector durable, so a one-byte overlap must dirty the entire sector.
		let fs = MemoryFs::new();
		let file = fs.create(Path::new("/a"), 2048).unwrap();
		assert_eq!(file.pwrite(500, &[7u8; 20]).unwrap(), 20);
		assert_eq!(
			file.sector_states(),
			vec![SectorState::Dirty, SectorState::Dirty, SectorState::Unwritten, SectorState::Unwritten]
		);
	}

	#[test]
	fn sync_promotes_every_dirty_sector() {
		// only a sync may turn dirty into durable; untouched sectors must stay unwritten.
		let fs = MemoryFs::new();
		let file = fs.create(Path::new("/a"), 2048).unwrap();
		file.pwrite(0, &[1u8; 600]).unwrap();
		file.sync_data().unwrap();
		assert_eq!(
			file.sector_states(),
			vec![
				SectorState::Durable,
				SectorState::Durable,
				SectorState::Unwritten,
				SectorState::Unwritten
			]
		);
	}

	#[test]
	fn write_masked_skips_unmasked_sectors_but_reports_full_length() {
		// a torn write must tell the caller it succeeded while only some sectors reached the platter.
		let fs = MemoryFs::new();
		let file = fs.create(Path::new("/a"), 1536).unwrap();
		let mut mask = SectorMask::empty(3);
		mask.set(0);
		mask.set(2);
		assert_eq!(file.write_masked(0, &[0xABu8; 1536], &mask).unwrap(), 1536);
		let bytes = read_all(&file);
		assert!(bytes[0..512].iter().all(|byte| *byte == 0xAB));
		assert!(bytes[512..1024].iter().all(|byte| *byte == 0));
		assert!(bytes[1024..1536].iter().all(|byte| *byte == 0xAB));
		assert_eq!(file.sector_states(), vec![SectorState::Dirty, SectorState::Unwritten, SectorState::Dirty]);
	}

	#[test]
	fn mask_ignores_out_of_range_indices() {
		// a mask shorter than the span reads as unset rather than panicking, so no fault plan aborts a run.
		let mut mask = SectorMask::empty(2);
		mask.set(5);
		assert_eq!(mask.sectors(), 2);
		assert!(!mask.is_set(5));
		let mut full = SectorMask::full(2);
		full.clear(1);
		assert!(full.is_set(0));
		assert!(!full.is_set(1));
	}

	#[test]
	fn pread_round_trips_clamps_and_stops_at_the_end() {
		// a short read at the end must report the count copied, and a read past the end is not an error.
		let fs = MemoryFs::new();
		let file = fs.create(Path::new("/a"), 10).unwrap();
		file.pwrite(0, &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]).unwrap();
		let mut buf = [0u8; 4];
		assert_eq!(file.pread(2, &mut buf).unwrap(), 4);
		assert_eq!(buf, [3, 4, 5, 6]);
		let mut tail = [0u8; 16];
		assert_eq!(file.pread(5, &mut tail).unwrap(), 5);
		assert_eq!(&tail[..5], &[6, 7, 8, 9, 10]);
		assert_eq!(file.pread(10, &mut tail).unwrap(), 0);
		assert_eq!(file.pread(9999, &mut tail).unwrap(), 0);
	}

	#[test]
	fn write_past_the_end_grows_bytes_and_sectors_together() {
		// the sector vector must track the byte vector, or a crash would index a sector that does not exist.
		let fs = MemoryFs::new();
		let file = fs.create(Path::new("/a"), 0).unwrap();
		assert!(file.sector_states().is_empty());
		assert_eq!(file.pwrite(1000, &[9u8; 10]).unwrap(), 10);
		assert_eq!(file.len().unwrap(), 1010);
		assert_eq!(file.sector_states(), vec![SectorState::Unwritten, SectorState::Dirty]);
		assert_eq!(fs.open(Path::new("/a")).unwrap().sector_states().len(), 2);
	}

	#[test]
	fn truncate_shortens_bytes_and_sector_states_together() {
		// a shrink must drop the sectors it removed, otherwise a crash would restore bytes past the new end.
		let fs = MemoryFs::new();
		let file = fs.create(Path::new("/a"), 2048).unwrap();
		file.pwrite(0, &[3u8; 2048]).unwrap();
		file.truncate(600).unwrap();
		assert_eq!(file.len().unwrap(), 600);
		assert_eq!(file.sector_states().len(), 2);
		file.truncate(100).unwrap();
		assert_eq!(file.sector_states().len(), 1);
		assert_eq!(read_all(&file), vec![3u8; 100]);
		file.truncate(1024).unwrap();
		assert_eq!(file.sector_states(), vec![SectorState::Dirty, SectorState::Unwritten]);
	}

	#[test]
	fn crash_reverts_dirty_sectors_to_their_prior_bytes_and_state() {
		// an unsynced write must vanish exactly, leaving the sector in the state it held before it went dirty.
		let fs = MemoryFs::new();
		let file = fs.create(Path::new("/a"), 1024).unwrap();
		file.pwrite(0, &[1u8; 512]).unwrap();
		file.sync_data().unwrap();
		file.pwrite(0, &[2u8; 512]).unwrap();
		file.pwrite(512, &[2u8; 512]).unwrap();
		fs.crash();
		let bytes = read_all(&file);
		assert!(bytes[0..512].iter().all(|byte| *byte == 1));
		assert!(bytes[512..1024].iter().all(|byte| *byte == 0));
		assert_eq!(file.sector_states(), vec![SectorState::Durable, SectorState::Unwritten]);
	}

	#[test]
	fn crash_after_truncate_drops_shadow_beyond_the_new_end() {
		// a shadow entry past the new end must not be replayed, or the crash would resurrect truncated bytes.
		let fs = MemoryFs::new();
		let file = fs.create(Path::new("/a"), 2048).unwrap();
		file.pwrite(1536, &[8u8; 512]).unwrap();
		file.truncate(512).unwrap();
		fs.crash();
		assert_eq!(file.len().unwrap(), 512);
		assert_eq!(file.sector_states(), vec![SectorState::Unwritten]);
	}

	#[test]
	fn sync_clears_the_shadow_so_a_later_crash_keeps_the_data() {
		// once a sync covers a sector the bytes are durable, so a crash after it must change nothing.
		let fs = MemoryFs::new();
		let file = fs.create(Path::new("/a"), 512).unwrap();
		file.pwrite(0, &[5u8; 512]).unwrap();
		file.sync_data().unwrap();
		fs.crash();
		assert_eq!(read_all(&file), vec![5u8; 512]);
		assert_eq!(file.sector_states(), vec![SectorState::Durable]);
	}

	#[test]
	fn sector_size_is_configurable_and_consistent() {
		// the sector size decides the blast radius of a torn write, so every handle must agree on it.
		let fs = MemoryFs::with_sector_bytes(64);
		let file = fs.create(Path::new("/a"), 100).unwrap();
		assert_eq!(file.sector_bytes(), 64);
		assert_eq!(file.sector_states().len(), 2);
		file.pwrite(0, &[1u8; 100]).unwrap();
		assert_eq!(file.sector_states(), vec![SectorState::Dirty, SectorState::Dirty]);

		let wide = MemoryFs::new();
		let file = wide.create(Path::new("/a"), 100).unwrap();
		assert_eq!(file.sector_bytes(), SECTOR_BYTES);
		file.pwrite(0, &[1u8; 100]).unwrap();
		assert_eq!(file.sector_states(), vec![SectorState::Dirty]);
	}

	#[test]
	fn mkdir_needs_a_parent_directory_and_rejects_an_existing_path() {
		// a tree that accepts an orphan path lets a walk see files under a parent that never existed.
		let fs = MemoryFs::new();
		assert_eq!(fs.mkdir(Path::new("/a/b")), Err(FsError::NotFound(PathBuf::from("/a/b"))));
		fs.mkdir(Path::new("/a")).unwrap();
		assert_eq!(fs.mkdir(Path::new("/a")), Err(FsError::AlreadyExists(PathBuf::from("/a"))));
		fs.create(Path::new("/a/f"), 0).unwrap();
		assert_eq!(fs.mkdir(Path::new("/a/f")), Err(FsError::AlreadyExists(PathBuf::from("/a/f"))));
	}

	#[test]
	fn create_needs_a_parent_directory_and_rejects_an_existing_path() {
		let fs = MemoryFs::new();
		assert!(fs.create(Path::new("/a/f"), 0).is_err());
		fs.mkdir(Path::new("/a")).unwrap();
		fs.create(Path::new("/a/f"), 0).unwrap();
		assert_eq!(fs.create(Path::new("/a/f"), 0).err(), Some(FsError::AlreadyExists(PathBuf::from("/a/f"))));
	}

	#[test]
	fn open_and_open_mut_report_a_missing_file() {
		let fs = MemoryFs::new();
		assert_eq!(fs.open(Path::new("/a")).err(), Some(FsError::NotFound(PathBuf::from("/a"))));
		assert_eq!(fs.open_mut(Path::new("/a")).err(), Some(FsError::NotFound(PathBuf::from("/a"))));
		fs.create(Path::new("/a"), 8).unwrap();
		let file = fs.open(Path::new("/a")).unwrap();
		assert_eq!(file.path(), Path::new("/a"));
		assert_eq!(file.len().unwrap(), 8);
		assert_eq!(file.sector_bytes(), SECTOR_BYTES);
		assert_eq!(file.sector_states(), vec![SectorState::Unwritten]);
	}

	#[test]
	fn open_shares_the_state_with_the_creating_handle() {
		// two handles onto one path are one file, so a write through either must be visible through the other.
		let fs = MemoryFs::new();
		let writer = fs.create(Path::new("/a"), 512).unwrap();
		writer.pwrite(0, &[4u8; 8]).unwrap();
		let reader = fs.open(Path::new("/a")).unwrap();
		let mut buf = [0u8; 8];
		assert_eq!(reader.pread(0, &mut buf).unwrap(), 8);
		assert_eq!(buf, [4u8; 8]);
		assert_eq!(reader.sector_states(), vec![SectorState::Dirty]);
	}

	#[test]
	fn read_dir_lists_direct_children_in_ascending_order() {
		// the walk order feeds seed replay, so it must be sorted rather than whatever a hash gives back.
		let fs = MemoryFs::new();
		fs.mkdir(Path::new("/d")).unwrap();
		fs.mkdir(Path::new("/d/sub")).unwrap();
		fs.create(Path::new("/d/c"), 0).unwrap();
		fs.create(Path::new("/d/a"), 0).unwrap();
		fs.create(Path::new("/d/sub/deep"), 0).unwrap();
		assert_eq!(
			fs.read_dir(Path::new("/d")).unwrap(),
			vec![PathBuf::from("/d/a"), PathBuf::from("/d/c"), PathBuf::from("/d/sub")]
		);
		assert_eq!(fs.read_dir(Path::new("/nope")).err(), Some(FsError::NotFound(PathBuf::from("/nope"))));
		assert_eq!(fs.read_dir(Path::new("/d/a")).err(), Some(FsError::NotADirectory(PathBuf::from("/d/a"))));
	}

	#[test]
	fn rename_moves_the_entry_and_keeps_the_contents() {
		let fs = MemoryFs::new();
		let file = fs.create(Path::new("/a"), 512).unwrap();
		file.pwrite(0, &[6u8; 4]).unwrap();
		fs.rename(Path::new("/a"), Path::new("/b")).unwrap();
		assert_eq!(fs.open(Path::new("/a")).err(), Some(FsError::NotFound(PathBuf::from("/a"))));
		let moved = fs.open(Path::new("/b")).unwrap();
		let mut buf = [0u8; 4];
		assert_eq!(moved.pread(0, &mut buf).unwrap(), 4);
		assert_eq!(buf, [6u8; 4]);
		assert_eq!(
			fs.rename(Path::new("/a"), Path::new("/c")).err(),
			Some(FsError::NotFound(PathBuf::from("/a")))
		);
	}

	#[test]
	fn rename_into_a_missing_directory_leaves_the_source_in_place() {
		// a failed rename must not lose the file, or recovery would find neither name.
		let fs = MemoryFs::new();
		fs.create(Path::new("/a"), 0).unwrap();
		assert_eq!(
			fs.rename(Path::new("/a"), Path::new("/d/b")).err(),
			Some(FsError::NotFound(PathBuf::from("/d/b")))
		);
		assert!(fs.open(Path::new("/a")).is_ok());
	}

	#[test]
	fn unlink_removes_the_entry_or_reports_it_missing() {
		let fs = MemoryFs::new();
		fs.create(Path::new("/a"), 0).unwrap();
		fs.unlink(Path::new("/a")).unwrap();
		assert_eq!(fs.unlink(Path::new("/a")).err(), Some(FsError::NotFound(PathBuf::from("/a"))));
	}

	#[test]
	fn sync_dir_checks_the_directory_exists() {
		let fs = MemoryFs::new();
		fs.sync_dir(Path::new("/")).unwrap();
		assert_eq!(fs.sync_dir(Path::new("/d")).err(), Some(FsError::NotFound(PathBuf::from("/d"))));
		fs.mkdir(Path::new("/d")).unwrap();
		fs.sync_dir(Path::new("/d")).unwrap();
	}

	#[test]
	fn a_cloned_handle_shares_one_filesystem() {
		// the wasm build hands the same disk to several owners, so a clone must not fork the namespace.
		let fs = MemoryFs::new();
		let clone = fs.clone();
		clone.mkdir(Path::new("/d")).unwrap();
		assert_eq!(fs.read_dir(Path::new("/")).unwrap(), vec![PathBuf::from("/d")]);
	}
}
