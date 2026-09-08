// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	path::{Path, PathBuf},
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use crate::{
	io::fs::{
		Create, Filesystem, FsError, Len, Mkdir, Open, OpenMut, Pread, Pwrite, ReadDir, Rename, Result,
		SyncData, SyncDir, Truncate, Unlink,
		memory::{MemoryFile, MemoryFileMut, MemoryFileState, MemoryFs, SectorMask, sector_span},
	},
	sync::mutex::Mutex,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteOutcome {
	Land,
	Torn(SectorMask),
	Corrupt(SectorMask),
	Misdirected(i64),
	Lost,
	Short(usize),
	Err(FsError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadOutcome {
	Clean,
	Corrupt(SectorMask),
	Misdirected(i64),
	Short(usize),
	Err(FsError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncOutcome {
	Honest,
	Lying,
	Err(FsError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetaOutcome {
	Durable,
	NotDurable,
	Err(FsError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Continue {
	Yes,
	Crash,
}

pub trait TestingHooks: Send + Sync {
	fn on_pwrite(&self, _file: FileId, _offset: u64, _len: usize) -> WriteOutcome {
		WriteOutcome::Land
	}

	fn on_pread(&self, _file: FileId, _offset: u64, _len: usize) -> ReadOutcome {
		ReadOutcome::Clean
	}

	fn on_sync(&self, _file: FileId) -> SyncOutcome {
		SyncOutcome::Honest
	}

	fn on_rename(&self, _from: &Path, _to: &Path) -> MetaOutcome {
		MetaOutcome::Durable
	}

	fn on_unlink(&self, _path: &Path) -> MetaOutcome {
		MetaOutcome::Durable
	}

	fn on_syscall(&self, _call: u64) -> Continue {
		Continue::Yes
	}
}

pub struct NoFaults;

impl TestingHooks for NoFaults {}

enum Undo {
	Rename {
		from: PathBuf,
		to: PathBuf,
	},
	Unlink {
		path: PathBuf,
		state: Arc<MemoryFileState>,
	},
}

struct Inner {
	inner: MemoryFs,
	hooks: Arc<dyn TestingHooks>,
	calls: AtomicU64,
	next_file_id: AtomicU64,
	undone: Mutex<Vec<Undo>>,
}

impl Inner {
	fn syscall(&self) {
		let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
		if self.hooks.on_syscall(call) == Continue::Crash {
			self.crash();
			panic!("simulated crash at syscall {call}");
		}
	}

	fn next_id(&self) -> FileId {
		FileId(self.next_file_id.fetch_add(1, Ordering::SeqCst))
	}

	fn crash(&self) {
		let undone: Vec<Undo> = self.undone.lock().drain(..).collect();
		for undo in undone.into_iter().rev() {
			match undo {
				Undo::Rename {
					from,
					to,
				} => {
					if let Ok(state) = self.inner.detach(&to) {
						self.inner.attach(&from, state);
					}
				}
				Undo::Unlink {
					path,
					state,
				} => self.inner.attach(&path, state),
			}
		}
		self.inner.crash();
	}
}

fn shift_offset(offset: u64, shift: i64) -> u64 {
	if shift >= 0 {
		offset.saturating_add(shift as u64)
	} else {
		offset.saturating_sub(shift.unsigned_abs())
	}
}

fn full_mask(offset: u64, len: usize, sector_bytes: usize) -> SectorMask {
	SectorMask::full(sector_span(offset, len, sector_bytes))
}

fn invert_masked(offset: u64, buf: &mut [u8], mask: &SectorMask, sector_bytes: usize) {
	let offset = offset as usize;
	let end = offset + buf.len();
	let first = offset / sector_bytes;
	for index in 0..mask.sectors() {
		if !mask.is_set(index) {
			continue;
		}
		let sector = first + index;
		let lo = (sector * sector_bytes).max(offset);
		let hi = (sector * sector_bytes + sector_bytes).min(end);
		if lo >= hi {
			continue;
		}
		for byte in &mut buf[lo - offset..hi - offset] {
			*byte = !*byte;
		}
	}
}

trait Sectored: Pread {
	fn sector_bytes(&self) -> usize;
}

impl Sectored for MemoryFile {
	fn sector_bytes(&self) -> usize {
		MemoryFile::sector_bytes(self)
	}
}

impl Sectored for MemoryFileMut {
	fn sector_bytes(&self) -> usize {
		MemoryFileMut::sector_bytes(self)
	}
}

fn hooked_pread<F: Sectored>(inner: &Inner, id: FileId, file: &F, offset: u64, buf: &mut [u8]) -> Result<usize> {
	match inner.hooks.on_pread(id, offset, buf.len()) {
		ReadOutcome::Clean => file.pread(offset, buf),
		ReadOutcome::Corrupt(mask) => {
			let read = file.pread(offset, buf)?;
			invert_masked(offset, &mut buf[..read], &mask, file.sector_bytes());
			Ok(read)
		}
		ReadOutcome::Misdirected(shift) => file.pread(shift_offset(offset, shift), buf),
		ReadOutcome::Short(short) => {
			let short = short.min(buf.len());
			file.pread(offset, &mut buf[..short])?;
			Ok(short)
		}
		ReadOutcome::Err(error) => Err(error),
	}
}

#[derive(Clone)]
pub struct TestingFs(Arc<Inner>);

impl TestingFs {
	pub fn new(inner: MemoryFs, hooks: Arc<dyn TestingHooks>) -> Self {
		Self(Arc::new(Inner {
			inner,
			hooks,
			calls: AtomicU64::new(0),
			next_file_id: AtomicU64::new(0),
			undone: Mutex::new(Vec::new()),
		}))
	}

	pub fn calls(&self) -> u64 {
		self.0.calls.load(Ordering::SeqCst)
	}

	pub fn crash(&self) {
		self.0.crash();
	}
}

pub struct TestingFile {
	id: FileId,
	inner: MemoryFile,
	fs: Arc<Inner>,
}

pub struct TestingFileMut {
	id: FileId,
	inner: MemoryFileMut,
	fs: Arc<Inner>,
}

impl Filesystem for TestingFs {
	type File = TestingFile;
	type FileMut = TestingFileMut;
}

impl Mkdir for TestingFs {
	fn mkdir(&self, path: &Path) -> Result<()> {
		self.0.syscall();
		self.0.inner.mkdir(path)
	}
}

impl Create for TestingFs {
	fn create(&self, path: &Path, len: u64) -> Result<TestingFileMut> {
		self.0.syscall();
		let inner = self.0.inner.create(path, len)?;
		Ok(TestingFileMut {
			id: self.0.next_id(),
			inner,
			fs: Arc::clone(&self.0),
		})
	}
}

impl Open for TestingFs {
	fn open(&self, path: &Path) -> Result<TestingFile> {
		self.0.syscall();
		let inner = self.0.inner.open(path)?;
		Ok(TestingFile {
			id: self.0.next_id(),
			inner,
			fs: Arc::clone(&self.0),
		})
	}
}

impl OpenMut for TestingFs {
	fn open_mut(&self, path: &Path) -> Result<TestingFileMut> {
		self.0.syscall();
		let inner = self.0.inner.open_mut(path)?;
		Ok(TestingFileMut {
			id: self.0.next_id(),
			inner,
			fs: Arc::clone(&self.0),
		})
	}
}

impl ReadDir for TestingFs {
	fn read_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
		self.0.syscall();
		self.0.inner.read_dir(path)
	}
}

impl Rename for TestingFs {
	fn rename(&self, from: &Path, to: &Path) -> Result<()> {
		self.0.syscall();
		match self.0.hooks.on_rename(from, to) {
			MetaOutcome::Durable => self.0.inner.rename(from, to),
			MetaOutcome::NotDurable => {
				self.0.inner.rename(from, to)?;
				self.0.undone.lock().push(Undo::Rename {
					from: from.to_path_buf(),
					to: to.to_path_buf(),
				});
				Ok(())
			}
			MetaOutcome::Err(error) => Err(error),
		}
	}
}

impl Unlink for TestingFs {
	fn unlink(&self, path: &Path) -> Result<()> {
		self.0.syscall();
		match self.0.hooks.on_unlink(path) {
			MetaOutcome::Durable => self.0.inner.unlink(path),
			MetaOutcome::NotDurable => {
				let state = self.0.inner.detach(path)?;
				self.0.undone.lock().push(Undo::Unlink {
					path: path.to_path_buf(),
					state,
				});
				Ok(())
			}
			MetaOutcome::Err(error) => Err(error),
		}
	}
}

impl SyncDir for TestingFs {
	fn sync_dir(&self, path: &Path) -> Result<()> {
		self.0.syscall();
		self.0.inner.sync_dir(path)
	}
}

impl Pread for TestingFile {
	fn pread(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
		self.fs.syscall();
		hooked_pread(&self.fs, self.id, &self.inner, offset, buf)
	}
}

impl Len for TestingFile {
	fn len(&self) -> Result<u64> {
		self.fs.syscall();
		self.inner.len()
	}
}

impl Pread for TestingFileMut {
	fn pread(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
		self.fs.syscall();
		hooked_pread(&self.fs, self.id, &self.inner, offset, buf)
	}
}

impl Pwrite for TestingFileMut {
	fn pwrite(&self, offset: u64, buf: &[u8]) -> Result<usize> {
		self.fs.syscall();
		let sector_bytes = self.inner.sector_bytes();
		match self.fs.hooks.on_pwrite(self.id, offset, buf.len()) {
			WriteOutcome::Land => {
				self.inner.write_masked(offset, buf, &full_mask(offset, buf.len(), sector_bytes))
			}
			WriteOutcome::Torn(mask) => {
				self.inner.write_masked(offset, buf, &mask)?;
				Ok(buf.len())
			}
			WriteOutcome::Corrupt(mask) => {
				let mut corrupted = buf.to_vec();
				invert_masked(offset, &mut corrupted, &mask, sector_bytes);
				self.inner.write_masked(
					offset,
					&corrupted,
					&full_mask(offset, corrupted.len(), sector_bytes),
				)
			}
			WriteOutcome::Misdirected(shift) => {
				let target = shift_offset(offset, shift);
				self.inner.write_masked(target, buf, &full_mask(target, buf.len(), sector_bytes))?;
				Ok(buf.len())
			}
			WriteOutcome::Lost => Ok(buf.len()),
			WriteOutcome::Short(short) => {
				let short = short.min(buf.len());
				self.inner.write_masked(
					offset,
					&buf[..short],
					&full_mask(offset, short, sector_bytes),
				)?;
				Ok(short)
			}
			WriteOutcome::Err(error) => Err(error),
		}
	}
}

impl SyncData for TestingFileMut {
	fn sync_data(&self) -> Result<()> {
		self.fs.syscall();
		match self.fs.hooks.on_sync(self.id) {
			SyncOutcome::Honest => self.inner.sync_data(),
			SyncOutcome::Lying => Ok(()),
			SyncOutcome::Err(error) => Err(error),
		}
	}
}

impl Truncate for TestingFileMut {
	fn truncate(&self, len: u64) -> Result<()> {
		self.fs.syscall();
		self.inner.truncate(len)
	}
}

impl Len for TestingFileMut {
	fn len(&self) -> Result<u64> {
		self.fs.syscall();
		self.inner.len()
	}
}

#[cfg(test)]
mod tests {
	use std::panic::{AssertUnwindSafe, catch_unwind};

	use super::*;
	use crate::io::fs::memory::SectorState;

	fn setup(hooks: Arc<dyn TestingHooks>) -> (MemoryFs, TestingFs) {
		let memory = MemoryFs::new();
		let fs = TestingFs::new(memory.clone(), hooks);
		(memory, fs)
	}

	fn bytes(memory: &MemoryFs, path: &str) -> Vec<u8> {
		let file = memory.open(Path::new(path)).unwrap();
		let len = file.len().unwrap() as usize;
		let mut buf = vec![0u8; len];
		file.pread(0, &mut buf).unwrap();
		buf
	}

	fn states(memory: &MemoryFs, path: &str) -> Vec<SectorState> {
		memory.open(Path::new(path)).unwrap().sector_states()
	}

	struct WriteHook(WriteOutcome);

	impl TestingHooks for WriteHook {
		fn on_pwrite(&self, _file: FileId, _offset: u64, _len: usize) -> WriteOutcome {
			self.0.clone()
		}
	}

	struct ReadHook(ReadOutcome);

	impl TestingHooks for ReadHook {
		fn on_pread(&self, _file: FileId, _offset: u64, _len: usize) -> ReadOutcome {
			self.0.clone()
		}
	}

	struct SyncHook(SyncOutcome);

	impl TestingHooks for SyncHook {
		fn on_sync(&self, _file: FileId) -> SyncOutcome {
			self.0.clone()
		}
	}

	struct RenameHook(MetaOutcome);

	impl TestingHooks for RenameHook {
		fn on_rename(&self, _from: &Path, _to: &Path) -> MetaOutcome {
			self.0.clone()
		}
	}

	struct UnlinkHook(MetaOutcome);

	impl TestingHooks for UnlinkHook {
		fn on_unlink(&self, _path: &Path) -> MetaOutcome {
			self.0.clone()
		}
	}

	struct CrashAt(u64);

	impl TestingHooks for CrashAt {
		fn on_syscall(&self, call: u64) -> Continue {
			if call == self.0 {
				Continue::Crash
			} else {
				Continue::Yes
			}
		}
	}

	#[test]
	fn land_writes_every_touched_sector() {
		// the pass-through outcome must behave exactly like the bare backend, or a fault-free run would drift.
		let (memory, fs) = setup(Arc::new(WriteHook(WriteOutcome::Land)));
		let file = fs.create(Path::new("/a"), 1024).unwrap();
		assert_eq!(file.pwrite(0, &[1u8; 600]).unwrap(), 600);
		assert_eq!(states(&memory, "/a"), vec![SectorState::Dirty, SectorState::Dirty]);
		assert_eq!(&bytes(&memory, "/a")[..600], &[1u8; 600]);
	}

	#[test]
	fn torn_write_lands_only_the_masked_sectors_yet_reports_success() {
		// the caller is told the whole write succeeded while half of it never reached the platter.
		let mut mask = SectorMask::empty(2);
		mask.set(1);
		let (memory, fs) = setup(Arc::new(WriteHook(WriteOutcome::Torn(mask))));
		let file = fs.create(Path::new("/a"), 1024).unwrap();
		assert_eq!(file.pwrite(0, &[9u8; 1024]).unwrap(), 1024);
		let data = bytes(&memory, "/a");
		assert!(data[..512].iter().all(|byte| *byte == 0));
		assert!(data[512..].iter().all(|byte| *byte == 9));
		assert_eq!(states(&memory, "/a"), vec![SectorState::Unwritten, SectorState::Dirty]);
	}

	#[test]
	fn corrupt_write_inverts_exactly_the_masked_sectors() {
		// corruption must be a deterministic bit flip, so one seed always reproduces the same damage.
		let mut mask = SectorMask::empty(2);
		mask.set(0);
		let (memory, fs) = setup(Arc::new(WriteHook(WriteOutcome::Corrupt(mask))));
		let file = fs.create(Path::new("/a"), 1024).unwrap();
		assert_eq!(file.pwrite(0, &[0x0Fu8; 1024]).unwrap(), 1024);
		let data = bytes(&memory, "/a");
		assert!(data[..512].iter().all(|byte| *byte == 0xF0));
		assert!(data[512..].iter().all(|byte| *byte == 0x0F));
		assert_eq!(states(&memory, "/a"), vec![SectorState::Dirty, SectorState::Dirty]);
	}

	#[test]
	fn misdirected_write_lands_at_the_shifted_offset() {
		// a write that hits the wrong block corrupts a neighbour the caller never named.
		let (memory, fs) = setup(Arc::new(WriteHook(WriteOutcome::Misdirected(512))));
		let file = fs.create(Path::new("/a"), 1024).unwrap();
		assert_eq!(file.pwrite(0, &[7u8; 512]).unwrap(), 512);
		let data = bytes(&memory, "/a");
		assert!(data[..512].iter().all(|byte| *byte == 0));
		assert!(data[512..].iter().all(|byte| *byte == 7));
	}

	#[test]
	fn misdirected_write_saturates_at_zero() {
		// a negative shift larger than the offset must clamp rather than wrap into a huge offset.
		let (memory, fs) = setup(Arc::new(WriteHook(WriteOutcome::Misdirected(-4096))));
		let file = fs.create(Path::new("/a"), 1024).unwrap();
		assert_eq!(file.pwrite(512, &[7u8; 8]).unwrap(), 8);
		assert_eq!(&bytes(&memory, "/a")[..8], &[7u8; 8]);
	}

	#[test]
	fn lost_write_changes_nothing_but_reports_success() {
		// a silently dropped write is indistinguishable from a good one until the data is read back.
		let (memory, fs) = setup(Arc::new(WriteHook(WriteOutcome::Lost)));
		let file = fs.create(Path::new("/a"), 1024).unwrap();
		assert_eq!(file.pwrite(0, &[3u8; 1024]).unwrap(), 1024);
		assert_eq!(bytes(&memory, "/a"), vec![0u8; 1024]);
		assert_eq!(states(&memory, "/a"), vec![SectorState::Unwritten; 2]);
	}

	#[test]
	fn short_write_lands_a_prefix_and_reports_it() {
		// a short count is the one honest failure a caller can see, so the byte count must match the damage.
		let (memory, fs) = setup(Arc::new(WriteHook(WriteOutcome::Short(600))));
		let file = fs.create(Path::new("/a"), 1024).unwrap();
		assert_eq!(file.pwrite(0, &[5u8; 1024]).unwrap(), 600);
		let data = bytes(&memory, "/a");
		assert!(data[..600].iter().all(|byte| *byte == 5));
		assert!(data[600..].iter().all(|byte| *byte == 0));
	}

	#[test]
	fn write_error_reaches_the_caller_untouched() {
		// an injected error must arrive as the exact error the plan named, and change nothing on disk.
		let error = FsError::NoSpace(PathBuf::from("/a"));
		let (memory, fs) = setup(Arc::new(WriteHook(WriteOutcome::Err(error.clone()))));
		let file = fs.create(Path::new("/a"), 1024).unwrap();
		assert_eq!(file.pwrite(0, &[5u8; 8]).err(), Some(error));
		assert_eq!(bytes(&memory, "/a"), vec![0u8; 1024]);
	}

	#[test]
	fn clean_read_returns_the_stored_bytes() {
		let (_memory, fs) = setup(Arc::new(ReadHook(ReadOutcome::Clean)));
		let file = fs.create(Path::new("/a"), 16).unwrap();
		file.pwrite(0, &[1, 2, 3, 4]).unwrap();
		let mut buf = [0u8; 4];
		assert_eq!(file.pread(0, &mut buf).unwrap(), 4);
		assert_eq!(buf, [1, 2, 3, 4]);
	}

	#[test]
	fn corrupt_read_inverts_the_masked_sectors_without_touching_the_file() {
		// a read fault must damage only the caller's buffer, or it would silently rewrite the disk.
		let mut mask = SectorMask::empty(1);
		mask.set(0);
		let (memory, fs) = setup(Arc::new(ReadHook(ReadOutcome::Corrupt(mask))));
		let file = fs.create(Path::new("/a"), 512).unwrap();
		file.pwrite(0, &[0x0Fu8; 512]).unwrap();
		let mut buf = [0u8; 512];
		assert_eq!(file.pread(0, &mut buf).unwrap(), 512);
		assert!(buf.iter().all(|byte| *byte == 0xF0));
		assert!(bytes(&memory, "/a").iter().all(|byte| *byte == 0x0F));
	}

	#[test]
	fn misdirected_read_returns_another_offset() {
		let (_memory, fs) = setup(Arc::new(ReadHook(ReadOutcome::Misdirected(512))));
		let file = fs.create(Path::new("/a"), 1024).unwrap();
		file.pwrite(512, &[6u8; 512]).unwrap();
		let mut buf = [0u8; 4];
		assert_eq!(file.pread(0, &mut buf).unwrap(), 4);
		assert_eq!(buf, [6u8; 4]);
	}

	#[test]
	fn short_read_fills_only_a_prefix() {
		// the tail of the buffer must stay as the caller left it, which is how a short read is detected.
		let (_memory, fs) = setup(Arc::new(ReadHook(ReadOutcome::Short(2))));
		let file = fs.create(Path::new("/a"), 8).unwrap();
		file.pwrite(0, &[1, 2, 3, 4, 5, 6, 7, 8]).unwrap();
		let mut buf = [0u8; 8];
		assert_eq!(file.pread(0, &mut buf).unwrap(), 2);
		assert_eq!(buf, [1, 2, 0, 0, 0, 0, 0, 0]);
	}

	#[test]
	fn read_error_reaches_the_caller_untouched() {
		let error = FsError::Io {
			path: PathBuf::from("/a"),
			message: "bad sector".to_string(),
		};
		let (_memory, fs) = setup(Arc::new(ReadHook(ReadOutcome::Err(error.clone()))));
		let file = fs.create(Path::new("/a"), 8).unwrap();
		let mut buf = [0u8; 8];
		assert_eq!(file.pread(0, &mut buf).err(), Some(error));
	}

	#[test]
	fn honest_sync_promotes_every_dirty_sector() {
		let (memory, fs) = setup(Arc::new(SyncHook(SyncOutcome::Honest)));
		let file = fs.create(Path::new("/a"), 512).unwrap();
		file.pwrite(0, &[1u8; 512]).unwrap();
		file.sync_data().unwrap();
		assert_eq!(states(&memory, "/a"), vec![SectorState::Durable]);
	}

	#[test]
	fn lying_sync_reports_success_and_promotes_nothing() {
		// this is the fault the whole seam exists for: a sync that returns ok and flushes nothing.
		let (memory, fs) = setup(Arc::new(SyncHook(SyncOutcome::Lying)));
		let file = fs.create(Path::new("/a"), 512).unwrap();
		file.pwrite(0, &[1u8; 512]).unwrap();
		assert_eq!(file.sync_data(), Ok(()));
		assert_eq!(states(&memory, "/a"), vec![SectorState::Dirty]);
	}

	#[test]
	fn sync_error_leaves_the_sectors_dirty() {
		let error = FsError::Io {
			path: PathBuf::from("/a"),
			message: "eio".to_string(),
		};
		let (memory, fs) = setup(Arc::new(SyncHook(SyncOutcome::Err(error.clone()))));
		let file = fs.create(Path::new("/a"), 512).unwrap();
		file.pwrite(0, &[1u8; 512]).unwrap();
		assert_eq!(file.sync_data().err(), Some(error));
		assert_eq!(states(&memory, "/a"), vec![SectorState::Dirty]);
	}

	#[test]
	fn durable_rename_survives_a_crash() {
		let (memory, fs) = setup(Arc::new(RenameHook(MetaOutcome::Durable)));
		fs.create(Path::new("/a"), 8).unwrap();
		fs.rename(Path::new("/a"), Path::new("/b")).unwrap();
		fs.crash();
		assert!(memory.open(Path::new("/b")).is_ok());
		assert!(memory.open(Path::new("/a")).is_err());
	}

	#[test]
	fn not_durable_rename_is_undone_by_a_crash() {
		// a rename whose directory entry never reached the platter must be back at the old name after a crash.
		let (memory, fs) = setup(Arc::new(RenameHook(MetaOutcome::NotDurable)));
		fs.create(Path::new("/a"), 8).unwrap();
		fs.rename(Path::new("/a"), Path::new("/b")).unwrap();
		assert!(memory.open(Path::new("/b")).is_ok());
		fs.crash();
		assert!(memory.open(Path::new("/a")).is_ok());
		assert!(memory.open(Path::new("/b")).is_err());
	}

	#[test]
	fn rename_error_leaves_both_names_as_they_were() {
		let error = FsError::Io {
			path: PathBuf::from("/a"),
			message: "eio".to_string(),
		};
		let (memory, fs) = setup(Arc::new(RenameHook(MetaOutcome::Err(error.clone()))));
		fs.create(Path::new("/a"), 8).unwrap();
		assert_eq!(fs.rename(Path::new("/a"), Path::new("/b")).err(), Some(error));
		assert!(memory.open(Path::new("/a")).is_ok());
		assert!(memory.open(Path::new("/b")).is_err());
	}

	#[test]
	fn durable_unlink_survives_a_crash() {
		let (memory, fs) = setup(Arc::new(UnlinkHook(MetaOutcome::Durable)));
		fs.create(Path::new("/a"), 8).unwrap();
		fs.unlink(Path::new("/a")).unwrap();
		fs.crash();
		assert!(memory.open(Path::new("/a")).is_err());
	}

	#[test]
	fn not_durable_unlink_comes_back_with_its_contents() {
		// recovery has to face a file the caller believed was gone, contents and durable sectors intact.
		let (memory, fs) = setup(Arc::new(UnlinkHook(MetaOutcome::NotDurable)));
		let file = fs.create(Path::new("/a"), 512).unwrap();
		file.pwrite(0, &[2u8; 512]).unwrap();
		file.sync_data().unwrap();
		fs.unlink(Path::new("/a")).unwrap();
		assert!(memory.open(Path::new("/a")).is_err());
		fs.crash();
		assert_eq!(bytes(&memory, "/a"), vec![2u8; 512]);
		assert_eq!(states(&memory, "/a"), vec![SectorState::Durable]);
	}

	#[test]
	fn unlink_error_leaves_the_file_in_place() {
		let error = FsError::Io {
			path: PathBuf::from("/a"),
			message: "eio".to_string(),
		};
		let (memory, fs) = setup(Arc::new(UnlinkHook(MetaOutcome::Err(error.clone()))));
		fs.create(Path::new("/a"), 8).unwrap();
		assert_eq!(fs.unlink(Path::new("/a")).err(), Some(error));
		assert!(memory.open(Path::new("/a")).is_ok());
	}

	#[test]
	fn every_operation_advances_one_shared_syscall_counter() {
		// a crash point is a single integer for the whole run, so every descriptor must share the counter.
		let (_memory, fs) = setup(Arc::new(NoFaults));
		assert_eq!(fs.calls(), 0);
		fs.mkdir(Path::new("/d")).unwrap();
		let file = fs.create(Path::new("/d/a"), 8).unwrap();
		file.pwrite(0, &[1u8; 8]).unwrap();
		file.sync_data().unwrap();
		file.len().unwrap();
		file.truncate(4).unwrap();
		let mut buf = [0u8; 4];
		file.pread(0, &mut buf).unwrap();
		fs.read_dir(Path::new("/d")).unwrap();
		fs.sync_dir(Path::new("/d")).unwrap();
		fs.open(Path::new("/d/a")).unwrap();
		fs.open_mut(Path::new("/d/a")).unwrap();
		fs.rename(Path::new("/d/a"), Path::new("/d/b")).unwrap();
		fs.unlink(Path::new("/d/b")).unwrap();
		assert_eq!(fs.calls(), 13);
	}

	#[test]
	fn the_syscall_hook_crashes_at_an_exact_call_number() {
		// the crash must stop the caller's stack, because the process it models is dead.
		let (memory, fs) = setup(Arc::new(CrashAt(3)));
		let file = fs.create(Path::new("/a"), 512).unwrap();
		file.pwrite(0, &[8u8; 512]).unwrap();
		let panicked = catch_unwind(AssertUnwindSafe(|| file.pwrite(0, &[9u8; 512])));
		assert!(panicked.is_err());
		assert_eq!(fs.calls(), 3);
		assert_eq!(bytes(&memory, "/a"), vec![0u8; 512]);
		assert_eq!(states(&memory, "/a"), vec![SectorState::Unwritten]);
	}

	#[test]
	fn write_then_sync_then_crash_keeps_the_data() {
		// a sync that really flushed is the only promise recovery may rely on.
		let (memory, fs) = setup(Arc::new(NoFaults));
		let file = fs.create(Path::new("/a"), 1024).unwrap();
		file.pwrite(0, &[4u8; 1024]).unwrap();
		file.sync_data().unwrap();
		fs.crash();
		assert_eq!(bytes(&memory, "/a"), vec![4u8; 1024]);
		assert_eq!(states(&memory, "/a"), vec![SectorState::Durable; 2]);
	}

	#[test]
	fn write_then_crash_without_a_sync_loses_the_data() {
		// an unsynced write has no promise attached, so a crash must roll the sectors all the way back.
		let (memory, fs) = setup(Arc::new(NoFaults));
		let file = fs.create(Path::new("/a"), 1024).unwrap();
		file.pwrite(0, &[4u8; 1024]).unwrap();
		fs.crash();
		assert_eq!(bytes(&memory, "/a"), vec![0u8; 1024]);
		assert_eq!(states(&memory, "/a"), vec![SectorState::Unwritten; 2]);
	}

	#[test]
	fn a_lying_sync_loses_data_the_caller_was_told_was_safe() {
		// this is the postgres fsync bug: sync_data returned ok, the sectors stayed dirty, the crash ate them.
		let (memory, fs) = setup(Arc::new(SyncHook(SyncOutcome::Lying)));
		let file = fs.create(Path::new("/a"), 1024).unwrap();
		file.pwrite(0, &[4u8; 1024]).unwrap();
		assert_eq!(file.sync_data(), Ok(()));
		fs.crash();
		assert_eq!(bytes(&memory, "/a"), vec![0u8; 1024]);
		assert_eq!(states(&memory, "/a"), vec![SectorState::Unwritten; 2]);
	}

	#[test]
	fn no_faults_is_byte_for_byte_identical_to_a_bare_memory_fs() {
		// the wrapper may only add faults; with none configured it must be invisible to the caller.
		fn drive<F: Pread + Pwrite + SyncData + Truncate + Len>(file: &F) {
			file.pwrite(0, &[1u8; 100]).unwrap();
			file.pwrite(700, &[2u8; 50]).unwrap();
			file.sync_data().unwrap();
			file.pwrite(300, &[3u8; 10]).unwrap();
			file.truncate(900).unwrap();
			file.pwrite(890, &[4u8; 20]).unwrap();
		}

		let bare = MemoryFs::new();
		drive(&bare.create(Path::new("/a"), 1024).unwrap());

		let (memory, fs) = setup(Arc::new(NoFaults));
		drive(&fs.create(Path::new("/a"), 1024).unwrap());

		assert_eq!(bytes(&memory, "/a"), bytes(&bare, "/a"));
		assert_eq!(states(&memory, "/a"), states(&bare, "/a"));
		assert_eq!(
			memory.open(Path::new("/a")).unwrap().len().unwrap(),
			bare.open(Path::new("/a")).unwrap().len().unwrap()
		);
	}
}
