// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::path::{Path, PathBuf};

use reifydb_codec::log::{
	LogVersion,
	reader::{HINT_BYTES, Hint},
};
use reifydb_runtime::io::fs::{Create, Filesystem, FsError, Len, Mkdir, Open, ReadDir, Rename, Unlink};

use crate::{
	error::{LogError, Result},
	segment::{STAGING_SUFFIX, discard, read_exact, staging, write_all},
};

pub const DIR_NAME: &str = "readers";
pub const MAX_ID: usize = 64;

pub fn register<F: Filesystem + Create + Mkdir + Open + Rename + Unlink>(fs: &F, dir: &Path, id: &str) -> Result<()> {
	let path = path_of(dir, id)?;
	make_dir(fs, &dir.join(DIR_NAME))?;
	if fs.open(&path).is_ok() {
		return Err(LogError::AlreadyExists(path));
	}
	publish(fs, &path, LogVersion::ZERO)
}

pub fn unregister<F: Filesystem + Unlink>(fs: &F, dir: &Path, id: &str) -> Result<()> {
	discard(fs, &path_of(dir, id)?)
}

pub fn record<F: Filesystem + Create + Open + Rename + Unlink>(
	fs: &F,
	dir: &Path,
	id: &str,
	version: LogVersion,
) -> Result<()> {
	let path = path_of(dir, id)?;
	if fs.open(&path).is_err() {
		return Err(LogError::NotFound(path));
	}
	publish(fs, &path, version)
}

pub fn version_of<F: Filesystem + Open>(fs: &F, dir: &Path, id: &str) -> Result<LogVersion> {
	Ok(read(fs, &path_of(dir, id)?)?.unwrap_or(LogVersion::ZERO))
}

pub fn floor<F: Filesystem + Open + ReadDir>(fs: &F, dir: &Path) -> Result<Option<LogVersion>> {
	let entries = match fs.read_dir(&dir.join(DIR_NAME)) {
		Ok(entries) => entries,
		Err(FsError::NotFound(_)) => return Ok(None),
		Err(error) => return Err(error.into()),
	};
	let mut lowest: Option<LogVersion> = None;
	for path in entries {
		if path.as_os_str().as_encoded_bytes().ends_with(STAGING_SUFFIX.as_bytes()) {
			continue;
		}
		let pinned = read(fs, &path)?.unwrap_or(LogVersion::ZERO);
		lowest = Some(lowest.map_or(pinned, |low| low.min(pinned)));
	}
	Ok(lowest)
}

pub fn clamp<F: Filesystem + Create + Open + ReadDir + Rename + Unlink>(
	fs: &F,
	dir: &Path,
	ceiling: LogVersion,
) -> Result<()> {
	let entries = match fs.read_dir(&dir.join(DIR_NAME)) {
		Ok(entries) => entries,
		Err(FsError::NotFound(_)) => return Ok(()),
		Err(error) => return Err(error.into()),
	};
	for path in entries {
		if path.as_os_str().as_encoded_bytes().ends_with(STAGING_SUFFIX.as_bytes()) {
			continue;
		}
		if read(fs, &path)?.is_some_and(|pinned| pinned > ceiling) {
			publish(fs, &path, ceiling)?;
		}
	}
	Ok(())
}

pub fn path_of(dir: &Path, id: &str) -> Result<PathBuf> {
	if !valid(id) {
		return Err(LogError::InvalidReaderId {
			dir: dir.to_path_buf(),
			id: id.to_string(),
		});
	}
	Ok(dir.join(DIR_NAME).join(id))
}

fn valid(id: &str) -> bool {
	if id.is_empty() || id.len() > MAX_ID || id == "." || id == ".." {
		return false;
	}
	if id.ends_with(STAGING_SUFFIX) {
		return false;
	}
	id.chars().all(|at| at.is_ascii_alphanumeric() || at == '-' || at == '_' || at == '.')
}

fn read<F: Filesystem + Open>(fs: &F, path: &Path) -> Result<Option<LogVersion>> {
	let file = fs.open(path)?;
	if file.len()? < HINT_BYTES as u64 {
		return Ok(None);
	}
	let mut raw = [0u8; HINT_BYTES];
	if !read_exact(&file, 0, &mut raw)? {
		return Ok(None);
	}
	Ok(Hint::decode(&raw).map(|hint| hint.version))
}

fn publish<F: Filesystem + Create + Rename + Unlink>(fs: &F, path: &Path, version: LogVersion) -> Result<()> {
	let staged = staging(path);
	discard(fs, &staged)?;
	let file = fs.create(&staged, HINT_BYTES as u64)?;
	write_all(&file, &staged, 0, &Hint::new(version).encode())?;
	Ok(fs.rename(&staged, path)?)
}

fn make_dir<F: Filesystem + Mkdir>(fs: &F, path: &Path) -> Result<()> {
	match fs.mkdir(path) {
		Err(FsError::AlreadyExists(_)) => Ok(()),
		other => Ok(other?),
	}
}

#[cfg(test)]
mod tests {
	use reifydb_runtime::io::fs::{Mkdir, OpenMut, Pwrite, memory::MemoryFs};

	use super::*;

	const DIR: &str = "/log/p0";

	fn fixture() -> MemoryFs {
		let fs = MemoryFs::new();
		fs.mkdir(Path::new("/log")).unwrap();
		fs.mkdir(Path::new(DIR)).unwrap();
		fs
	}

	fn dir() -> &'static Path {
		Path::new(DIR)
	}

	#[test]
	fn an_id_that_is_not_a_usable_file_name_is_refused() {
		// the id becomes a path segment under readers/, so a separator or a parent reference
		// writes the hint outside the partition, and an id ending in the staging suffix would be
		// skipped by floor and pin nothing at all.
		for id in ["", ".", "..", "a/b", "../escape", "flow 3", "x.staging"] {
			assert!(matches!(path_of(dir(), id), Err(LogError::InvalidReaderId { .. })), "{id}");
		}
		assert!(path_of(dir(), &"a".repeat(MAX_ID + 1)).is_err());

		assert!(path_of(dir(), "flow-3").is_ok());
		assert!(path_of(dir(), &"a".repeat(MAX_ID)).is_ok());
	}

	#[test]
	fn a_registered_reader_pins_at_zero_until_it_records() {
		// registration is what makes a reader count, and a reader that has read nothing must
		// hold the whole log rather than let retention run ahead of it.
		let fs = fixture();
		register(&fs, dir(), "flow-3").unwrap();

		assert_eq!(floor(&fs, dir()).unwrap(), Some(LogVersion::ZERO));

		record(&fs, dir(), "flow-3", LogVersion::new(900)).unwrap();

		assert_eq!(floor(&fs, dir()).unwrap(), Some(LogVersion::new(900)));
	}

	#[test]
	fn the_floor_is_the_minimum_over_every_reader() {
		// one lagging reader has to hold the segment for all of them; taking anything but the
		// minimum unlinks a segment a slower reader has not read.
		let fs = fixture();
		register(&fs, dir(), "flow-3").unwrap();
		register(&fs, dir(), "operator").unwrap();
		record(&fs, dir(), "flow-3", LogVersion::new(900)).unwrap();
		record(&fs, dir(), "operator", LogVersion::new(400)).unwrap();

		assert_eq!(floor(&fs, dir()).unwrap(), Some(LogVersion::new(400)));
	}

	#[test]
	fn no_readers_directory_at_all_means_no_floor() {
		// decision 233: an empty set is "nobody is pinning", not "everybody is pinned at zero".
		// Reading it as zero would stop a log with no readers from ever purging.
		let fs = fixture();

		assert_eq!(floor(&fs, dir()).unwrap(), None);

		register(&fs, dir(), "flow-3").unwrap();
		unregister(&fs, dir(), "flow-3").unwrap();

		assert_eq!(floor(&fs, dir()).unwrap(), None);
	}

	#[test]
	fn a_hint_that_does_not_decode_pins_at_zero() {
		// decision 234: the hint is published without an fsync, so a crash can leave the name
		// durable over zero bytes. Treating that as absent would let purge unlink segments the
		// reader still needs; pinning at zero over retains and heals on the next record.
		let fs = fixture();
		register(&fs, dir(), "flow-3").unwrap();
		record(&fs, dir(), "flow-3", LogVersion::new(900)).unwrap();

		let file = fs.open_mut(&path_of(dir(), "flow-3").unwrap()).unwrap();
		file.pwrite(0, &[0u8; HINT_BYTES]).unwrap();

		assert_eq!(floor(&fs, dir()).unwrap(), Some(LogVersion::ZERO));
	}

	#[test]
	fn a_staging_file_left_by_a_crash_is_not_read_as_a_hint() {
		// publish stages under <id>.staging and renames; a crash between the two leaves a name
		// that decodes fine but belongs to no registered reader, so counting it would pin the
		// log on a reader that does not exist.
		let fs = fixture();
		register(&fs, dir(), "flow-3").unwrap();
		record(&fs, dir(), "flow-3", LogVersion::new(900)).unwrap();
		let staged = staging(&path_of(dir(), "flow-3").unwrap());
		let file = fs.create(&staged, HINT_BYTES as u64).unwrap();
		file.pwrite(0, &Hint::new(LogVersion::ZERO).encode()).unwrap();

		assert_eq!(floor(&fs, dir()).unwrap(), Some(LogVersion::new(900)));
	}

	#[test]
	fn recording_against_an_id_that_was_never_registered_is_refused() {
		// a typo in an id would otherwise create a phantom reader that never advances and
		// silently holds the log forever.
		let fs = fixture();

		assert!(matches!(record(&fs, dir(), "flow-3", LogVersion::new(900)), Err(LogError::NotFound(_))));
		assert_eq!(floor(&fs, dir()).unwrap(), None);
	}

	#[test]
	fn registering_twice_is_refused_rather_than_resetting_the_hint() {
		// a second register that silently rewrote the hint to zero would undo every position the
		// reader had recorded and pin the whole log again.
		let fs = fixture();
		register(&fs, dir(), "flow-3").unwrap();
		record(&fs, dir(), "flow-3", LogVersion::new(900)).unwrap();

		assert!(matches!(register(&fs, dir(), "flow-3"), Err(LogError::AlreadyExists(_))));
		assert_eq!(floor(&fs, dir()).unwrap(), Some(LogVersion::new(900)));
	}
}
