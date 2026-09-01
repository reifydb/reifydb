// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	error::Error as StdError,
	fmt,
	fmt::Display,
	path::{Path, PathBuf},
};

use reifydb_codec::log::{LogIndex, LogVersion, Position};
use reifydb_runtime::io::fs::FsError;

pub type Result<T> = std::result::Result<T, LogError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogError {
	NotFound(PathBuf),
	AlreadyExists(PathBuf),
	NoSpace(PathBuf),
	NotADirectory(PathBuf),
	SegmentFull {
		path: PathBuf,
		needed: u64,
		remaining: u64,
	},
	IndexShort {
		path: PathBuf,
		len: u64,
	},
	IndexMagic {
		path: PathBuf,
		found: u32,
	},
	VoteShort {
		path: PathBuf,
		len: u64,
	},
	VoteCorrupt {
		path: PathBuf,
	},
	SegmentIncomplete {
		path: PathBuf,
		end: Position,
		len: u64,
	},
	SegmentOutOfOrder {
		path: PathBuf,
		previous: LogVersion,
		found: LogVersion,
	},
	MetaShort {
		path: PathBuf,
		len: u64,
	},
	MetaMagic {
		path: PathBuf,
		found: u32,
	},
	MetaVersion {
		path: PathBuf,
		found: u32,
		expected: u32,
	},
	MetaCorrupt(PathBuf),
	NoSuchPartition {
		dir: PathBuf,
		requested: u32,
		count: u32,
	},
	InvalidReaderId {
		dir: PathBuf,
		id: String,
	},
	Purged {
		dir: PathBuf,
		requested: LogVersion,
		oldest: LogVersion,
	},
	OutOfOrder {
		dir: PathBuf,
		head: LogVersion,
		found: LogVersion,
	},
	IndexGap {
		dir: PathBuf,
		expected: LogIndex,
		found: LogIndex,
	},
	Io {
		path: PathBuf,
		message: String,
	},
}

impl From<FsError> for LogError {
	fn from(err: FsError) -> Self {
		match err {
			FsError::NotFound(path) => LogError::NotFound(path),
			FsError::AlreadyExists(path) => LogError::AlreadyExists(path),
			FsError::NoSpace(path) => LogError::NoSpace(path),
			FsError::NotADirectory(path) => LogError::NotADirectory(path),
			FsError::Io {
				path,
				message,
			} => LogError::Io {
				path,
				message,
			},
		}
	}
}

impl LogError {
	pub fn path(&self) -> &Path {
		match self {
			LogError::NotFound(path) => path,
			LogError::AlreadyExists(path) => path,
			LogError::NoSpace(path) => path,
			LogError::NotADirectory(path) => path,
			LogError::SegmentFull {
				path,
				..
			} => path,
			LogError::IndexShort {
				path,
				..
			} => path,
			LogError::IndexMagic {
				path,
				..
			} => path,
			LogError::VoteShort {
				path,
				..
			} => path,
			LogError::VoteCorrupt {
				path,
			} => path,
			LogError::SegmentIncomplete {
				path,
				..
			} => path,
			LogError::SegmentOutOfOrder {
				path,
				..
			} => path,
			LogError::MetaShort {
				path,
				..
			} => path,
			LogError::MetaMagic {
				path,
				..
			} => path,
			LogError::MetaVersion {
				path,
				..
			} => path,
			LogError::MetaCorrupt(path) => path,
			LogError::NoSuchPartition {
				dir,
				..
			} => dir,
			LogError::InvalidReaderId {
				dir,
				..
			} => dir,
			LogError::Purged {
				dir,
				..
			} => dir,
			LogError::OutOfOrder {
				dir,
				..
			} => dir,
			LogError::IndexGap {
				dir,
				..
			} => dir,
			LogError::Io {
				path,
				..
			} => path,
		}
	}
}

impl Display for LogError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			LogError::NotFound(path) => write!(f, "log file not found: {}", path.display()),
			LogError::AlreadyExists(path) => write!(f, "log file already exists: {}", path.display()),
			LogError::NoSpace(path) => write!(f, "log device out of space: {}", path.display()),
			LogError::NotADirectory(path) => write!(f, "log path is not a directory: {}", path.display()),
			LogError::SegmentFull {
				path,
				needed,
				remaining,
			} => write!(
				f,
				"log segment {} is full: {} bytes needed, {} remaining",
				path.display(),
				needed,
				remaining
			),
			LogError::IndexShort {
				path,
				len,
			} => write!(f, "log index {} is shorter than its header: {} bytes", path.display(), len),
			LogError::IndexMagic {
				path,
				found,
			} => write!(f, "log index {} has magic 0x{:08x}, not an index", path.display(), found),
			LogError::VoteShort {
				path,
				len,
			} => write!(f, "vote file {} is shorter than its two slots: {} bytes", path.display(), len),
			LogError::SegmentIncomplete {
				path,
				end,
				len,
			} => write!(f, "sealed log segment {} stops at {} of {} bytes", path.display(), end, len),
			LogError::SegmentOutOfOrder {
				path,
				previous,
				found,
			} => write!(
				f,
				"log segment {} starts at version {} at or below the preceding {}",
				path.display(),
				found,
				previous
			),
			LogError::VoteCorrupt {
				path,
			} => write!(
				f,
				"neither slot of vote file {} verifies, so the durable vote is lost",
				path.display()
			),
			LogError::MetaShort {
				path,
				len,
			} => write!(f, "log meta {} is shorter than its header: {} bytes", path.display(), len),
			LogError::MetaMagic {
				path,
				found,
			} => write!(f, "log meta {} has magic 0x{:08x}, not a meta file", path.display(), found),
			LogError::MetaVersion {
				path,
				found,
				expected,
			} => write!(
				f,
				"log meta {} is format version {}, this build reads {}",
				path.display(),
				found,
				expected
			),
			LogError::MetaCorrupt(path) => {
				write!(f, "log meta {} does not verify against its checksum", path.display())
			}
			LogError::NoSuchPartition {
				dir,
				requested,
				count,
			} => write!(f, "log {} has {} partitions, {} was requested", dir.display(), count, requested),
			LogError::InvalidReaderId {
				dir,
				id,
			} => write!(
				f,
				"log {} was given the reader id {:?}, which is not a usable file name",
				dir.display(),
				id
			),
			LogError::Purged {
				dir,
				requested,
				oldest,
			} => write!(
				f,
				"log {} was asked to read after version {}, but everything below version {} has been purged",
				dir.display(),
				requested.as_u64(),
				oldest.as_u64()
			),
			LogError::OutOfOrder {
				dir,
				head,
				found,
			} => write!(
				f,
				"log {} is at version {}, so version {} cannot be appended after it",
				dir.display(),
				head.as_u64(),
				found.as_u64()
			),
			LogError::IndexGap {
				dir,
				expected,
				found,
			} => write!(
				f,
				"log {} expects raft index {}, so index {} would leave a gap",
				dir.display(),
				expected.as_u64(),
				found.as_u64()
			),
			LogError::Io {
				path,
				message,
			} => write!(f, "log io error on {}: {}", path.display(), message),
		}
	}
}

impl StdError for LogError {}
