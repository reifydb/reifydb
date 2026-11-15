// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	error::Error as StdError,
	fmt,
	fmt::Display,
	path::{Path, PathBuf},
};

use reifydb_codec::log::{LogIndex, LogVersion, Position, Term};
use reifydb_runtime::io::fs::FsError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreLogError {
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
	IndexCorrupt(PathBuf),
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
	SegmentCorrupt {
		path: PathBuf,
		position: Position,
	},
	SegmentFormat {
		path: PathBuf,
		position: Position,
		reserved: u32,
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
	TermRegression {
		dir: PathBuf,
		last: Term,
		found: Term,
	},
	TruncateCommitted {
		dir: PathBuf,
		commit: LogIndex,
		found: LogIndex,
	},
	CommitOutOfRange {
		dir: PathBuf,
		low: LogIndex,
		high: LogIndex,
		found: LogIndex,
	},
	CompactOutOfRange {
		dir: PathBuf,
		low: LogIndex,
		high: LogIndex,
		found: LogIndex,
	},
	Io {
		path: PathBuf,
		message: String,
	},
}

impl From<FsError> for StoreLogError {
	fn from(err: FsError) -> Self {
		match err {
			FsError::NotFound(path) => StoreLogError::NotFound(path),
			FsError::AlreadyExists(path) => StoreLogError::AlreadyExists(path),
			FsError::NoSpace(path) => StoreLogError::NoSpace(path),
			FsError::NotADirectory(path) => StoreLogError::NotADirectory(path),
			FsError::Io {
				path,
				message,
			} => StoreLogError::Io {
				path,
				message,
			},
		}
	}
}

impl StoreLogError {
	pub fn path(&self) -> &Path {
		match self {
			StoreLogError::NotFound(path) => path,
			StoreLogError::AlreadyExists(path) => path,
			StoreLogError::NoSpace(path) => path,
			StoreLogError::NotADirectory(path) => path,
			StoreLogError::SegmentFull {
				path,
				..
			} => path,
			StoreLogError::IndexShort {
				path,
				..
			} => path,
			StoreLogError::IndexMagic {
				path,
				..
			} => path,
			StoreLogError::IndexCorrupt(path) => path,
			StoreLogError::VoteShort {
				path,
				..
			} => path,
			StoreLogError::VoteCorrupt {
				path,
			} => path,
			StoreLogError::SegmentIncomplete {
				path,
				..
			} => path,
			StoreLogError::SegmentCorrupt {
				path,
				..
			} => path,
			StoreLogError::SegmentFormat {
				path,
				..
			} => path,
			StoreLogError::SegmentOutOfOrder {
				path,
				..
			} => path,
			StoreLogError::MetaShort {
				path,
				..
			} => path,
			StoreLogError::MetaMagic {
				path,
				..
			} => path,
			StoreLogError::MetaVersion {
				path,
				..
			} => path,
			StoreLogError::MetaCorrupt(path) => path,
			StoreLogError::NoSuchPartition {
				dir,
				..
			} => dir,
			StoreLogError::InvalidReaderId {
				dir,
				..
			} => dir,
			StoreLogError::Purged {
				dir,
				..
			} => dir,
			StoreLogError::OutOfOrder {
				dir,
				..
			} => dir,
			StoreLogError::IndexGap {
				dir,
				..
			} => dir,
			StoreLogError::TermRegression {
				dir,
				..
			} => dir,
			StoreLogError::TruncateCommitted {
				dir,
				..
			} => dir,
			StoreLogError::CommitOutOfRange {
				dir,
				..
			} => dir,
			StoreLogError::CompactOutOfRange {
				dir,
				..
			} => dir,
			StoreLogError::Io {
				path,
				..
			} => path,
		}
	}
}

impl Display for StoreLogError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			StoreLogError::NotFound(path) => write!(f, "log file not found: {}", path.display()),
			StoreLogError::AlreadyExists(path) => write!(f, "log file already exists: {}", path.display()),
			StoreLogError::NoSpace(path) => write!(f, "log device out of space: {}", path.display()),
			StoreLogError::NotADirectory(path) => {
				write!(f, "log path is not a directory: {}", path.display())
			}
			StoreLogError::SegmentFull {
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
			StoreLogError::IndexShort {
				path,
				len,
			} => write!(f, "log index {} is shorter than its header: {} bytes", path.display(), len),
			StoreLogError::IndexMagic {
				path,
				found,
			} => write!(f, "log index {} has magic 0x{:08x}, not an index", path.display(), found),
			StoreLogError::IndexCorrupt(path) => {
				write!(f, "log index {} does not verify against its checksum", path.display())
			}
			StoreLogError::VoteShort {
				path,
				len,
			} => write!(f, "vote file {} is shorter than its two slots: {} bytes", path.display(), len),
			StoreLogError::SegmentIncomplete {
				path,
				end,
				len,
			} => write!(f, "sealed log segment {} stops at {} of {} bytes", path.display(), end, len),
			StoreLogError::SegmentOutOfOrder {
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
			StoreLogError::SegmentCorrupt {
				path,
				position,
			} => write!(
				f,
				"log segment {} is corrupt at {}, but valid records follow it",
				path.display(),
				position
			),
			StoreLogError::SegmentFormat {
				path,
				position,
				reserved,
			} => write!(
				f,
				"log segment {} has a record at {} with reserved word {}, so it was written by another format and cannot be replayed",
				path.display(),
				position,
				reserved
			),
			StoreLogError::VoteCorrupt {
				path,
			} => write!(
				f,
				"neither slot of vote file {} verifies, so the durable vote is lost",
				path.display()
			),
			StoreLogError::MetaShort {
				path,
				len,
			} => write!(f, "log meta {} is shorter than its header: {} bytes", path.display(), len),
			StoreLogError::MetaMagic {
				path,
				found,
			} => write!(f, "log meta {} has magic 0x{:08x}, not a meta file", path.display(), found),
			StoreLogError::MetaVersion {
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
			StoreLogError::MetaCorrupt(path) => {
				write!(f, "log meta {} does not verify against its checksum", path.display())
			}
			StoreLogError::NoSuchPartition {
				dir,
				requested,
				count,
			} => write!(f, "log {} has {} partitions, {} was requested", dir.display(), count, requested),
			StoreLogError::InvalidReaderId {
				dir,
				id,
			} => write!(
				f,
				"log {} was given the reader id {:?}, which is not a usable file name",
				dir.display(),
				id
			),
			StoreLogError::Purged {
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
			StoreLogError::OutOfOrder {
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
			StoreLogError::IndexGap {
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
			StoreLogError::TermRegression {
				dir,
				last,
				found,
			} => write!(
				f,
				"log {} is at term {}, so a record in term {} cannot be appended after it",
				dir.display(),
				last.as_u64(),
				found.as_u64()
			),
			StoreLogError::TruncateCommitted {
				dir,
				commit,
				found,
			} => write!(
				f,
				"log {} has committed through index {}, so it cannot be truncated from {}",
				dir.display(),
				commit.as_u64(),
				found.as_u64()
			),
			StoreLogError::CommitOutOfRange {
				dir,
				low,
				high,
				found,
			} => write!(
				f,
				"log {} can only commit between {} and {}, so {} is out of range",
				dir.display(),
				low.as_u64(),
				high.as_u64(),
				found.as_u64()
			),
			StoreLogError::CompactOutOfRange {
				dir,
				low,
				high,
				found,
			} => write!(
				f,
				"log {} can only compact between {} and {}, so {} is out of range",
				dir.display(),
				low.as_u64(),
				high.as_u64(),
				found.as_u64()
			),
			StoreLogError::Io {
				path,
				message,
			} => write!(f, "log io error on {}: {}", path.display(), message),
		}
	}
}

impl StdError for StoreLogError {}
