// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	error::Error as StdError,
	fmt,
	fmt::Display,
	path::{Path, PathBuf},
};

use reifydb_runtime::io::fs::FsError;

pub type Result<T> = std::result::Result<T, LogError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogError {
	NotFound(PathBuf),
	AlreadyExists(PathBuf),
	NoSpace(PathBuf),
	NotADirectory(PathBuf),
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
			LogError::Io {
				path,
				message,
			} => write!(f, "log io error on {}: {}", path.display(), message),
		}
	}
}

impl StdError for LogError {}
