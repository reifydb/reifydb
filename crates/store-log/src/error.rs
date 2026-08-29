// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	error::Error as StdError,
	fmt,
	fmt::Display,
	io,
	path::{Path, PathBuf},
};

pub type Result<T> = std::result::Result<T, LogError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogError {
	NotFound(PathBuf),
	AlreadyExists(PathBuf),
	NoSpace(PathBuf),
	Io {
		path: PathBuf,
		message: String,
	},
}

impl LogError {
	pub fn from_io(path: &Path, err: io::Error) -> Self {
		match err.kind() {
			io::ErrorKind::NotFound => LogError::NotFound(path.to_path_buf()),
			io::ErrorKind::AlreadyExists => LogError::AlreadyExists(path.to_path_buf()),
			io::ErrorKind::StorageFull => LogError::NoSpace(path.to_path_buf()),
			_ => LogError::Io {
				path: path.to_path_buf(),
				message: err.to_string(),
			},
		}
	}

	pub fn path(&self) -> &Path {
		match self {
			LogError::NotFound(path) => path,
			LogError::AlreadyExists(path) => path,
			LogError::NoSpace(path) => path,
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
			LogError::Io {
				path,
				message,
			} => write!(f, "log io error on {}: {}", path.display(), message),
		}
	}
}

impl StdError for LogError {}
