// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	path::{Path, PathBuf},
	sync::Arc,
};

use crate::error::Result;

#[cfg(unix)]
pub mod file;
#[cfg(feature = "testing")]
pub mod memory;

pub trait LogIo: Send + Sync {
	fn mkdir(&self, path: &Path) -> Result<()>;

	fn create(&self, path: &Path, len: u64) -> Result<Arc<dyn Handle>>;

	fn open(&self, path: &Path) -> Result<Arc<dyn Handle>>;

	fn read_dir(&self, path: &Path) -> Result<Vec<PathBuf>>;

	fn rename(&self, from: &Path, to: &Path) -> Result<()>;

	fn unlink(&self, path: &Path) -> Result<()>;

	fn sync_dir(&self, path: &Path) -> Result<()>;
}

pub trait Handle: Send + Sync {
	fn pread(&self, offset: u64, buf: &mut [u8]) -> Result<usize>;

	fn pwrite(&self, offset: u64, buf: &[u8]) -> Result<usize>;

	fn sync_data(&self) -> Result<()>;

	fn truncate(&self, len: u64) -> Result<()>;

	fn len(&self) -> Result<u64>;
}
