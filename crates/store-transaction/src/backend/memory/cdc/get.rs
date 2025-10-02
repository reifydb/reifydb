// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CommitVersion, Result,
	interface::{Cdc, CdcGet},
};

use crate::backend::memory::MemoryBackend;

impl CdcGet for MemoryBackend {
	fn get(&self, version: CommitVersion) -> Result<Option<Cdc>> {
		if let Some(entry) = self.cdcs.get(&version) {
			Ok(Some(entry.value().clone()))
		} else {
			Ok(None)
		}
	}
}
