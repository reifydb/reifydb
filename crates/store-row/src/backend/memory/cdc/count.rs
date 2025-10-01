// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{CommitVersion, Result, interface::CdcCount};

use crate::backend::memory::Memory;

impl CdcCount for Memory {
	fn count(&self, version: CommitVersion) -> Result<usize> {
		// Get the transaction for this specific version and count
		// changes
		if let Some(entry) = self.cdcs.get(&version) {
			Ok(entry.value().changes.len())
		} else {
			Ok(0)
		}
	}
}
