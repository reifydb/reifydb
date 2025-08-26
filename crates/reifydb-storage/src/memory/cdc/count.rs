// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{Result, Version, interface::CdcCount};

use crate::memory::Memory;

impl CdcCount for Memory {
	fn count(&self, version: Version) -> Result<usize> {
		// Get the events for this specific version
		if let Some(entry) = self.cdc_events.get(&version) {
			Ok(entry.value().len())
		} else {
			Ok(0)
		}
	}
}
