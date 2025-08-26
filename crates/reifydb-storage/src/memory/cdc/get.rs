// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	Result, Version,
	interface::{CdcEvent, CdcGet},
};

use crate::memory::Memory;

impl CdcGet for Memory {
	fn get(&self, version: Version) -> Result<Vec<CdcEvent>> {
		// Get the events for this specific version
		if let Some(entry) = self.cdc_events.get(&version) {
			Ok(entry.value().clone())
		} else {
			Ok(vec![])
		}
	}
}
