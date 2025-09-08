// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	Result, Version,
	interface::{CdcEvent, CdcGet},
};

use crate::memory::Memory;

impl CdcGet for Memory {
	fn get(&self, version: Version) -> Result<Vec<CdcEvent>> {
		// Get the transaction for this specific version and convert to
		// events
		if let Some(entry) = self.cdc_transactions.get(&version) {
			Ok(entry.value().to_events().collect())
		} else {
			Ok(vec![])
		}
	}
}
