// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	Result, Version,
	interface::{CdcCount, CdcEventKey},
};

use crate::memory::Memory;

impl CdcCount for Memory {
	fn count(&self, version: Version) -> Result<usize> {
		let start_key = CdcEventKey {
			version,
			sequence: 0,
		};
		let end_key = CdcEventKey {
			version: version + 1,
			sequence: 0,
		};

		let count = self.cdc_events.range(start_key..end_key).count();

		Ok(count)
	}
}
