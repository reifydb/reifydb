// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{CommitVersion, Result, interface::Cdc};

use crate::{CdcGet, backend::memory::MemoryBackend, cdc::converter::CdcConverter};

impl CdcGet for MemoryBackend {
	fn get(&self, version: CommitVersion) -> Result<Option<Cdc>> {
		let cdc = self.cdc.read();
		if let Some(internal_cdc) = cdc.get(&version).cloned() {
			Ok(Some(self.convert(internal_cdc)?))
		} else {
			Ok(None)
		}
	}
}
