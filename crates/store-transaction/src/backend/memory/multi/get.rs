// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{CommitVersion, EncodedKey, Result, interface::MultiVersionValues};

use crate::{MultiVersionGet, backend::memory::MemoryBackend};

impl MultiVersionGet for MemoryBackend {
	fn get(&self, key: &EncodedKey, version: CommitVersion) -> Result<Option<MultiVersionValues>> {
		let item = match self.multi.get(key) {
			Some(item) => item,
			None => return Ok(None),
		};

		// Get the value at the specified version
		let values = match item.value().get(version) {
			Some(values) => values,
			None => return Ok(None),
		};

		Ok(Some(MultiVersionValues {
			key: key.clone(),
			values,
			version,
		}))
	}
}
