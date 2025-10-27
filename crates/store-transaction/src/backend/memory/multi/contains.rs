// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{CommitVersion, EncodedKey, Result};

use crate::backend::{memory::MemoryBackend, multi::BackendMultiVersionContains};

impl BackendMultiVersionContains for MemoryBackend {
	fn contains(&self, key: &EncodedKey, version: CommitVersion) -> Result<bool> {
		let multi = self.multi.read();
		Ok(multi.get(key).map_or(false, |chain| chain.contains_at(version)))
	}
}
