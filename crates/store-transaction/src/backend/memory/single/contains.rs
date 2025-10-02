// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{EncodedKey, Result};

use crate::backend::{memory::MemoryBackend, single::BackendSingleVersionContains};

impl BackendSingleVersionContains for MemoryBackend {
	fn contains(&self, key: &EncodedKey) -> Result<bool> {
		Ok(self.single.get(key).is_some())
	}
}
