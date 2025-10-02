// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{EncodedKey, Result, interface::SingleVersionValues};

use crate::{SingleVersionGet, backend::memory::MemoryBackend};

impl SingleVersionGet for MemoryBackend {
	fn get(&self, key: &EncodedKey) -> Result<Option<SingleVersionValues>> {
		Ok(self.single.get(key).map(|item| SingleVersionValues {
			key: key.clone(),
			values: item.value().clone(),
		}))
	}
}
