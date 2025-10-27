// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{CommitVersion, EncodedKey, Result, interface::MultiVersionValues};

use crate::backend::{memory::MemoryBackend, multi::BackendMultiVersionGet, result::MultiVersionGetResult};

impl BackendMultiVersionGet for MemoryBackend {
	fn get(&self, key: &EncodedKey, version: CommitVersion) -> Result<MultiVersionGetResult> {
		let multi = self.multi.read();

		match multi.get(key).and_then(|chain| chain.get_at(version)) {
			Some(Some(values)) => Ok(MultiVersionGetResult::Value(MultiVersionValues {
				key: key.clone(),
				values,
				version,
			})),
			Some(None) => Ok(MultiVersionGetResult::Tombstone {
				key: key.clone(),
				version,
			}),
			None => Ok(MultiVersionGetResult::NotFound),
		}
	}
}
