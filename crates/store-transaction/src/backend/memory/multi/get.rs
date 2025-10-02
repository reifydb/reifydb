// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{CommitVersion, EncodedKey, Result, interface::MultiVersionValues};

use crate::backend::{memory::MemoryBackend, multi::BackendMultiVersionGet, result::MultiVersionGetResult};

impl BackendMultiVersionGet for MemoryBackend {
	fn get(&self, key: &EncodedKey, version: CommitVersion) -> Result<MultiVersionGetResult> {
		let item = match self.multi.get(key) {
			Some(item) => item,
			None => return Ok(MultiVersionGetResult::NotFound),
		};

		match item.value().get_or_tombstone(version) {
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
