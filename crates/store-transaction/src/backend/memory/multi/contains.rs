// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{CommitVersion, EncodedKey, Result};

use super::{StorageType, classify_key};
use crate::backend::{memory::MemoryBackend, multi::BackendMultiVersionContains};

impl BackendMultiVersionContains for MemoryBackend {
	fn contains(&self, key: &EncodedKey, version: CommitVersion) -> Result<bool> {
		// Route to the correct storage based on key type
		let result = match classify_key(key) {
			StorageType::Source(source_id) => {
				let sources = self.sources.read();
				sources.get(&source_id)
					.and_then(|table| table.get(key))
					.map_or(false, |chain| chain.contains_at(version))
			}
			StorageType::Operator(flow_node_id) => {
				let operators = self.operators.read();
				operators
					.get(&flow_node_id)
					.and_then(|table| table.get(key))
					.map_or(false, |chain| chain.contains_at(version))
			}
			StorageType::Multi => {
				let multi = self.multi.read();
				multi.get(key).map_or(false, |chain| chain.contains_at(version))
			}
		};

		Ok(result)
	}
}
