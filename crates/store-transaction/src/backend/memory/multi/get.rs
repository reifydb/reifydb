// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{CommitVersion, EncodedKey, Result, interface::MultiVersionValues};

use super::{StorageType, classify_key};
use crate::backend::{memory::MemoryBackend, multi::BackendMultiVersionGet, result::MultiVersionGetResult};

impl BackendMultiVersionGet for MemoryBackend {
	fn get(&self, key: &EncodedKey, version: CommitVersion) -> Result<MultiVersionGetResult> {
		// Route to the correct storage based on key type
		let chain = match classify_key(key) {
			StorageType::Source(source_id) => {
				let sources = self.sources.read();
				sources.get(&source_id).and_then(|table| table.get(key)).cloned()
			}
			StorageType::Operator(flow_node_id) => {
				let operators = self.operators.read();
				operators.get(&flow_node_id).and_then(|table| table.get(key)).cloned()
			}
			StorageType::Multi => {
				let multi = self.multi.read();
				multi.get(key).cloned()
			}
		};

		match chain.and_then(|chain| chain.get_at(version)) {
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
