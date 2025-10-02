// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{EncodedKey, Result, interface::SingleVersionValues};

use crate::backend::{memory::MemoryBackend, result::SingleVersionGetResult, single::BackendSingleVersionGet};

impl BackendSingleVersionGet for MemoryBackend {
	fn get(&self, key: &EncodedKey) -> Result<SingleVersionGetResult> {
		match self.single.get(key) {
			Some(item) => match item.value() {
				Some(values) => Ok(SingleVersionGetResult::Value(SingleVersionValues {
					key: key.clone(),
					values: values.clone(),
				})),
				None => Ok(SingleVersionGetResult::Tombstone {
					key: key.clone(),
				}),
			},
			None => Ok(SingleVersionGetResult::NotFound),
		}
	}
}
