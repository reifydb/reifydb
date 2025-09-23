// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CommitVersion, EncodedKey, Result,
	interface::{MultiVersionContains, SingleVersionContains},
};

use crate::memory::Memory;

impl MultiVersionContains for Memory {
	fn contains(&self, key: &EncodedKey, version: CommitVersion) -> Result<bool> {
		let result = match self.multi.get(key) {
			None => false,
			Some(values) => values.value().get(version).is_some(),
		};
		Ok(result)
	}
}

impl SingleVersionContains for Memory {
	fn contains(&self, key: &EncodedKey) -> Result<bool> {
		Ok(self.single.get(key).is_some())
	}
}
