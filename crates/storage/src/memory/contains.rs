// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	EncodedKey, Result, Version,
	interface::{UnversionedContains, VersionedContains},
};

use crate::memory::Memory;

impl VersionedContains for Memory {
	fn contains(&self, key: &EncodedKey, version: Version) -> Result<bool> {
		let result = match self.versioned.get(key) {
			None => false,
			Some(values) => values.value().get(version).is_some(),
		};
		Ok(result)
	}
}

impl UnversionedContains for Memory {
	fn contains(&self, key: &EncodedKey) -> Result<bool> {
		Ok(self.unversioned.get(key).is_some())
	}
}
