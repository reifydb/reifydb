// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CommitVersion, EncodedKey, Result,
	interface::{Unversioned, UnversionedGet, Versioned, VersionedGet},
};

use crate::memory::Memory;

impl VersionedGet for Memory {
	fn get(
		&self,
		key: &EncodedKey,
		version: CommitVersion,
	) -> Result<Option<Versioned>> {
		let item = match self.versioned.get(key) {
			Some(item) => item,
			None => return Ok(None),
		};

		// Get the value at the specified version
		let row = match item.value().get(version) {
			Some(row) => row,
			None => return Ok(None),
		};

		Ok(Some(Versioned {
			key: key.clone(),
			row,
			version,
		}))
	}
}

impl UnversionedGet for Memory {
	fn get(&self, key: &EncodedKey) -> Result<Option<Unversioned>> {
		Ok(self.unversioned.get(key).map(|item| Unversioned {
			key: key.clone(),
			row: item.value().clone(),
		}))
	}
}
