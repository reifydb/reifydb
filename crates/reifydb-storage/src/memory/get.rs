// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::Bound;

use reifydb_core::{
	EncodedKey, Result, Version,
	interface::{Unversioned, UnversionedGet, Versioned, VersionedGet},
};

use crate::memory::Memory;

impl VersionedGet for Memory {
	fn get(
		&self,
		key: &EncodedKey,
		version: Version,
	) -> Result<Option<Versioned>> {
		let item = match self.versioned.get(key) {
			Some(item) => item,
			None => return Ok(None),
		};
		let (version, value) = match item
			.value()
			.upper_bound(Bound::Included(&version))
			.and_then(|v| {
				if v.value().is_some() {
					Some((
						*v.key(),
						v.value().clone().unwrap(),
					))
				} else {
					None
				}
			}) {
			Some(result) => result,
			None => return Ok(None),
		};

		Ok(Some(Versioned {
			key: key.clone(),
			row: value,
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
