// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CommitVersion, EncodedKey, Result,
	interface::{MultiVersionGet, MultiVersionRow, SingleVersionGet, SingleVersionRow},
};

use crate::backend::memory::Memory;

impl MultiVersionGet for Memory {
	fn get(&self, key: &EncodedKey, version: CommitVersion) -> Result<Option<MultiVersionRow>> {
		let item = match self.multi.get(key) {
			Some(item) => item,
			None => return Ok(None),
		};

		// Get the value at the specified version
		let row = match item.value().get(version) {
			Some(row) => row,
			None => return Ok(None),
		};

		Ok(Some(MultiVersionRow {
			key: key.clone(),
			row,
			version,
		}))
	}
}

impl SingleVersionGet for Memory {
	fn get(&self, key: &EncodedKey) -> Result<Option<SingleVersionRow>> {
		Ok(self.single.get(key).map(|item| SingleVersionRow {
			key: key.clone(),
			row: item.value().clone(),
		}))
	}
}
