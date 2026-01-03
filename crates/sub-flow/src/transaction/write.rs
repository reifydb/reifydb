// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{EncodedKey, value::encoded::EncodedValues};

use super::FlowTransaction;

impl FlowTransaction<'_> {
	/// Set a value directly in the command transaction
	pub async fn set(&mut self, key: &EncodedKey, value: EncodedValues) -> crate::Result<()> {
		self.cmd.set(key, value).await?;
		Ok(())
	}

	/// Remove a key directly from the command transaction
	pub async fn remove(&mut self, key: &EncodedKey) -> crate::Result<()> {
		self.cmd.remove(key).await?;
		Ok(())
	}
}
