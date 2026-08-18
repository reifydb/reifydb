// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_value::Result;

use super::FlowTransaction;

impl FlowTransaction<'_, '_> {
	pub fn set(&mut self, key: &EncodedKey, value: impl Into<EncodedBytes>) -> Result<()> {
		self.txn.set(key, value.into())
	}

	pub fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		self.txn.remove(key)
	}

	pub fn remove_silent(&mut self, key: &EncodedKey) -> Result<()> {
		self.txn.remove_silent(key)
	}

	pub fn set_batch(&mut self, keys: &[EncodedKey], values: &[EncodedBytes]) -> Result<()> {
		for (key, value) in keys.iter().zip(values.iter()) {
			self.txn.set(key, value.clone())?;
		}
		Ok(())
	}

	pub fn remove_batch(&mut self, keys: &[EncodedKey]) -> Result<()> {
		for key in keys {
			self.txn.remove(key)?;
		}
		Ok(())
	}

	pub fn remove_silent_batch(&mut self, keys: &[EncodedKey]) -> Result<()> {
		for key in keys {
			self.txn.remove_silent(key)?;
		}
		Ok(())
	}
}
