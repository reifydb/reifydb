// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::iter;

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::operator::EncodedOperatorRow,
};
use reifydb_core::interface::store::{MultiVersionBatch, MultiVersionRow};
use reifydb_transaction::multi::RangeScope;
use reifydb_value::{Result, error::Error as ValueError};

use super::FlowTransaction;

impl FlowTransaction<'_, '_> {
	pub fn get(&mut self, key: &EncodedKey) -> Result<Option<EncodedOperatorRow>> {
		self.inner.store_reads += 1;
		match self.txn.get(key)? {
			Some(multi) => {
				Ok(Some(EncodedOperatorRow::try_from(multi.bytes.clone()).map_err(ValueError::from)?))
			}
			None => Ok(None),
		}
	}

	pub fn contains_key(&mut self, key: &EncodedKey) -> Result<bool> {
		self.txn.contains_key(key)
	}

	pub fn prefix(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		let range = EncodedKeyRange::prefix(prefix);
		let items = self.range(range, RangeScope::All, 1024).collect::<Result<Vec<_>>>()?;
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	pub fn range(
		&mut self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_> {
		match self.txn.range(range, scope, batch_size) {
			Ok(iter) => iter,
			Err(err) => Box::new(iter::once(Err(err))),
		}
	}

	pub fn range_rev(
		&mut self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_> {
		match self.txn.range_rev(range, scope, batch_size) {
			Ok(iter) => iter,
			Err(err) => Box::new(iter::once(Err(err))),
		}
	}
}
