// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::RwLockReadGuard;

use reifydb_core::interface::SingleVersionQueryTransaction;
use reifydb_store_transaction::{SingleVersionContains, SingleVersionGet};

use super::*;

pub struct SvlQueryTransaction<'a> {
	pub(super) store: RwLockReadGuard<'a, TransactionStore>,
}

impl SingleVersionQueryTransaction for SvlQueryTransaction<'_> {
	fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<SingleVersionValues>> {
		self.store.get(key)
	}

	fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool> {
		self.store.contains(key)
	}
}
