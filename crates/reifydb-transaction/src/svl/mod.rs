// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::{Arc, RwLock};

use reifydb_core::{
	CowVec, EncodedKey, EncodedKeyRange,
	delta::Delta,
	interface::{
		WithHooks, Unversioned, UnversionedStorage,
		UnversionedTransaction,
	},
	row::EncodedRow,
};

pub(crate) mod range;
pub(crate) mod range_rev;
mod read;
pub(crate) mod scan;
pub(crate) mod scan_rev;
mod write;

pub use read::SvlReadTransaction;
use reifydb_core::hook::Hooks;
pub use write::SvlWriteTransaction;

#[derive(Clone)]
pub struct SingleVersionLock<US> {
	inner: Arc<SvlInner<US>>,
}

struct SvlInner<US> {
	storage: RwLock<US>,
	hooks: Hooks,
}

impl<US> SingleVersionLock<US>
where
	US: UnversionedStorage,
{
	pub fn new(storage: US, hooks: Hooks) -> Self {
		Self {
			inner: Arc::new(SvlInner {
				storage: RwLock::new(storage),
				hooks,
			}),
		}
	}
}

impl<US> WithHooks for SingleVersionLock<US>
where
	US: UnversionedStorage,
{
	fn hooks(&self) -> &Hooks {
		&self.inner.hooks
	}
}

impl<US> UnversionedTransaction for SingleVersionLock<US>
where
	US: UnversionedStorage,
{
	type Query<'a> = SvlReadTransaction<'a, US>;
	type Command<'a> = SvlWriteTransaction<'a, US>;

	fn begin_query(&self) -> crate::Result<Self::Query<'_>> {
		let storage = self.inner.storage.read().unwrap();
		Ok(SvlReadTransaction {
			storage,
		})
	}

	fn begin_command(&self) -> crate::Result<Self::Command<'_>> {
		let storage = self.inner.storage.write().unwrap();
		Ok(SvlWriteTransaction::new(storage))
	}
}
