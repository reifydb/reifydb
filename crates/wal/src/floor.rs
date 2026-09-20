// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::reifydb_assertions;

use crate::{device::Mark, error::Result, lsn::Lsn, wal::Wal};

pub struct Floor<T, L> {
	wal: Wal<T, L>,
	name: String,
	position: Mutex<Option<Lsn>>,
}

impl<T, L> Floor<T, L> {
	pub(crate) fn new(wal: Wal<T, L>, name: String, position: Option<Lsn>) -> Self {
		Self {
			wal,
			name,
			position: Mutex::new(position),
		}
	}

	pub fn name(&self) -> &str {
		&self.name
	}

	pub fn position(&self) -> Option<Lsn> {
		*self.position.lock()
	}
}

impl<T, L: Mark> Floor<T, L> {
	pub fn advance(&self, lsn: Lsn) -> Result<()> {
		let mut position = self.position.lock();
		reifydb_assertions! {
			assert!(
				position.is_none_or(|held| lsn >= held),
				"the floor {} was moved back from {:?} to {lsn:?}, so gc is free to drop records the \
				 reader behind it has not read",
				self.name,
				*position
			);
		}
		if position.is_some_and(|held| lsn <= held) {
			return Ok(());
		}
		self.wal.device().record(&self.name, lsn.into())?;
		*position = Some(lsn);
		Ok(())
	}
}

impl<T, L> Drop for Floor<T, L> {
	fn drop(&mut self) {
		self.wal.release(&self.name);
	}
}
