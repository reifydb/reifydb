// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod multi;
mod single;

use reifydb_core::interface::TransactionStore;

use crate::{backend::Backend, config::TransactionStoreConfig};

#[derive(Clone)]
pub struct StandardTransactionStore {
	pub(crate) hot: Option<Backend>,
	pub(crate) warm: Option<Backend>,
	pub(crate) cold: Option<Backend>,
}

impl StandardTransactionStore {
	pub fn new(config: TransactionStoreConfig) -> crate::Result<Self> {
		Ok(Self {
			hot: config.hot.map(|c| c.backend),
			warm: config.warm.map(|c| c.backend),
			cold: config.cold.map(|c| c.backend),
		})
	}
}

impl TransactionStore for StandardTransactionStore {}
