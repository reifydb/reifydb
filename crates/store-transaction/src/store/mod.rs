// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{ops::Deref, sync::Arc, time::Duration};

use crate::{
	BackendConfig,
	backend::{Backend, cdc::BackendCdc, multi::BackendMulti, single::BackendSingle},
	config::TransactionStoreConfig,
	memory::MemoryBackend,
};

mod cdc;
mod cdc_iterator;
mod multi;
mod multi_iterator;
mod single;
mod single_iterator;

#[derive(Clone)]
pub struct StandardTransactionStore(Arc<StandardTransactionStoreInner>);

pub struct StandardTransactionStoreInner {
	pub(crate) hot: Option<Backend>,
	pub(crate) warm: Option<Backend>,
	pub(crate) cold: Option<Backend>,
}

impl StandardTransactionStore {
	pub fn new(config: TransactionStoreConfig) -> crate::Result<Self> {
		Ok(Self(Arc::new(StandardTransactionStoreInner {
			hot: config.hot.map(|c| c.backend),
			warm: config.warm.map(|c| c.backend),
			cold: config.cold.map(|c| c.backend),
		})))
	}
}

impl Deref for StandardTransactionStore {
	type Target = StandardTransactionStoreInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl StandardTransactionStore {
	pub fn testing_memory() -> Self {
		let memory = MemoryBackend::new();

		Self::new(TransactionStoreConfig {
			hot: Some(BackendConfig {
				backend: Backend {
					multi: BackendMulti::Memory(memory.clone()),
					single: BackendSingle::Memory(memory.clone()),
					cdc: BackendCdc::Memory(memory),
				},
				retention_period: Duration::from_millis(100),
			}),
			warm: None,
			cold: None,
			retention: Default::default(),
			merge_config: Default::default(),
		})
		.unwrap()
	}
}
