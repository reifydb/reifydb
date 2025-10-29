// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{ops::Deref, sync::Arc, time::Duration};

use crate::{
	BackendConfig,
	backend::{Backend, cdc::BackendCdc, gc::GarbageCollector, multi::BackendMulti, single::BackendSingle},
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
	#[allow(dead_code)] // Held for Drop impl
	pub(crate) gc: Option<GarbageCollector>,
}

impl StandardTransactionStore {
	pub fn new(config: TransactionStoreConfig) -> crate::Result<Self> {
		let hot = config.hot.map(|c| c.backend);
		let warm = config.warm.map(|c| c.backend);
		let cold = config.cold.map(|c| c.backend);

		// Spawn GC thread if enabled and we have at least one backend
		let gc = if config.gc.enabled {
			// Use the first available backend for GC
			let backend = hot.as_ref().or(warm.as_ref()).or(cold.as_ref());

			backend.map(|b| {
				let interval = Duration::from_secs(config.gc.interval_secs);
				GarbageCollector::spawn(b.multi.clone(), interval)
			})
		} else {
			None
		};

		Ok(Self(Arc::new(StandardTransactionStoreInner {
			hot,
			warm,
			cold,
			gc,
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
			gc: Default::default(),
		})
		.unwrap()
	}
}
