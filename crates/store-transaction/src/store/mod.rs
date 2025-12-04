// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{ops::Deref, sync::Arc, time::Duration};

use tracing::instrument;

use crate::{BackendConfig, backend::BackendStorage, config::TransactionStoreConfig};

mod cdc;
mod cdc_iterator;
mod multi;
mod multi_iterator;
pub mod router;
mod single;
mod single_iterator;
pub mod version_manager;

#[derive(Clone)]
pub struct StandardTransactionStore(Arc<StandardTransactionStoreInner>);

pub struct StandardTransactionStoreInner {
	pub(crate) hot: Option<BackendStorage>,
	pub(crate) warm: Option<BackendStorage>,
	pub(crate) cold: Option<BackendStorage>,
}

impl StandardTransactionStore {
	#[instrument(level = "info", skip(config), fields(
		has_hot = config.hot.is_some(),
		has_warm = config.warm.is_some(),
		has_cold = config.cold.is_some()
	))]
	pub fn new(config: TransactionStoreConfig) -> crate::Result<Self> {
		let hot = config.hot.map(|c| c.storage);
		let warm = config.warm.map(|c| c.storage);
		let cold = config.cold.map(|c| c.storage);

		Ok(Self(Arc::new(StandardTransactionStoreInner {
			hot,
			warm,
			cold,
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
		Self::new(TransactionStoreConfig {
			hot: Some(BackendConfig {
				storage: BackendStorage::memory(),
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
