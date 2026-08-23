// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod metrics;
mod read;
mod storage;

use std::{ops::Deref, sync::Arc};

use reifydb_runtime::actor::mailbox::ActorRef;
use tracing::instrument;

use crate::{
	config::CdcStoreConfig,
	flush::{
		actor::{CdcFlushActor, FlushMessage, flush_pending},
		block::flush_with,
	},
	tier::{commit::CdcCommitBufferTier, persistent::CdcPersistentTier, read::CdcReadBufferTier},
};

pub struct CdcStoreInner {
	pub(crate) commit: CdcCommitBufferTier,
	pub(crate) read: Option<CdcReadBufferTier>,
	pub(crate) persistent: CdcPersistentTier,
	pub(crate) flusher: ActorRef<FlushMessage>,
}

#[derive(Clone)]
pub struct CdcStore(Arc<CdcStoreInner>);

impl Deref for CdcStore {
	type Target = CdcStoreInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl CdcStore {
	#[instrument(name = "store::cdc::new", level = "debug", skip(config))]
	pub fn new(config: CdcStoreConfig) -> Self {
		let commit = config.commit.storage.clone();
		let read = config.read.and_then(CdcReadBufferTier::new);
		let persistent = config.persistent.storage.clone();
		if let Ok(Some(sealed)) = persistent.max_version() {
			commit.seal_floor(sealed);
		}
		let flusher = CdcFlushActor::spawn(
			&config.spawner,
			commit.clone(),
			persistent.clone(),
			read.clone(),
			config.persistent.flush_interval,
		);
		commit.attach_flusher(flusher.clone());
		Self(Arc::new(CdcStoreInner {
			commit,
			read,
			persistent,
			flusher,
		}))
	}

	#[instrument(name = "store::cdc::flush_pending", level = "debug", skip(self))]
	pub fn flush_pending(&self) -> bool {
		flush_pending(&self.flusher)
	}

	#[instrument(name = "store::cdc::flush_staged", level = "debug", skip_all)]
	pub fn flush_staged(&self, staged: &mut dyn FnMut()) {
		flush_with(&self.commit, &self.persistent, self.read.as_ref(), staged);
	}

	#[instrument(name = "store::cdc::shutdown", level = "debug", skip(self))]
	pub fn shutdown(&self) {
		self.flush_pending();
		self.persistent.shutdown();
	}
}
