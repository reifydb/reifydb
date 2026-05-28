// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use crate::row::page::PageId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromotionDecision {
	None,
	Sync,
}

pub trait StoreManager: Send + Sync + 'static {
	fn on_read(&self, page: PageId, hit: bool);

	fn on_write_committed(&self, page: PageId, keys: u64);

	fn on_persisted(&self, page: PageId, keys: u64);
}
