// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::store::EntryKind;

use crate::row::{manager::PromotionDecision, page::PageId};

pub trait StoreStrategy: Send + Sync + 'static {
	fn touch(&self, page: PageId);

	fn select_evict(&self, kind: EntryKind, n: usize) -> Vec<PageId>;

	fn promotion_disposition(&self, page: PageId) -> PromotionDecision;
}

pub struct NoopStrategy;

impl StoreStrategy for NoopStrategy {
	fn touch(&self, _page: PageId) {}

	fn select_evict(&self, _kind: EntryKind, _n: usize) -> Vec<PageId> {
		Vec::new()
	}

	fn promotion_disposition(&self, _page: PageId) -> PromotionDecision {
		PromotionDecision::None
	}
}
