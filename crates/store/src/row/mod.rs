// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

pub mod manager;
pub mod page;
pub mod state;
pub mod strategy;

pub use manager::{PromotionDecision, StoreManager};
pub use page::{DEFAULT_BUCKET_SHIFT, PageId, key_range_of, page_of};
pub use state::PageState;
pub use strategy::{NoopStrategy, StoreStrategy};
