// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#[path = "state/fixtures.rs"]
mod fixtures;

#[path = "state/update_preserves_created_at.rs"]
mod update_preserves_created_at;

#[path = "state/first_insert_uses_caller_created_at.rs"]
mod first_insert_uses_caller_created_at;

#[path = "state/zero_prior_anchor_is_not_pinned.rs"]
mod zero_prior_anchor_is_not_pinned;
