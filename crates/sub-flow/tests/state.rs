// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

#[path = "state/fixtures.rs"]
mod fixtures;

#[path = "state/update_preserves_created_at.rs"]
mod update_preserves_created_at;

#[path = "state/first_insert_uses_caller_created_at.rs"]
mod first_insert_uses_caller_created_at;

#[path = "state/zero_prior_anchor_is_not_pinned.rs"]
mod zero_prior_anchor_is_not_pinned;
