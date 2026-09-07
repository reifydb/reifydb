// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[path = "regression/latest_snapshot_join_republish.rs"]
mod latest_snapshot_join_republish;

#[path = "regression/snapshot_join_retraction.rs"]
mod snapshot_join_retraction;

#[path = "regression/update_pre_fidelity.rs"]
mod update_pre_fidelity;

#[path = "regression/view_dictionary_columns.rs"]
mod view_dictionary_columns;

#[path = "regression/view_read_after_upstream_write.rs"]
mod view_read_after_upstream_write;

#[path = "regression/window_membership_cleanup.rs"]
mod window_membership_cleanup;
