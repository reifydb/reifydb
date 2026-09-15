// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[path = "regression/flow_aggregate_digest_key.rs"]
mod flow_aggregate_digest_key;

#[path = "regression/flow_aggregate_output_schema.rs"]
mod flow_aggregate_output_schema;

#[path = "regression/flow_digest_group_key.rs"]
mod flow_digest_group_key;

#[path = "regression/flow_distinct_typed_key.rs"]
mod flow_distinct_typed_key;

#[path = "regression/flow_panic_health.rs"]
mod flow_panic_health;

#[path = "regression/flow_key_identity.rs"]
mod flow_key_identity;

#[path = "regression/latest_snapshot_join_republish.rs"]
mod latest_snapshot_join_republish;

#[path = "regression/natural_join_input_schema.rs"]
mod natural_join_input_schema;

#[path = "regression/natural_join_without_shared_column.rs"]
mod natural_join_without_shared_column;

#[path = "regression/snapshot_join_retraction.rs"]
mod snapshot_join_retraction;

#[path = "regression/update_pre_fidelity.rs"]
mod update_pre_fidelity;

#[path = "regression/view_dictionary_columns.rs"]
mod view_dictionary_columns;

#[path = "regression/view_hop_source_order.rs"]
mod view_hop_source_order;

#[path = "regression/view_read_after_upstream_write.rs"]
mod view_read_after_upstream_write;

#[path = "regression/window_membership_cleanup.rs"]
mod window_membership_cleanup;
