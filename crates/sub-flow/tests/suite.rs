// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[path = "aggregate_expression_input.rs"]
mod aggregate_expression_input;
#[path = "apply_unknown_operator_view.rs"]
mod apply_unknown_operator_view;
#[path = "apply_unknown_operator_view_create.rs"]
mod apply_unknown_operator_view_create;
#[path = "call_argument_named_like_a_type.rs"]
mod call_argument_named_like_a_type;
#[path = "custom_operator_timer.rs"]
mod custom_operator_timer;
#[path = "digest_view.rs"]
mod digest_view;
#[path = "digest_view_lifecycle.rs"]
mod digest_view_lifecycle;
#[path = "digest_view_sink_write.rs"]
mod digest_view_sink_write;
#[path = "digest_view_sort_key.rs"]
mod digest_view_sort_key;
#[path = "enum_variant_in_view.rs"]
mod enum_variant_in_view;
#[path = "flow_isolation.rs"]
mod flow_isolation;
#[path = "flow_materialization.rs"]
mod flow_materialization;
#[path = "flow_routing.rs"]
mod flow_routing;
#[path = "flow_watermark.rs"]
mod flow_watermark;
#[path = "frontier_propagation.rs"]
mod frontier_propagation;
#[path = "frontier_shutdown.rs"]
mod frontier_shutdown;
#[path = "lifecycle_coverage.rs"]
mod lifecycle_coverage;
#[path = "managed_reclaim.rs"]
mod managed_reclaim;
#[path = "partitioned_view.rs"]
mod partitioned_view;
#[path = "percentile_rolling_view.rs"]
mod percentile_rolling_view;
#[path = "percentile_view.rs"]
mod percentile_view;
#[path = "percentile_window_kinds.rs"]
mod percentile_window_kinds;
#[path = "regression.rs"]
mod regression;
#[path = "replay_determinism.rs"]
mod replay_determinism;
#[path = "ringbuffer_eviction_propagation.rs"]
mod ringbuffer_eviction_propagation;
#[path = "ringbuffer_row_ttl.rs"]
mod ringbuffer_row_ttl;
#[path = "state.rs"]
mod state;
#[path = "time_propagation.rs"]
mod time_propagation;
#[path = "view_calling_a_script_routine.rs"]
mod view_calling_a_script_routine;
#[path = "view_calling_a_udf.rs"]
mod view_calling_a_udf;
#[path = "window_epoch.rs"]
mod window_epoch;
#[path = "window_metadata.rs"]
mod window_metadata;
#[path = "window_replay_determinism.rs"]
mod window_replay_determinism;
#[path = "window_rolling.rs"]
mod window_rolling;
#[path = "window_seal.rs"]
mod window_seal;
#[path = "window_sliding.rs"]
mod window_sliding;
