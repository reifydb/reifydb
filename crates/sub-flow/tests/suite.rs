// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[path = "custom_operator_timer.rs"]
mod custom_operator_timer;
#[path = "fd_lifecycle.rs"]
mod fd_lifecycle;
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
#[path = "partitioned_view.rs"]
mod partitioned_view;
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
