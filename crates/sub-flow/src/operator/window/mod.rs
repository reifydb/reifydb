// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod accumulator;
pub mod aggregate;
pub mod aggregation;
pub mod memory;
pub mod operator;
pub mod rolling;
pub mod session;
pub mod sliding;
pub mod state;
pub mod store;
pub mod tumbling;

use tracing::warn;

use crate::operator::window::operator::WindowOperator;

pub(crate) fn warn_when_expiry_capped(operator: &WindowOperator, expired: usize) {
	let expire_batch = operator.engine_config().expire_batch();
	if expired >= expire_batch {
		warn!(
			node_id = operator.core.node.0,
			expired, expire_batch, "window expiry hit per-tick batch cap, backlog deferred to next tick"
		);
	}
}
