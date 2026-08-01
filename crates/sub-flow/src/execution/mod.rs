// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Shared flow execution core: drives a flow's operator graph over a batch of change deltas. Both
//! the transactional (inline pre-commit) and deferred (CDC) paths run through this same code.

mod batch;
mod dispatch;
pub mod reclaim;
mod routing;
mod tick;
mod timers;

use reifydb_core::common::TimeDomain;
use reifydb_flow::transaction::FlowTransaction;
use reifydb_rql::flow::flow::FlowDag;
use reifydb_value::value::datetime::DateTime;

pub(crate) fn retention_instant(txn: &FlowTransaction, flow: &FlowDag, event_at: DateTime) -> DateTime {
	match flow.time_domain() {
		TimeDomain::Processing => txn.clock().now(),
		TimeDomain::Event => event_at,
	}
}
