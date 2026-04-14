// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::datetime::DateTime;

use crate::event::metric::{CdcEvictedEvent, CdcWrittenEvent, MultiCommittedEvent, RequestExecutedEvent};

/// Message type for the metric collector actor.
#[derive(Clone, Debug)]
pub enum MetricMessage {
	Tick(DateTime),
	RequestExecuted(RequestExecutedEvent),
	MultiCommitted(MultiCommittedEvent),
	CdcWritten(CdcWrittenEvent),
	CdcEvicted(CdcEvictedEvent),
}
