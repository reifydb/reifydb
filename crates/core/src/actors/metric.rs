// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::datetime::DateTime;

use crate::event::metric::{CdcEvictedEvent, CdcWrittenEvent, MultiCommittedEvent, RequestExecutedEvent};

#[derive(Clone, Debug)]
pub enum MetricMessage {
	Tick(DateTime),
	RequestExecuted(RequestExecutedEvent),
	MultiCommitted(MultiCommittedEvent),
	CdcWritten(CdcWrittenEvent),
	CdcEvicted(CdcEvictedEvent),
}
