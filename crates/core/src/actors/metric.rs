// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::datetime::DateTime;

use crate::event::metric::{
	CdcEvictedEvent, CdcWrittenEvent, MultiCommittedEvent, ProfilerSnapshotEvent, RequestExecutedEvent,
};

#[derive(Clone, Debug)]
pub enum MetricMessage {
	Tick(DateTime),
	RequestExecuted(RequestExecutedEvent),
	MultiCommitted(MultiCommittedEvent),
	CdcWritten(CdcWrittenEvent),
	CdcEvicted(CdcEvictedEvent),
	ProfilerSnapshot(ProfilerSnapshotEvent),
}
