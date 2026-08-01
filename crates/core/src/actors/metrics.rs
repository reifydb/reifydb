// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::datetime::DateTime;

use crate::event::{
	lifecycle::VersionEpochSampledEvent,
	metric::{CdcEvictedEvent, CdcWrittenEvent, MultiCommittedEvent, MultiSweptEvent, RequestExecutedEvent},
};

#[derive(Clone, Debug)]
pub enum MetricsMessage {
	Tick(DateTime),
	Flush,
	RequestExecuted(RequestExecutedEvent),
	MultiCommitted(MultiCommittedEvent),
	MultiSwept(MultiSweptEvent),
	CdcWritten(CdcWrittenEvent),
	CdcEvicted(CdcEvictedEvent),
	VersionEpochSampled(VersionEpochSampledEvent),
}
