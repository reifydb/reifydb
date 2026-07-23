// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::duration::Duration;

use crate::lifecycle::{class::RetentionClass, progress::Progress};

pub trait LifecycleTask: Send + 'static {
	fn name(&self) -> &'static str;

	fn interval(&self) -> Duration;

	fn classes(&self) -> &'static [RetentionClass];

	fn run_slice(&mut self) -> Progress;
}
