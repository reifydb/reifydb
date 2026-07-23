// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::count::Count;

use crate::define_event;

define_event! {






	pub struct VersionEpochSampledEvent {
		pub durable_samples: Count,
		pub pruned: Count,
	}
}
