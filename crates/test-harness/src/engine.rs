// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_engine::{engine::StandardEngine, test_harness::TestEngine};

pub trait AsEngine {
	fn standard_engine(&self) -> &StandardEngine;
}

impl AsEngine for StandardEngine {
	fn standard_engine(&self) -> &StandardEngine {
		self
	}
}

impl AsEngine for TestEngine {
	fn standard_engine(&self) -> &StandardEngine {
		self.inner()
	}
}
