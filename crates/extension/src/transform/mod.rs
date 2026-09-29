// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod context;
#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
pub mod extern_c;
#[cfg(feature = "wasm")]
pub mod extern_wasm;
pub mod registry;

use arrow_array::RecordBatch;
use reifydb_value::Result;

pub trait Transform: Send + Sync {
	fn apply(&self, ctx: &context::TransformContext, input: RecordBatch) -> Result<RecordBatch>;
}
