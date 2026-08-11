// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod context;
#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
pub mod extern_c;
pub mod extern_wasm;
pub mod registry;

use reifydb_core::value::column::columns::Columns;
use reifydb_value::Result;

pub trait Transform: Send + Sync {
	fn apply(&self, ctx: &context::TransformContext, input: Columns) -> Result<Columns>;
}
