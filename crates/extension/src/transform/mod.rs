// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod context;
#[cfg(reifydb_target = "native")]
pub mod ffi;
#[cfg(reifydb_target = "native")]
pub mod ffi_loader;
pub mod registry;
pub mod wasm;
pub mod wasm_loader;

use reifydb_core::value::column::columns::Columns;
use reifydb_type::Result;

/// A stateless Columns → Columns transformation.
pub trait Transform: Send + Sync {
	fn apply(&self, ctx: &context::TransformContext, input: Columns) -> Result<Columns>;
}
