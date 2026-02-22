// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod context;
#[cfg(reifydb_target = "native")]
pub mod ffi;
#[cfg(reifydb_target = "native")]
pub mod loader;
pub mod registry;
pub mod wasm;
pub mod wasm_loader;

use reifydb_core::value::column::columns::Columns;

/// A stateless Columns → Columns transformation.
pub trait Transform: Send + Sync {
	fn apply(&self, ctx: &context::TransformContext, input: Columns) -> reifydb_type::Result<Columns>;
}
