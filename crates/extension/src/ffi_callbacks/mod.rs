// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Shared FFI runtime infrastructure: builder registry / callbacks used by
//! every native FFI extension point (operators, transforms, procedures).
//!
//! `BuilderRegistry` plus the host-side builder callbacks lets a guest emit
//! output columns by writing directly into host-pool-owned buffers, instead
//! of allocating its own `Columns` and round-tripping through a marshal/
//! unmarshal copy.

pub mod builder;
pub mod panic;

use reifydb_abi::callbacks::builder::EmitDiffKind;
use reifydb_core::value::column::columns::Columns;

use crate::ffi_callbacks::builder::BuilderRegistry;

/// Drain a `BuilderRegistry` that received a single Insert-shaped diff and
/// return its `post` (or `pre` for a Remove) columns. Used by the
/// single-Columns FFI hot paths (operator `pull`, transforms, procedures).
pub fn single_columns_from_registry(registry: &BuilderRegistry) -> Columns {
	let mut diffs = registry.drain();
	if let Some(first) = diffs.drain(..).next() {
		match first.kind {
			EmitDiffKind::Insert | EmitDiffKind::Update => first.post.unwrap_or_else(Columns::empty),
			EmitDiffKind::Remove => first.pre.unwrap_or_else(Columns::empty),
		}
	} else {
		Columns::empty()
	}
}
