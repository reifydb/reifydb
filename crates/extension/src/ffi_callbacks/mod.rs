// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod builder;
pub mod panic;

use reifydb_abi::callbacks::builder::EmitDiffKind;
use reifydb_core::value::column::columns::Columns;

use crate::ffi_callbacks::builder::BuilderRegistry;

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
