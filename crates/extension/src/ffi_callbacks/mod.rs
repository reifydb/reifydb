// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

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
