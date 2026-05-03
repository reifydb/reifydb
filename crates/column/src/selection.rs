// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::mask::RowMask;

#[derive(Clone, Debug)]
pub enum Selection {
	All,
	None_,
	Mask(RowMask),
}

impl Selection {
	pub fn is_all(&self) -> bool {
		matches!(self, Self::All)
	}

	pub fn is_none(&self) -> bool {
		matches!(self, Self::None_)
	}

	pub fn as_mask(&self) -> Option<&RowMask> {
		match self {
			Self::Mask(m) => Some(m),
			_ => None,
		}
	}
}
