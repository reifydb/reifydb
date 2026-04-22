// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::mask::RowMask;

// Result of evaluating a predicate against a `Table`.
//
// `None_` uses a trailing underscore so pattern matches don't shadow
// `Option::None`.
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
