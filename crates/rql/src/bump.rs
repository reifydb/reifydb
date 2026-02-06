// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub(crate) use bumpalo::{Bump, collections::Vec as BumpVec};
use reifydb_type::fragment::Fragment;
pub use reifydb_type::fragment::{StatementColumn, StatementLine};

pub(crate) type BumpBox<'b, T> = bumpalo::boxed::Box<'b, T>;

/// A bump-allocated fragment that avoids heap allocation during the transient pipeline.
///
/// Unlike `Fragment` (which uses `Arc<String>`), `BumpFragment` stores `&'bump str`
/// slices — either zero-copy references into the original input string, or bump-allocated
/// strings for constructed text. Converts to owned `Fragment` at materialization boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BumpFragment<'bump> {
	None,
	Statement {
		text: &'bump str,
		line: StatementLine,
		column: StatementColumn,
	},
	Internal {
		text: &'bump str,
	},
}

impl<'bump> BumpFragment<'bump> {
	pub fn internal(bump: &'bump Bump, text: &str) -> Self {
		BumpFragment::Internal {
			text: bump.alloc_str(text),
		}
	}

	pub fn text(&self) -> &str {
		match self {
			BumpFragment::None => "",
			BumpFragment::Statement {
				text,
				..
			} => text,
			BumpFragment::Internal {
				text,
			} => text,
		}
	}

	pub fn line(&self) -> StatementLine {
		match self {
			BumpFragment::Statement {
				line,
				..
			} => *line,
			_ => StatementLine(1),
		}
	}

	pub fn column(&self) -> StatementColumn {
		match self {
			BumpFragment::Statement {
				column,
				..
			} => *column,
			_ => StatementColumn(0),
		}
	}

	pub fn to_owned(&self) -> Fragment {
		match self {
			BumpFragment::None => Fragment::None,
			BumpFragment::Statement {
				text,
				line,
				column,
			} => Fragment::statement(*text, line.0, column.0),
			BumpFragment::Internal {
				text,
			} => Fragment::internal(*text),
		}
	}
}

impl Default for BumpFragment<'_> {
	fn default() -> Self {
		BumpFragment::None
	}
}
