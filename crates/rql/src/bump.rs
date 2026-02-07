// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, sync::Arc};

pub(crate) use bumpalo::{Bump, collections::Vec as BumpVec};
use reifydb_type::fragment::{Fragment, StatementColumn, StatementLine};

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
			} => Fragment::Statement {
				text: Arc::from(*text),
				line: *line,
				column: *column,
			},
			BumpFragment::Internal {
				text,
			} => Fragment::Internal {
				text: Arc::from(*text),
			},
		}
	}
}

impl Default for BumpFragment<'_> {
	fn default() -> Self {
		BumpFragment::None
	}
}

/// Deduplicates `Arc<str>` allocations within a single query compilation.
///
/// Identifiers like column names, table names, and operators often repeat across
/// a query (e.g., `SELECT a, b FROM t WHERE a > 5` — `"a"` appears twice).
/// The interner maps `&str → Arc<str>`, so the second occurrence just bumps a refcount.
pub(crate) struct FragmentInterner {
	strings: HashMap<*const str, Arc<str>>,
}

impl FragmentInterner {
	pub fn new() -> Self {
		Self {
			strings: HashMap::new(),
		}
	}

	pub fn intern(&mut self, text: &str) -> Arc<str> {
		// Use the raw pointer as key — within a single compilation, bump-allocated
		// strings at the same address are the same string.
		let key = text as *const str;
		self.strings.entry(key).or_insert_with(|| Arc::from(text)).clone()
	}

	pub fn intern_fragment(&mut self, fragment: &BumpFragment<'_>) -> Fragment {
		match fragment {
			BumpFragment::None => Fragment::None,
			BumpFragment::Statement {
				text,
				line,
				column,
			} => Fragment::Statement {
				text: self.intern(text),
				line: *line,
				column: *column,
			},
			BumpFragment::Internal {
				text,
			} => Fragment::Internal {
				text: self.intern(text),
			},
		}
	}
}
