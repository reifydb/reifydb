// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, sync::Arc};

pub(crate) use bumpalo::{Bump, collections::Vec as BumpVec};
use reifydb_type::fragment::{Fragment, StatementColumn, StatementLine};

pub type BumpBox<'b, T> = bumpalo::boxed::Box<'b, T>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BumpFragment<'bump> {
	#[default]
	None,
	Statement {
		text: &'bump str,
		offset: usize,
		source_end: usize,
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

	pub fn offset(&self) -> usize {
		match self {
			BumpFragment::Statement {
				offset,
				..
			} => *offset,
			_ => 0,
		}
	}

	pub fn source_end(&self) -> usize {
		match self {
			BumpFragment::Statement {
				source_end,
				..
			} => *source_end,
			_ => 0,
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
				offset: _,
				source_end: _,
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
		let key = text as *const str;
		self.strings.entry(key).or_insert_with(|| Arc::from(text)).clone()
	}

	pub fn intern_fragment(&mut self, fragment: &BumpFragment<'_>) -> Fragment {
		match fragment {
			BumpFragment::None => Fragment::None,
			BumpFragment::Statement {
				text,
				offset: _,
				source_end: _,
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
