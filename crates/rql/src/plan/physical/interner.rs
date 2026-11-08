// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_rql_frontend::bump::BumpFragment;
use reifydb_value::fragment::Fragment;

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
